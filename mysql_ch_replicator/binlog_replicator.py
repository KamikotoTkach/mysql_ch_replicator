import contextlib
import pickle
import struct
import time
import os
import os.path
import json
import random
import re

from enum import Enum
from logging import getLogger
from dataclasses import dataclass

from pymysql.err import OperationalError

from .pymysqlreplication import BinLogStreamReader
from .pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from .pymysqlreplication.event import HeartbeatLogEvent, QueryEvent

from .config import Settings, BinlogReplicatorSettings
from .mysql_api import MySQLApi
from .utils import GracefulKiller


logger = getLogger(__name__)


class EventType(Enum):
    UNKNOWN = 0
    ADD_EVENT = 1
    REMOVE_EVENT = 2
    QUERY = 3


@dataclass
class LogEvent:
    transaction_id: tuple = 0  # (file_name, log_pos)
    db_name: str = ''
    table_name: str = ''
    records: object = None
    event_type: int = EventType.UNKNOWN.value


class TransactionNotFound(Exception):
    def __init__(self, db_name, transaction_id):
        self.db_name = db_name
        self.transaction_id = transaction_id
        super().__init__('transaction not found', db_name, transaction_id)


@dataclass
class DbReplicatorStateInfo:
    db_name: str
    state_path: str
    last_processed_transaction: tuple


def get_binlog_file_num(file_name):
    return int(file_name.rsplit('.', 1)[1])


def compare_transactions(left, right):
    left_file_num = get_binlog_file_num(left[0])
    right_file_num = get_binlog_file_num(right[0])
    if left_file_num != right_file_num:
        return left_file_num - right_file_num
    return left[1] - right[1]


def min_transaction(transactions):
    return min(transactions, key=lambda item: (get_binlog_file_num(item[0]), item[1]))


def normalize_transaction(transaction_id):
    if transaction_id is None:
        return None
    return tuple(transaction_id)


def load_db_replicator_state_data(state_path):
    try:
        data = open(state_path, 'rb').read()
        return pickle.loads(data)
    except Exception:
        logger.warning(f'failed to load db_replicator state {state_path}', exc_info=True)
        return None


def load_db_replicator_state_transaction(state_path):
    data = load_db_replicator_state_data(state_path)
    if not data:
        return None
    return normalize_transaction(data.get('last_processed_transaction'))


def get_db_replicator_state_infos(data_dir, databases=None):
    if not os.path.exists(data_dir):
        return []

    allowed_databases = set(databases) if databases is not None else None
    result = []
    for entry in os.scandir(data_dir):
        if not entry.is_dir():
            continue
        db_name = os.path.basename(entry.path)
        if allowed_databases is not None and db_name not in allowed_databases:
            continue
        state_path = os.path.join(entry.path, 'state.pckl')
        if not os.path.exists(state_path):
            continue
        transaction_id = load_db_replicator_state_transaction(state_path)
        if transaction_id is None:
            continue
        result.append(DbReplicatorStateInfo(
            db_name=db_name,
            state_path=state_path,
            last_processed_transaction=transaction_id,
        ))
    return result


def get_db_replicator_state_transactions(data_dir, db_name):
    db_path = os.path.join(data_dir, db_name)
    if not os.path.exists(db_path):
        return []

    result = []
    for file_name in os.listdir(db_path):
        if file_name == 'state.pckl' or (file_name.startswith('state_worker_') and file_name.endswith('.pckl')):
            transaction_id = load_db_replicator_state_transaction(os.path.join(db_path, file_name))
            if transaction_id is not None:
                result.append(transaction_id)
    return result


def get_binlog_stream_events():
    return [
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
        QueryEvent,
        HeartbeatLogEvent,
    ]


def get_mysql_connection_settings(settings: Settings):
    mysql_settings = {
        'host': settings.mysql.host,
        'port': settings.mysql.port,
        'user': settings.mysql.user,
        'passwd': settings.mysql.password,
        'charset': settings.mysql.charset,
    }
    return mysql_settings


def create_binlog_stream(settings: Settings, log_file=None, log_pos=None, blocking=True):
    return BinLogStreamReader(
        connection_settings=get_mysql_connection_settings(settings),
        server_id=random.randint(1, 2**32-2),
        blocking=blocking,
        resume_stream=True,
        log_pos=log_pos,
        log_file=log_file,
        mysql_timezone=settings.mysql_timezone,
        slave_heartbeat=BinlogReplicator.SLAVE_HEARTBEAT_INTERVAL if blocking else None,
        only_events=get_binlog_stream_events(),
    )


def decode_if_bytes(value):
    if isinstance(value, bytes):
        return value.decode('utf-8')
    return value


def get_query_event_db_name(query: str) -> str:
    return BinlogReplicator._try_parse_db_name_from_query(query)


def convert_stream_event_to_log_event(settings: Settings, event, transaction_id):
    if isinstance(event, HeartbeatLogEvent):
        return None

    if not isinstance(event, (DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent, QueryEvent)):
        return None

    log_event = LogEvent()
    if hasattr(event, 'table'):
        log_event.table_name = decode_if_bytes(event.table)

        if not settings.is_table_matches(log_event.table_name):
            return None

    log_event.db_name = decode_if_bytes(event.schema)

    if isinstance(event, UpdateRowsEvent) or isinstance(event, WriteRowsEvent):
        log_event.event_type = EventType.ADD_EVENT.value

    if isinstance(event, DeleteRowsEvent):
        log_event.event_type = EventType.REMOVE_EVENT.value

    if isinstance(event, QueryEvent):
        log_event.event_type = EventType.QUERY.value

    if log_event.event_type == EventType.UNKNOWN.value:
        return None

    if log_event.event_type == EventType.QUERY.value:
        db_name_from_query = get_query_event_db_name(event.query)
        if db_name_from_query:
            log_event.db_name = db_name_from_query

    if not settings.is_database_matches(log_event.db_name):
        return None

    log_event.transaction_id = transaction_id

    if isinstance(event, QueryEvent):
        log_event.records = event.query
        return log_event

    log_event.records = []
    for row in event.rows:
        if isinstance(event, DeleteRowsEvent):
            vals = row['values']
            vals = list(vals.values())
            log_event.records.append(vals)

        elif isinstance(event, UpdateRowsEvent):
            vals = row['after_values']
            vals = list(vals.values())
            log_event.records.append(vals)

        elif isinstance(event, WriteRowsEvent):
            vals = row['values']
            vals = list(vals.values())
            log_event.records.append(vals)

    return log_event


class FileWriter:
    FLUSH_INTERVAL = 1

    def __init__(self, file_path):
        self.num_records = 0
        self.file = open(file_path, 'wb')
        self.last_flush_time = 0

    def close(self):
        self.file.close()

    def write_event(self, log_event):
        data = pickle.dumps(log_event)
        data_size = len(data)
        data = struct.pack('>I', data_size) + data
        self.file.write(data)
        curr_time = time.time()
        if curr_time - self.last_flush_time > FileWriter.FLUSH_INTERVAL:
            self.file.flush()
        self.num_records += len(log_event.records)


class FileReader:
    def __init__(self, file_path):
        self.file = open(file_path, 'rb')
        self.current_buffer = b''
        self.file_num = int(os.path.basename(file_path).split('.')[0])

    def close(self):
        self.file.close()

    def read_next_event(self) -> LogEvent:

        # read size if we don't have enough bytes to get size
        if len(self.current_buffer) < 4:
            self.current_buffer += self.file.read(4 - len(self.current_buffer))

        # still no size - unable to read
        if len(self.current_buffer) < 4:
            return None

        size_data = self.current_buffer[:4]
        size_to_read = struct.unpack('>I', size_data)[0]

        # read
        if len(self.current_buffer) != size_to_read + 4:
            self.current_buffer += self.file.read(size_to_read + 4 - len(self.current_buffer))

        if len(self.current_buffer) != size_to_read + 4:
            return None

        event = pickle.loads(self.current_buffer[4:])
        self.current_buffer = b''
        return event


def get_existing_file_nums(data_dir, db_name):
    db_path = os.path.join(data_dir, db_name)
    if not os.path.exists(db_path):
        os.mkdir(db_path)
    existing_files = os.listdir(db_path)
    existing_files = [f for f in existing_files if f.endswith('.bin')]
    existing_file_nums = sorted([int(f.split('.')[0]) for f in existing_files])
    return existing_file_nums


def get_file_name_by_num(data_dir, db_name, file_num):
    return os.path.join(data_dir, db_name, f'{file_num}.bin')


class DataReader:
    def __init__(self, replicator_settings: BinlogReplicatorSettings, db_name: str):
        self.data_dir = replicator_settings.data_dir
        self.db_name = db_name
        self.current_file_reader: FileReader = None

    def get_last_transaction_id(self):
        last_file_name = self.get_last_file_name()
        if last_file_name is None:
            return None
        file_reader = FileReader(file_path=last_file_name)
        last_transaction_id = None
        while True:
            event = file_reader.read_next_event()
            if event is None:
                break
            last_transaction_id = event.transaction_id
        file_reader.close()
        return last_transaction_id

    def get_last_file_name(self):
        existing_file_nums = get_existing_file_nums(self.data_dir, self.db_name)
        if existing_file_nums:
            last_file_num = max(existing_file_nums)
            file_name = f'{last_file_num}.bin'
            file_name = os.path.join(self.data_dir, self.db_name, file_name)
            return file_name
        return None

    def get_first_transaction_in_file(self, file_num):
        file_name = get_file_name_by_num(self.data_dir, self.db_name, file_num)
        file_reader = FileReader(file_name)
        first_event = file_reader.read_next_event()
        file_reader.close()
        if first_event is None:
            return None
        return first_event.transaction_id

    def file_has_transaction(self, file_num, transaction_id) -> bool:
        file_name = get_file_name_by_num(self.data_dir, self.db_name, file_num)
        reader = FileReader(file_name)
        try:
            while True:
                event = reader.read_next_event()
                if event is None:
                    break
                if event.transaction_id == transaction_id:
                    return True
            return False
        finally:
            reader.close()

    def get_file_with_transaction(self, existing_file_nums, transaction_id):
        if not existing_file_nums:
            raise TransactionNotFound(self.db_name, transaction_id)

        matching_file_num = None
        prev_file_num = None
        for file_num in existing_file_nums:
            file_first_transaction = self.get_first_transaction_in_file(file_num)
            if file_first_transaction is None:
                continue
            if compare_transactions(file_first_transaction, transaction_id) > 0:
                matching_file_num = prev_file_num
                break
            prev_file_num = file_num
        if matching_file_num is None:
            matching_file_num = existing_file_nums[-1]

        idx = existing_file_nums.index(matching_file_num)
        for i in range(max(0, idx-10), idx+10):
            if i >= len(existing_file_nums):
                break
            file_num = existing_file_nums[i]
            if self.file_has_transaction(file_num, transaction_id):
                return file_num

        raise TransactionNotFound(self.db_name, transaction_id)

    def set_position(self, transaction_id):
        existing_file_nums = get_existing_file_nums(self.data_dir, self.db_name)

        if transaction_id is None:
            # todo: handle empty files case
            if not existing_file_nums:
                self.current_file_reader = None
                logger.info(f'set position - no files found')
                return

            matching_file_num = existing_file_nums[0]
            file_name = get_file_name_by_num(self.data_dir, self.db_name, matching_file_num)
            self.current_file_reader = FileReader(file_name)
            logger.info(f'set position to the first file {file_name}')
            return

        matching_file_num = self.get_file_with_transaction(existing_file_nums, transaction_id)

        file_name = get_file_name_by_num(self.data_dir, self.db_name, matching_file_num)
        logger.info(f'set position to {file_name}')

        self.current_file_reader = FileReader(file_name)
        while True:
            event = self.current_file_reader.read_next_event()
            if event is None:
                break
            if event.transaction_id == transaction_id:
                logger.info(f'found transaction {transaction_id} inside {file_name}')
                return
            if compare_transactions(event.transaction_id, transaction_id) > 0:
                break
        raise TransactionNotFound(self.db_name, transaction_id)

    def read_next_event(self) -> LogEvent:
        if self.current_file_reader is None:
            # no file reader - try to read from the beginning
            existing_file_nums = get_existing_file_nums(self.data_dir, self.db_name)
            if not existing_file_nums:
                return None
            file_num = existing_file_nums[0]
            file_name = get_file_name_by_num(self.data_dir, self.db_name, file_num)
            self.current_file_reader = FileReader(file_name)
            return self.read_next_event()

        result = self.current_file_reader.read_next_event()

        if result is None:
            # no result in current file - check if new file available
            next_file_num = self.current_file_reader.file_num + 1
            next_file_path = get_file_name_by_num(self.data_dir, self.db_name, next_file_num)
            if not os.path.exists(next_file_path):
                return None
            logger.debug(f'switching to next file {next_file_path}')
            self.current_file_reader = FileReader(next_file_path)
            return self.read_next_event()

        return result


class DataWriter:
    def __init__(self, replicator_settings: BinlogReplicatorSettings):
        self.data_dir = replicator_settings.data_dir
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)
        self.records_per_file = replicator_settings.records_per_file
        self.db_file_writers: dict = {}  # db_name => FileWriter

    def store_event(self, log_event: LogEvent):
        logger.debug(f'store event {log_event.transaction_id}')
        file_writer = self.get_or_create_file_writer(log_event.db_name)
        file_writer.write_event(log_event)

    def get_or_create_file_writer(self, db_name: str) -> FileWriter:
        file_writer = self.db_file_writers.get(db_name)
        if file_writer is not None:
            if file_writer.num_records >= self.records_per_file:
                file_writer.close()
                del self.db_file_writers[db_name]
                file_writer = None
        if file_writer is None:
            file_writer = self.create_file_writer(db_name)
            self.db_file_writers[db_name] = file_writer
        return file_writer

    def create_file_writer(self, db_name: str) -> FileWriter:
        next_free_file = self.get_next_file_name(db_name)
        return FileWriter(next_free_file)

    def get_next_file_name(self, db_name: str):
        existing_file_nums = get_existing_file_nums(self.data_dir, db_name)

        last_file_num = 0
        if existing_file_nums:
            last_file_num = max(existing_file_nums)

        new_file_num = last_file_num + 1
        new_file_name = f'{new_file_num}.bin'
        new_file_name = os.path.join(self.data_dir, db_name, new_file_name)
        return new_file_name

    def get_oldest_protected_file_num(self, db_name, existing_file_nums):
        protected_transactions = get_db_replicator_state_transactions(self.data_dir, db_name)
        if not protected_transactions:
            return None, True

        reader = DataReader(BinlogReplicatorSettings(data_dir=self.data_dir), db_name)
        protected_file_nums = []
        for transaction_id in protected_transactions:
            try:
                protected_file_nums.append(reader.get_file_with_transaction(existing_file_nums, transaction_id))
            except TransactionNotFound:
                logger.warning(
                    f'skip old binlog cleanup for {db_name}: '
                    f'protected transaction {transaction_id} is missing from local relay',
                )
                return None, False
        return min(protected_file_nums), True

    def remove_old_files(self, ts_from):
        PRESERVE_FILES_COUNT = 5

        subdirs = [f.path for f in os.scandir(self.data_dir) if f.is_dir()]
        for db_path in subdirs:
            db_name = os.path.basename(db_path)
            existing_file_nums = get_existing_file_nums(self.data_dir, db_name)
            protected_file_num, can_cleanup = self.get_oldest_protected_file_num(db_name, existing_file_nums)
            if not can_cleanup:
                continue

            if len(existing_file_nums) <= PRESERVE_FILES_COUNT + 1:
                file_nums_to_check = []
            else:
                # Preserve the current writer file and several latest closed files.
                file_nums_to_check = existing_file_nums[:-(PRESERVE_FILES_COUNT + 1)]

            if protected_file_num is not None:
                file_nums_to_check = [
                    file_num for file_num in file_nums_to_check
                    if file_num < protected_file_num
                ]

            for file_num in file_nums_to_check:
                file_path = os.path.join(self.data_dir, db_name, f'{file_num}.bin')
                modify_time = os.path.getmtime(file_path)
                if modify_time <= ts_from:
                    os.remove(file_path)

    def close_all(self):
        for file_writer in self.db_file_writers.values():
            file_writer.close()


class State:

    def __init__(self, file_name):
        self.file_name = file_name
        self.last_seen_transaction = None
        self.prev_last_seen_transaction = None
        self.pid = None
        self.load()

    def load(self):
        file_name = self.file_name
        if not os.path.exists(file_name):
            return
        data = open(file_name, 'rt').read()
        data = json.loads(data)
        self.last_seen_transaction = data['last_seen_transaction']
        self.prev_last_seen_transaction = data['prev_last_seen_transaction']
        self.pid = data.get('pid', None)
        if self.last_seen_transaction is not None:
            self.last_seen_transaction = tuple(self.last_seen_transaction)
        if self.prev_last_seen_transaction is not None:
            self.prev_last_seen_transaction = tuple(self.prev_last_seen_transaction)

    def save(self):
        file_name = self.file_name
        data = json.dumps({
            'last_seen_transaction': self.last_seen_transaction,
            'prev_last_seen_transaction': self.prev_last_seen_transaction,
            'pid': os.getpid(),
        })
        with open(file_name + '.tmp', 'wt') as f:
            f.write(data)
        os.rename(file_name + '.tmp', file_name)


class RelayRecovery:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.data_dir = settings.binlog_replicator.data_dir

    def recover_if_required(self, databases=None):
        state_infos = get_db_replicator_state_infos(self.data_dir, databases=databases)
        missing_states = self.get_missing_states(state_infos=state_infos)
        if not missing_states:
            logger.info('local relay recovery not required')
            return False

        databases_to_rebuild = sorted({state.db_name for state in state_infos})
        start_transaction = min_transaction([state.last_processed_transaction for state in state_infos])
        start_file = start_transaction[0]
        logger.warning(
            f'local relay recovery required for databases {[state.db_name for state in missing_states]}; '
            f'rebuilding local relay for databases {databases_to_rebuild}, '
            f'starting from MySQL binlog file {start_file}',
        )

        self.validate_mysql_binlog_available(start_file)
        backups = self.backup_bin_files(databases_to_rebuild)
        data_writer = DataWriter(self.settings.binlog_replicator)
        final_transaction = None
        try:
            final_transaction = self.rebuild_relay(
                data_writer=data_writer,
                affected_databases=set(databases_to_rebuild),
                start_file=start_file,
            )
            data_writer.close_all()
            self.validate_recovered_states(state_infos)
            self.save_binlog_state(final_transaction)
        except Exception:
            data_writer.close_all()
            self.restore_backups(backups)
            raise

        logger.warning(
            f'local relay recovery finished for databases {databases_to_rebuild}, '
            f'last recovered transaction: {final_transaction}',
        )
        return True

    def get_missing_states(self, state_infos):
        result = []
        for state_info in state_infos:
            existing_file_nums = get_existing_file_nums(self.data_dir, state_info.db_name)
            reader = DataReader(self.settings.binlog_replicator, state_info.db_name)
            try:
                reader.get_file_with_transaction(existing_file_nums, state_info.last_processed_transaction)
            except TransactionNotFound:
                result.append(state_info)
        return result

    def validate_mysql_binlog_available(self, start_file):
        mysql_api = MySQLApi(
            database=None,
            mysql_settings=self.settings.mysql,
            mysql_timezone=self.settings.mysql_timezone,
        )
        try:
            mysql_binlog_files = mysql_api.get_binlog_files()
        finally:
            with contextlib.suppress(Exception):
                mysql_api.close()

        if start_file not in mysql_binlog_files:
            raise RuntimeError(
                f'cannot recover local relay from MySQL: required binlog file {start_file} is not available; '
                f'available files: {mysql_binlog_files}'
            )

    def backup_bin_files(self, databases):
        backups = {}
        timestamp = int(time.time())
        for db_name in databases:
            db_path = os.path.join(self.data_dir, db_name)
            if not os.path.exists(db_path):
                continue
            bin_files = [
                file_name for file_name in os.listdir(db_path)
                if file_name.endswith('.bin')
            ]
            if not bin_files:
                continue

            backup_dir = os.path.join(db_path, f'recovery_backup_{timestamp}')
            suffix = 1
            while os.path.exists(backup_dir):
                backup_dir = os.path.join(db_path, f'recovery_backup_{timestamp}_{suffix}')
                suffix += 1
            os.mkdir(backup_dir)

            backups[db_name] = backup_dir
            for file_name in bin_files:
                os.replace(os.path.join(db_path, file_name), os.path.join(backup_dir, file_name))
        return backups

    def restore_backups(self, backups):
        for db_name, backup_dir in backups.items():
            db_path = os.path.join(self.data_dir, db_name)
            for file_name in os.listdir(db_path):
                if file_name.endswith('.bin'):
                    os.remove(os.path.join(db_path, file_name))
            for file_name in os.listdir(backup_dir):
                os.replace(os.path.join(backup_dir, file_name), os.path.join(db_path, file_name))
            with contextlib.suppress(OSError):
                os.rmdir(backup_dir)

    def rebuild_relay(self, data_writer, affected_databases, start_file):
        stream = create_binlog_stream(self.settings, log_file=start_file, log_pos=4, blocking=False)
        final_transaction = (start_file, 4)
        try:
            while True:
                event = stream.fetchone()
                if event is None:
                    if stream.log_file is not None and stream.log_pos is not None:
                        final_transaction = (stream.log_file, stream.log_pos)
                    break

                if stream.log_file is None or stream.log_pos is None:
                    continue

                transaction_id = (stream.log_file, stream.log_pos)
                final_transaction = transaction_id
                log_event = convert_stream_event_to_log_event(self.settings, event, transaction_id)
                if log_event is None:
                    continue
                if log_event.db_name not in affected_databases:
                    continue
                data_writer.store_event(log_event)
        finally:
            with contextlib.suppress(Exception):
                stream.close()

        return final_transaction

    def validate_recovered_states(self, state_infos):
        for state_info in state_infos:
            existing_file_nums = get_existing_file_nums(self.data_dir, state_info.db_name)
            reader = DataReader(self.settings.binlog_replicator, state_info.db_name)
            reader.get_file_with_transaction(existing_file_nums, state_info.last_processed_transaction)

    def save_binlog_state(self, final_transaction):
        if final_transaction is None:
            return
        state = State(os.path.join(self.data_dir, 'state.json'))
        state.prev_last_seen_transaction = final_transaction
        state.last_seen_transaction = final_transaction
        state.save()


class BinlogReplicator:
    SAVE_UPDATE_INTERVAL = 60
    BINLOG_CLEAN_INTERVAL = 5 * 60
    SLAVE_HEARTBEAT_INTERVAL = 5
    MAX_EVENTS_PER_TICK = 1000

    def __init__(self, settings: Settings):
        self.settings = settings
        self.mysql_settings = settings.mysql
        self.replicator_settings = settings.binlog_replicator
        self.data_writer = DataWriter(self.replicator_settings)
        self.state = State(os.path.join(self.replicator_settings.data_dir, 'state.json'))
        logger.info(f'state start position: {self.state.prev_last_seen_transaction}')

        log_file, log_pos = None, None
        if self.state.prev_last_seen_transaction:
            log_file, log_pos = self.state.prev_last_seen_transaction

        self.stream = create_binlog_stream(settings, log_file=log_file, log_pos=log_pos, blocking=True)
        self.last_state_update = 0
        self.last_binlog_clear_time = 0

    def read_stream_event(self):
        return self.stream.fetchone()

    def is_heartbeat_event(self, event):
        return isinstance(event, HeartbeatLogEvent)

    def convert_stream_event_to_log_event(self, event, transaction_id):
        return convert_stream_event_to_log_event(self.settings, event, transaction_id)

    def clear_old_binlog_if_required(self):
        curr_time = time.time()
        if curr_time - self.last_binlog_clear_time < BinlogReplicator.BINLOG_CLEAN_INTERVAL:
            return

        self.last_binlog_clear_time = curr_time
        self.data_writer.remove_old_files(curr_time - self.replicator_settings.binlog_retention_period)

    @classmethod
    def _try_parse_db_name_from_query(cls, query: str) -> str:
        """
         Extract the database name from a MySQL CREATE TABLE or ALTER TABLE query.
         Supports multiline queries and quoted identifiers that may include special characters.

         Examples:
           - CREATE TABLE `mydb`.`mytable` ( ... )
           - ALTER TABLE mydb.mytable ADD COLUMN id int NOT NULL
           - CREATE TABLE IF NOT EXISTS mydb.mytable ( ... )
           - ALTER TABLE "mydb"."mytable" ...
           - CREATE TABLE IF NOT EXISTS `multidb` . `multitable` ( ... )
           - CREATE TABLE `replication-test_db`.`test_table_2` ( ... )

         Returns the database name, or an empty string if not found.
         """
        # Updated regex:
        # 1. Matches optional leading whitespace.
        # 2. Matches "CREATE TABLE" or "ALTER TABLE" (with optional IF NOT EXISTS).
        # 3. Optionally captures a database name, which can be either:
        #      - Quoted (using backticks or double quotes) and may contain special characters.
        #      - Unquoted (letters, digits, and underscores only).
        # 4. Allows optional whitespace around the separating dot.
        # 5. Matches the table name (which we do not capture).
        pattern = re.compile(
            r'^\s*'  # optional leading whitespace/newlines
            r'(?i:(?:create|alter))\s+table\s+'  # "CREATE TABLE" or "ALTER TABLE"
            r'(?:if\s+not\s+exists\s+)?'  # optional "IF NOT EXISTS"
            # Optional DB name group: either quoted or unquoted, followed by optional whitespace, a dot, and more optional whitespace.
            r'(?:(?:[`"](?P<dbname_quoted>[^`"]+)[`"]|(?P<dbname_unquoted>[a-zA-Z0-9_]+))\s*\.\s*)?'
            r'[`"]?[a-zA-Z0-9_]+[`"]?',  # table name (quoted or not)
            re.IGNORECASE | re.DOTALL  # case-insensitive, dot matches newline
        )

        m = pattern.search(query)
        if m:
            # Return the quoted db name if found; else return the unquoted name if found.
            if m.group('dbname_quoted'):
                return m.group('dbname_quoted')
            elif m.group('dbname_unquoted'):
                return m.group('dbname_unquoted')
        return ''

    def run(self):
        last_transaction_id = None

        killer = GracefulKiller()

        last_log_time = time.time()
        total_processed_events = 0

        while not killer.kill_now:
            try:
                curr_time = time.time()
                if curr_time - last_log_time > 60:
                    last_log_time = curr_time
                    logger.info(
                        f'last transaction id: {last_transaction_id}, processed events: {total_processed_events}',
                    )

                last_read_count = 0
                while not killer.kill_now:
                    event = self.read_stream_event()
                    if event is None:
                        break

                    last_read_count += 1
                    total_processed_events += 1
                    transaction_id = (self.stream.log_file, self.stream.log_pos)
                    last_transaction_id = transaction_id

                    self.update_state_if_required(transaction_id)

                    logger.debug(f'received event {type(event)}, {transaction_id}')

                    if self.is_heartbeat_event(event):
                        break

                    log_event = self.convert_stream_event_to_log_event(event, transaction_id)
                    if log_event is None:
                        continue

                    logger.debug(f'event matched {transaction_id}, {log_event.db_name}, {log_event.table_name}')

                    if self.settings.debug_log_level:
                        # records serialization is heavy, only do it with debug log enabled
                        logger.debug(
                            f'store event {transaction_id}, '
                            f'event type: {log_event.event_type}, '
                            f'database: {log_event.db_name} '
                            f'table: {log_event.table_name} '
                            f'records: {log_event.records}',
                        )

                    self.data_writer.store_event(log_event)

                    if last_read_count >= BinlogReplicator.MAX_EVENTS_PER_TICK:
                        break

                self.update_state_if_required(last_transaction_id)
                self.clear_old_binlog_if_required()

            except OperationalError as e:
                logger.error(f'operational error {str(e)}', exc_info=True)
                time.sleep(15)
            except Exception as e:
                logger.error(f'unhandled error {str(e)}', exc_info=True)
                raise

        logger.info('stopping binlog_replicator')
        self.data_writer.close_all()
        self.update_state_if_required(last_transaction_id, force=True)
        logger.info('stopped')

    def update_state_if_required(self, transaction_id, force: bool = False):
        curr_time = time.time()
        if curr_time - self.last_state_update < BinlogReplicator.SAVE_UPDATE_INTERVAL and not force:
            return
        if not os.path.exists(self.replicator_settings.data_dir):
            os.mkdir(self.replicator_settings.data_dir)
        self.state.prev_last_seen_transaction = self.state.last_seen_transaction
        self.state.last_seen_transaction = transaction_id
        self.state.save()
        self.last_state_update = curr_time
        #print('saved state', transaction_id, self.state.prev_last_seen_transaction)
