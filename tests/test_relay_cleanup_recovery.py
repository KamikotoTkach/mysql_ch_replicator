import os
import pickle
import sys
import time
import types

if 'mysql_ch_replicator.pymysqlreplication.cpp_accelerated' not in sys.modules:
    fake_cpp_accelerated = types.ModuleType('mysql_ch_replicator.pymysqlreplication.cpp_accelerated')
    fake_cpp_accelerated.cpp_mysql_to_json = lambda data: data
    sys.modules['mysql_ch_replicator.pymysqlreplication.cpp_accelerated'] = fake_cpp_accelerated

from mysql_ch_replicator.binlog_replicator import (
    DataReader,
    DataWriter,
    FileReader,
    FileWriter,
    LogEvent,
    RelayRecovery,
    State as BinlogState,
)
from mysql_ch_replicator.config import BinlogReplicatorSettings, MysqlSettings
from mysql_ch_replicator.pymysqlreplication.row_event import WriteRowsEvent


class FakeSettings:
    def __init__(self, data_dir):
        self.mysql = MysqlSettings()
        self.binlog_replicator = BinlogReplicatorSettings(data_dir=data_dir)
        self.mysql_timezone = 'UTC'
        self.debug_log_level = False

    def is_table_matches(self, table_name):
        return True

    def is_database_matches(self, db_name):
        return True


class FakeMySQLApi:
    binlog_files = []

    def __init__(self, *args, **kwargs):
        pass

    def get_binlog_files(self):
        return self.binlog_files

    def close(self):
        pass


class FakeWriteRowsEvent(WriteRowsEvent):
    def __init__(self, schema, table='test_table', rows=None, log_pos=100):
        self.schema = schema
        self.table = table
        self._rows = rows or [{'values': {'id': log_pos}}]
        self.log_pos = log_pos

    @property
    def rows(self):
        return self._rows


class FakeStream:
    def __init__(self, events, log_file='mysql-bin.000006'):
        self.events = list(events)
        self.log_file = log_file
        self.log_pos = 4
        self.closed = False

    def fetchone(self):
        if not self.events:
            return None
        event = self.events.pop(0)
        self.log_pos = event.log_pos
        return event

    def close(self):
        self.closed = True


def write_state(data_dir, db_name, transaction_id):
    db_path = os.path.join(data_dir, db_name)
    os.makedirs(db_path, exist_ok=True)
    state_path = os.path.join(db_path, 'state.pckl')
    with open(state_path, 'wb') as f:
        pickle.dump({'last_processed_transaction': transaction_id}, f)


def write_event_file(data_dir, db_name, file_num, transaction_ids):
    db_path = os.path.join(data_dir, db_name)
    os.makedirs(db_path, exist_ok=True)
    writer = FileWriter(os.path.join(db_path, f'{file_num}.bin'))
    for transaction_id in transaction_ids:
        writer.write_event(LogEvent(
            transaction_id=transaction_id,
            db_name=db_name,
            table_name='test_table',
            records=[[transaction_id[1]]],
        ))
    writer.close()


def write_numbered_event_files(data_dir, db_name, count):
    for file_num in range(1, count + 1):
        write_event_file(
            data_dir=data_dir,
            db_name=db_name,
            file_num=file_num,
            transaction_ids=[('mysql-bin.000001', file_num * 100)],
        )


def make_files_old(data_dir, db_name):
    old_time = time.time() - 1000
    db_path = os.path.join(data_dir, db_name)
    for file_name in os.listdir(db_path):
        if file_name.endswith('.bin'):
            os.utime(os.path.join(db_path, file_name), (old_time, old_time))


def existing_bin_files(data_dir, db_name):
    db_path = os.path.join(data_dir, db_name)
    return sorted(file_name for file_name in os.listdir(db_path) if file_name.endswith('.bin'))


def read_transactions(data_dir, db_name):
    result = []
    for file_name in existing_bin_files(data_dir, db_name):
        reader = FileReader(os.path.join(data_dir, db_name, file_name))
        while True:
            event = reader.read_next_event()
            if event is None:
                break
            result.append(event.transaction_id)
        reader.close()
    return result


def test_cleanup_preserves_file_needed_by_db_replicator(tmp_path):
    data_dir = str(tmp_path)
    db_name = 'auction'
    write_numbered_event_files(data_dir, db_name, 8)
    write_state(data_dir, db_name, ('mysql-bin.000001', 300))
    make_files_old(data_dir, db_name)

    writer = DataWriter(BinlogReplicatorSettings(data_dir=data_dir))
    writer.remove_old_files(time.time())

    assert existing_bin_files(data_dir, db_name) == [
        '3.bin', '4.bin', '5.bin', '6.bin', '7.bin', '8.bin',
    ]


def test_cleanup_skips_database_when_protected_transaction_is_missing(tmp_path):
    data_dir = str(tmp_path)
    db_name = 'auction'
    write_numbered_event_files(data_dir, db_name, 8)
    write_state(data_dir, db_name, ('mysql-bin.000001', 9999))
    make_files_old(data_dir, db_name)

    writer = DataWriter(BinlogReplicatorSettings(data_dir=data_dir))
    writer.remove_old_files(time.time())

    assert existing_bin_files(data_dir, db_name) == [
        '1.bin', '2.bin', '3.bin', '4.bin', '5.bin', '6.bin', '7.bin', '8.bin',
    ]


def test_cleanup_uses_old_retention_rule_without_db_state(tmp_path):
    data_dir = str(tmp_path)
    db_name = 'auction'
    write_numbered_event_files(data_dir, db_name, 8)
    make_files_old(data_dir, db_name)

    writer = DataWriter(BinlogReplicatorSettings(data_dir=data_dir))
    writer.remove_old_files(time.time())

    assert existing_bin_files(data_dir, db_name) == [
        '3.bin', '4.bin', '5.bin', '6.bin', '7.bin', '8.bin',
    ]


def test_recovery_rebuilds_missing_local_relay_from_mysql_binlog(tmp_path, monkeypatch):
    data_dir = str(tmp_path)
    settings = FakeSettings(data_dir)
    write_event_file(data_dir, 'auction', 1, [('mysql-bin.000007', 10)])
    write_state(data_dir, 'auction', ('mysql-bin.000006', 200))

    FakeMySQLApi.binlog_files = ['mysql-bin.000006', 'mysql-bin.000007']
    monkeypatch.setattr('mysql_ch_replicator.binlog_replicator.MySQLApi', FakeMySQLApi)

    stream = FakeStream([
        FakeWriteRowsEvent(schema='auction', log_pos=100),
        FakeWriteRowsEvent(schema='economy', log_pos=150),
        FakeWriteRowsEvent(schema='auction', log_pos=200),
        FakeWriteRowsEvent(schema='auction', log_pos=300),
    ])
    monkeypatch.setattr(
        'mysql_ch_replicator.binlog_replicator.create_binlog_stream',
        lambda *args, **kwargs: stream,
    )

    assert RelayRecovery(settings).recover_if_required(databases=['auction']) is True

    assert read_transactions(data_dir, 'auction') == [
        ('mysql-bin.000006', 100),
        ('mysql-bin.000006', 200),
        ('mysql-bin.000006', 300),
    ]
    assert DataReader(settings.binlog_replicator, 'auction').get_file_with_transaction(
        [1], ('mysql-bin.000006', 200),
    ) == 1

    state = BinlogState(os.path.join(data_dir, 'state.json'))
    assert state.last_seen_transaction == ('mysql-bin.000006', 300)
    assert state.prev_last_seen_transaction == ('mysql-bin.000006', 300)

    backup_dirs = [
        item for item in os.listdir(os.path.join(data_dir, 'auction'))
        if item.startswith('recovery_backup_')
    ]
    assert backup_dirs


def test_recovery_fails_when_mysql_binlog_is_not_available(tmp_path, monkeypatch):
    data_dir = str(tmp_path)
    settings = FakeSettings(data_dir)
    write_event_file(data_dir, 'auction', 1, [('mysql-bin.000007', 10)])
    write_state(data_dir, 'auction', ('mysql-bin.000006', 200))

    FakeMySQLApi.binlog_files = ['mysql-bin.000007']
    monkeypatch.setattr('mysql_ch_replicator.binlog_replicator.MySQLApi', FakeMySQLApi)

    try:
        RelayRecovery(settings).recover_if_required(databases=['auction'])
    except RuntimeError as exc:
        assert 'required binlog file mysql-bin.000006 is not available' in str(exc)
    else:
        assert False, 'expected RuntimeError'

    assert existing_bin_files(data_dir, 'auction') == ['1.bin']
