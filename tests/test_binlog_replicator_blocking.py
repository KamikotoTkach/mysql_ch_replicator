import os
import tempfile
import threading
import time
import types
import sys

try:
    import mysql.connector  # noqa: F401
except ImportError:
    mysql_module = types.ModuleType('mysql')
    mysql_connector_module = types.ModuleType('mysql.connector')
    mysql_connector_module.connect = lambda **kwargs: None
    mysql_connector_module.errors = types.SimpleNamespace(DatabaseError=Exception)
    mysql_module.connector = mysql_connector_module
    sys.modules.setdefault('mysql', mysql_module)
    sys.modules.setdefault('mysql.connector', mysql_connector_module)

from mysql_ch_replicator.binlog_replicator import BinlogReplicator, EventType
from mysql_ch_replicator.config import BinlogReplicatorSettings, MysqlSettings
from mysql_ch_replicator.pymysqlreplication.event import QueryEvent
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


class FakeStream:
    def __init__(self, events):
        self.events = list(events)
        self.log_file = 'mysql-bin.000001'
        self.log_pos = 4
        self.fetchone_calls = 0

    def fetchone(self):
        self.fetchone_calls += 1
        if not self.events:
            return None
        event = self.events.pop(0)
        self.log_pos = event.log_pos
        return event


class BlockingFakeStream:
    log_file = 'mysql-bin.000001'
    log_pos = 4

    def __init__(self, entered_event):
        self.entered_event = entered_event

    def fetchone(self):
        self.entered_event.set()
        time.sleep(60)


class FakeDataWriter:
    def __init__(self):
        self.events = []
        self.closed = False
        self.remove_old_files_calls = 0

    def store_event(self, log_event):
        self.events.append(log_event)

    def remove_old_files(self, ts_from):
        self.remove_old_files_calls += 1

    def close_all(self):
        self.closed = True


class FakeState:
    def __init__(self):
        self.last_seen_transaction = None
        self.prev_last_seen_transaction = None
        self.save_calls = 0

    def save(self):
        self.save_calls += 1


class FakeKiller:
    instances = []

    def __init__(self):
        self.kill_now = False
        FakeKiller.instances.append(self)


class FakeHeartbeatEvent:
    log_pos = 110


class FakeQueryEvent(QueryEvent):
    def __init__(self, schema, query, log_pos=120):
        self.schema = schema
        self.query = query
        self.log_pos = log_pos


class FakeWriteRowsEvent(WriteRowsEvent):
    def __init__(self, schema='test_db', table='test_table', rows=None, log_pos=130):
        self.schema = schema
        self.table = table
        self._rows = rows or [{'values': {'id': 1, 'name': 'Alice'}}]
        self.log_pos = log_pos

    @property
    def rows(self):
        return self._rows


def make_replicator(monkeypatch, events):
    tmp_dir = tempfile.TemporaryDirectory()
    settings = FakeSettings(tmp_dir.name)

    monkeypatch.setattr(
        'mysql_ch_replicator.binlog_replicator.BinLogStreamReader',
        lambda **kwargs: FakeStream(events),
    )

    replicator = BinlogReplicator(settings)
    replicator._tmp_dir = tmp_dir
    replicator.data_writer = FakeDataWriter()
    replicator.state = FakeState()
    replicator.last_state_update = 0
    replicator.last_binlog_clear_time = 0
    return replicator


def run_once(monkeypatch, replicator):
    FakeKiller.instances = []

    def stop_after_tick(self):
        FakeKiller.instances[0].kill_now = True

    monkeypatch.setattr('mysql_ch_replicator.binlog_replicator.GracefulKiller', FakeKiller)
    monkeypatch.setattr(replicator, 'clear_old_binlog_if_required', stop_after_tick.__get__(replicator, BinlogReplicator))
    replicator.run()


def test_binlog_stream_is_blocking_and_uses_heartbeat(monkeypatch):
    captured_kwargs = {}

    def create_stream(**kwargs):
        captured_kwargs.update(kwargs)
        return FakeStream([])

    with tempfile.TemporaryDirectory() as tmp_dir:
        monkeypatch.setattr('mysql_ch_replicator.binlog_replicator.BinLogStreamReader', create_stream)
        BinlogReplicator(FakeSettings(tmp_dir))

    assert captured_kwargs['blocking'] is True
    assert captured_kwargs['slave_heartbeat'] == BinlogReplicator.SLAVE_HEARTBEAT_INTERVAL
    assert WriteRowsEvent in captured_kwargs['only_events']
    assert QueryEvent in captured_kwargs['only_events']


def test_heartbeat_breaks_tick_and_does_not_write_event(monkeypatch):
    replicator = make_replicator(monkeypatch, [FakeHeartbeatEvent()])
    monkeypatch.setattr(replicator, 'is_heartbeat_event', lambda event: isinstance(event, FakeHeartbeatEvent))

    run_once(monkeypatch, replicator)

    assert replicator.stream.fetchone_calls == 1
    assert replicator.data_writer.events == []
    assert replicator.state.last_seen_transaction == ('mysql-bin.000001', 110)
    assert replicator.data_writer.closed is True


def test_none_event_breaks_tick_without_sleeping_or_reconnecting(monkeypatch):
    replicator = make_replicator(monkeypatch, [])

    run_once(monkeypatch, replicator)

    assert replicator.stream.fetchone_calls == 1
    assert replicator.data_writer.events == []
    assert replicator.data_writer.closed is True


def test_useful_event_is_stored_before_heartbeat_tick(monkeypatch):
    replicator = make_replicator(monkeypatch, [FakeWriteRowsEvent(log_pos=130), FakeHeartbeatEvent()])
    monkeypatch.setattr(replicator, 'is_heartbeat_event', lambda event: isinstance(event, FakeHeartbeatEvent))

    run_once(monkeypatch, replicator)

    assert replicator.stream.fetchone_calls == 2
    assert len(replicator.data_writer.events) == 1

    log_event = replicator.data_writer.events[0]
    assert log_event.transaction_id == ('mysql-bin.000001', 130)
    assert log_event.db_name == 'test_db'
    assert log_event.table_name == 'test_table'
    assert log_event.event_type == EventType.ADD_EVENT.value
    assert log_event.records == [[1, 'Alice']]


def test_query_event_is_stored_and_query_db_name_overrides_schema(monkeypatch):
    event = FakeQueryEvent(schema='wrong_db', query='ALTER TABLE `right_db`.`test_table` ADD COLUMN value int', log_pos=140)
    replicator = make_replicator(monkeypatch, [event, FakeHeartbeatEvent()])
    monkeypatch.setattr(replicator, 'is_heartbeat_event', lambda event: isinstance(event, FakeHeartbeatEvent))

    run_once(monkeypatch, replicator)

    assert len(replicator.data_writer.events) == 1
    log_event = replicator.data_writer.events[0]
    assert log_event.db_name == 'right_db'
    assert log_event.event_type == EventType.QUERY.value
    assert log_event.records == 'ALTER TABLE `right_db`.`test_table` ADD COLUMN value int'


def test_max_events_per_tick_runs_periodic_tasks_without_waiting_for_heartbeat(monkeypatch):
    old_limit = BinlogReplicator.MAX_EVENTS_PER_TICK
    BinlogReplicator.MAX_EVENTS_PER_TICK = 3
    try:
        events = [
            FakeWriteRowsEvent(log_pos=101),
            FakeWriteRowsEvent(log_pos=102),
            FakeWriteRowsEvent(log_pos=103),
            FakeWriteRowsEvent(log_pos=104),
        ]
        replicator = make_replicator(monkeypatch, events)

        run_once(monkeypatch, replicator)

        assert replicator.stream.fetchone_calls == 3
        assert len(replicator.data_writer.events) == 3
        assert replicator.state.last_seen_transaction == ('mysql-bin.000001', 103)
    finally:
        BinlogReplicator.MAX_EVENTS_PER_TICK = old_limit


def test_blocking_read_does_not_reconnect_in_idle(monkeypatch):
    captured_kwargs = {}
    entered = threading.Event()

    def create_stream(**kwargs):
        captured_kwargs.update(kwargs)
        return BlockingFakeStream(entered)

    with tempfile.TemporaryDirectory() as tmp_dir:
        monkeypatch.setattr('mysql_ch_replicator.binlog_replicator.BinLogStreamReader', create_stream)
        replicator = BinlogReplicator(FakeSettings(tmp_dir))
        worker = threading.Thread(target=replicator.read_stream_event, daemon=True)
        worker.start()

        assert entered.wait(1)
        assert worker.is_alive()
        assert captured_kwargs['blocking'] is True
        assert captured_kwargs['slave_heartbeat'] == BinlogReplicator.SLAVE_HEARTBEAT_INTERVAL
