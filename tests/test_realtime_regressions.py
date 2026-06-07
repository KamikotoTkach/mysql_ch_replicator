import importlib
import sys
import types
import uuid

import pytest

from mysql_ch_replicator.clickhouse_api import ClickhouseApi
from mysql_ch_replicator.table_structure import TableField, TableStructure


@pytest.fixture
def realtime_class(monkeypatch):
    module_name = 'mysql_ch_replicator.db_replicator_realtime'
    existing_module = sys.modules.get(module_name)
    if existing_module is not None:
        yield existing_module.DbReplicatorRealtime
        return

    used_fake_binlog = False
    try:
        module = importlib.import_module(module_name)
    except TypeError:
        fake_binlog_replicator = types.ModuleType('mysql_ch_replicator.binlog_replicator')
        fake_binlog_replicator.LogEvent = object
        fake_binlog_replicator.EventType = object
        monkeypatch.setitem(sys.modules, 'mysql_ch_replicator.binlog_replicator', fake_binlog_replicator)
        sys.modules.pop(module_name, None)
        module = importlib.import_module(module_name)
        used_fake_binlog = True

    yield module.DbReplicatorRealtime

    if used_fake_binlog:
        sys.modules.pop(module_name, None)


def test_realtime_delete_uuid_primary_key_is_quoted(realtime_class):
    value = uuid.UUID('122df378-9855-3419-a6d5-56e2066f2b6e')

    structure = TableStructure(
        fields=[TableField(name='id', field_type='UUID')],
        primary_keys=['id'],
    )
    structure.preprocess()

    record_id = realtime_class.__new__(realtime_class)._get_record_id(
        structure,
        [value.bytes],
    )

    api = ClickhouseApi.__new__(ClickhouseApi)
    assert api._format_delete_key(record_id) == "'122df378-9855-3419-a6d5-56e2066f2b6e'"


def test_realtime_delete_string_primary_key_is_quoted_and_escaped():
    api = ClickhouseApi.__new__(ClickhouseApi)

    assert api._format_delete_key("abc'def") == "'abc\\'def'"


def test_realtime_delete_composite_string_key_is_formatted():
    api = ClickhouseApi.__new__(ClickhouseApi)

    assert api._format_delete_key(('account-1', 'USD')) == "'account-1','USD'"


def test_realtime_create_table_is_idempotent(realtime_class):
    mysql_structure = TableStructure(table_name='flyway_schema_history')
    ch_structure = TableStructure(table_name='flyway_schema_history')

    class FakeConverter:
        def parse_create_table_query(self, query):
            return mysql_structure, ch_structure

    class FakeConfig:
        def is_table_matches(self, table_name):
            return True

        def get_indexes(self, database, table_name):
            return None

        def get_partition_bys(self, database, table_name):
            return None

    class FakeClickhouseApi:
        def create_table(self, structure, additional_indexes=None, additional_partition_bys=None):
            self.created_structure = structure

    class FakeState:
        tables_structure = {}

    class FakeReplicator:
        database = 'punishments'
        converter = FakeConverter()
        config = FakeConfig()
        clickhouse_api = FakeClickhouseApi()
        state = FakeState()

        def get_target_table_name(self, table_name):
            return table_name

    realtime = realtime_class.__new__(realtime_class)
    realtime.replicator = FakeReplicator()

    realtime.handle_create_table_query('CREATE TABLE `flyway_schema_history` (`installed_rank` INT)', 'punishments')

    assert realtime.replicator.clickhouse_api.created_structure.if_not_exists is True
