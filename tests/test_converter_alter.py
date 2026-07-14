from types import SimpleNamespace

import pytest

from mysql_ch_replicator.converter import (
    AlterOperationCategory,
    MysqlToClickhouseConverter,
    UnsupportedAlterOperation,
)
from mysql_ch_replicator.table_structure import TableField, TableStructure


def make_structure(*fields):
    structure = TableStructure(
        fields=[TableField(name=name, field_type=field_type) for name, field_type in fields],
        primary_keys=['id'],
    )
    structure.preprocess()
    return structure


def make_replicator(execute_command):
    mysql_structure = make_structure(('id', 'INT'))
    ch_structure = make_structure(('id', 'Int32'))
    return SimpleNamespace(
        config=SimpleNamespace(
            types_mapping={},
            is_database_matches=lambda database: True,
            is_table_matches=lambda table: True,
        ),
        database='mydb',
        target_database='mydb',
        state=SimpleNamespace(tables_structure={'mytable': (mysql_structure, ch_structure)}),
        clickhouse_api=SimpleNamespace(
            execute_command=execute_command,
            get_on_cluster_clause=lambda: '',
        ),
        get_target_table_name=lambda table: table,
    )


def test_alter_drop_check_is_ignored():
    converter = MysqlToClickhouseConverter()
    query = "ALTER TABLE `mydb`.`mytable` DROP CHECK `chk_balance_non_negative`"

    _, _, _, operations = converter.parse_alter_query(query, 'mydb')

    assert [operation.category for operation in operations] == [AlterOperationCategory.SAFE_NOOP]
    converter.convert_alter_query(query, 'mydb')


def test_alter_plan_rejects_unsupported_clause_before_applying_supported_clause():
    commands = []
    converter = MysqlToClickhouseConverter(make_replicator(commands.append))

    with pytest.raises(UnsupportedAlterOperation, match='requires_resync'):
        converter.convert_alter_query(
            "ALTER TABLE `mydb`.`mytable` ADD COLUMN amount INT, ADD PARTITION (PARTITION p1 VALUES LESS THAN (10))",
            'mydb',
        )

    assert commands == []


def test_alter_updates_state_only_after_clickhouse_ddl_succeeds():
    def fail(_):
        raise RuntimeError('ClickHouse failed')

    replicator = make_replicator(fail)
    converter = MysqlToClickhouseConverter(replicator)

    with pytest.raises(RuntimeError, match='ClickHouse failed'):
        converter.convert_alter_query(
            "ALTER TABLE `mydb`.`mytable` ADD COLUMN amount INT",
            'mydb',
        )

    mysql_structure, ch_structure = replicator.state.tables_structure['mytable']
    assert not mysql_structure.has_field('amount')
    assert not ch_structure.has_field('amount')


def test_alter_column_ddl_is_replay_safe_when_clickhouse_already_applied_it():
    commands = []
    replicator = make_replicator(commands.append)
    converter = MysqlToClickhouseConverter(replicator)

    converter.convert_alter_query(
        "ALTER TABLE `mydb`.`mytable` ADD COLUMN amount INT",
        'mydb',
    )

    mysql_structure, ch_structure = replicator.state.tables_structure['mytable']
    assert mysql_structure.has_field('amount')
    assert ch_structure.has_field('amount')
    assert 'ADD COLUMN IF NOT EXISTS' in commands[0]


@pytest.mark.parametrize('query', [
    "ALTER TABLE t ADD KEY idx_amount (amount)",
    "ALTER TABLE t DROP CONSTRAINT fk_account",
    "ALTER TABLE t ALTER CHECK chk_amount NOT ENFORCED",
    "ALTER TABLE t ADD PRIMARY KEY (id)",
    "ALTER TABLE t DROP PRIMARY KEY",
    "ALTER TABLE t ALGORITHM=INPLACE",
    "ALTER TABLE t LOCK=NONE",
    "ALTER TABLE t ENGINE=InnoDB",
])
def test_alter_mysql_metadata_operations_are_safe_noops(query):
    converter = MysqlToClickhouseConverter()

    _, _, _, operations = converter.parse_alter_query(query, 'mydb')

    assert [operation.category for operation in operations] == [AlterOperationCategory.SAFE_NOOP]


def test_alter_operation_with_equals_does_not_merge_into_following_clause():
    converter = MysqlToClickhouseConverter()

    _, _, _, operations = converter.parse_alter_query(
        "ALTER TABLE t ALGORITHM=INPLACE, ADD COLUMN amount INT",
        'mydb',
    )

    assert [operation.category for operation in operations] == [
        AlterOperationCategory.SAFE_NOOP,
        AlterOperationCategory.COLUMN_CHANGE,
    ]


@pytest.mark.parametrize('query', [
    "ALTER TABLE t ADD PARTITION (PARTITION p1 VALUES LESS THAN (10))",
    "ALTER TABLE t TRUNCATE PARTITION p1",
    "ALTER TABLE t RENAME TO t_new",
])
def test_alter_structural_operations_require_resync(query):
    converter = MysqlToClickhouseConverter()

    _, _, _, operations = converter.parse_alter_query(query, 'mydb')

    assert [operation.category for operation in operations] == [AlterOperationCategory.REQUIRES_RESYNC]
