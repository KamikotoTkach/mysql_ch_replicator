from mysql_ch_replicator.db_replicator_initial import DbReplicatorInitial


def test_initial_replication_cursor_uses_last_ordered_record_for_string_primary_key():
    records = [
        ('record_a', 'segment_a', 100),
        ('record_c', 'segment_b', 200),
        ('Record_b', 'segment_c', 300),
    ]
    primary_key_ids = [0, 1]

    python_max_primary_key = max([record[key_idx] for key_idx in primary_key_ids] for record in records)

    assert python_max_primary_key == ['record_c', 'segment_b']
    assert DbReplicatorInitial.get_last_record_primary_key(records, primary_key_ids) == ['Record_b', 'segment_c']
