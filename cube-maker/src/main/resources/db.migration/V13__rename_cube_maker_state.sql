ALTER TABLE batch_processor_state RENAME TO cube_maker_state;

ALTER TABLE cube_maker_state
    RENAME CONSTRAINT batch_processor_state_pkey TO cube_maker_state_pkey;

ALTER TABLE cube_maker_state
    RENAME CONSTRAINT batch_processor_state_file_name_key TO cube_maker_state_file_name_key;

ALTER SEQUENCE batch_processor_state_id_seq RENAME TO cube_maker_state_id_seq;

COMMENT ON TABLE cube_maker_state IS
    'Tracks Parquet files processed by Cube Maker for incremental reading';
