-- Cube Maker State Table
-- Tracks which Parquet files have been successfully processed for incremental reading
CREATE TABLE IF NOT EXISTS batch_processor_state (
    id BIGSERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL UNIQUE,
    file_timestamp TIMESTAMP NOT NULL,
    batch_id UUID NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

COMMENT ON TABLE batch_processor_state IS 'Tracks processed Parquet files for incremental reading (Topic 4)';
COMMENT ON COLUMN batch_processor_state.file_name IS 'Name of the Parquet file processed (e.g., netflow-p0-2026-08-27T10-30-00Z-abc.parquet)';
COMMENT ON COLUMN batch_processor_state.file_timestamp IS 'Timestamp extracted from filename (UTC)';
COMMENT ON COLUMN batch_processor_state.batch_id IS 'UUID of the batch run that processed this file';
COMMENT ON COLUMN batch_processor_state.processed_at IS 'When this file was marked as processed';

CREATE INDEX idx_file_timestamp ON batch_processor_state(file_timestamp);
CREATE INDEX idx_batch_id ON batch_processor_state(batch_id);
CREATE INDEX idx_file_name ON batch_processor_state(file_name);