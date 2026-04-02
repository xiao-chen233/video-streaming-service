CREATE TABLE IF NOT EXISTS stream (
  id VARCHAR(128) PRIMARY KEY,
  url TEXT NOT NULL,
  status VARCHAR(32) NOT NULL,
  node_id VARCHAR(128) NOT NULL,
  output_dir TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS record_file (
  id BIGSERIAL PRIMARY KEY,
  stream_id VARCHAR(128) NOT NULL REFERENCES stream(id),
  file_path TEXT NOT NULL,
  start_time TIMESTAMPTZ NOT NULL,
  end_time TIMESTAMPTZ NOT NULL,
  size BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_stream_file UNIQUE (stream_id, file_path)
);

CREATE INDEX IF NOT EXISTS idx_record_file_stream_id ON record_file(stream_id);
