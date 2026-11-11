CREATE TABLE dwh.meta_files (
    file_name text NOT NULL,
    sha256 text NOT NULL,
    report_dt date NOT NULL,
    load_kind text NOT NULL CHECK (load_kind IN ('full','transactions')),
    archive_name text NOT NULL UNIQUE,
    archived boolean NOT NULL DEFAULT false,
    load_dt timestamp NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY(file_name, sha256)
);
-- Не выдумываем время старых отчётов: их прежняя дата сохраняется до пересчёта.
ALTER TABLE dwh.rep_fraud ALTER COLUMN report_dt TYPE timestamp USING report_dt::timestamp;
