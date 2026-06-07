-- schema.sql

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS replication_log;

CREATE TABLE data (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    value TEXT,
    version INT DEFAULT 1,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    operation_id UUID
);

CREATE TABLE replication_log (
    log_id SERIAL PRIMARY KEY,
    operation_type VARCHAR(10) NOT NULL,
    record_id INT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    node VARCHAR(20) NOT NULL
);

