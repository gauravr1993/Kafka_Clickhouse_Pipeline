CREATE DATABASE IF NOT EXISTS iot;

-- Kafka engine table (temporary stream)
CREATE TABLE IF NOT EXISTS iot.temperature_stream (
  device_id String,
  timestamp DateTime,
  temperature Float32
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'temperature',
         kafka_group_name = 'temperature_consumer',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

-- MergeTree table (persistent storage)
CREATE TABLE IF NOT EXISTS iot.temperature_data (
  device_id String,
  timestamp DateTime,
  temperature Float32
) ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (device_id, timestamp);

-- Materialized view to consume Kafka and insert into MergeTree
CREATE MATERIALIZED VIEW IF NOT EXISTS iot.mv_temperature TO iot.temperature_data AS
SELECT * FROM iot.temperature_stream;


CREATE TABLE IF NOT EXISTS iot.anomalies (
  device_id String,
  timestamp DateTime,
  temperature Float32,
  anomaly_type String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (device_id, timestamp);