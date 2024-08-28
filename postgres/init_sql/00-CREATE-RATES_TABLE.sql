DROP TABLE IF EXISTS rates;
DROP INDEX IF EXISTS event_time_idx;
CREATE TABLE IF NOT EXISTS rates(event_id BIGINT, event_time BIGINT, ccy_couple VARCHAR, rate real);
CREATE INDEX IF NOT EXISTS event_time_idx on rates (ccy_couple, event_time desc, rate desc);
\copy rates FROM '/custom_data/rates_sample.csv' WITH DELIMITER ',' CSV HEADER;
