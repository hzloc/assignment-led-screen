DROP TABLE IF EXISTS rates;
CREATE TABLE rates_at_five_pm(
    ccy_couple VARCHAR,
    RATE REAL,
    ts timestamp default (now() at time zone 'utc')

);
