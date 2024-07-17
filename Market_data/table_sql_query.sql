DROP TABLE IF EXISTS aldykarinacp_coderhouse.stage_market_data_daily;

CREATE TABLE aldykarinacp_coderhouse.stage_market_data_daily(
    ID VARCHAR(50) PRIMARY KEY,
    date TIMESTAMP,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume BIGINT,
    symbol VARCHAR(10),
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) SORTKEY (date, symbol);