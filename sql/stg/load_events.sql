CREATE TABLE IF NOT EXISTS stg.events (
    event_id TEXT PRIMARY KEY,
    visitor_id TEXT,
    event_type TEXT,
    item_id BIGINT,
    transaction_id TEXT,
    event_ts TIMESTAMP WITHOUT TIME ZONE,
    event_date DATE,
    ingestion_date DATE NOT NULL
);

DELETE FROM stg.events WHERE ingestion_date = '{{ ds }}'::date;

WITH dedup AS (
    SELECT
        re."timestamp",
        re.visitorid,
        LOWER(re.event) AS event_type,
        re.itemid,
        re.transactionid,
        ROW_NUMBER() OVER (
            PARTITION BY re."timestamp", re.visitorid, re.itemid, COALESCE(re.transactionid, '')
            ORDER BY re.ingestion_date DESC
        ) AS rn
    FROM raw.events re
    WHERE re.ingestion_date = '{{ ds }}'::date
)
INSERT INTO stg.events (
    event_id,
    visitor_id,
    event_type,
    item_id,
    transaction_id,
    event_ts,
    event_date,
    ingestion_date
)
SELECT
    md5(CONCAT_WS('::', dedup."timestamp", dedup.visitorid, dedup.itemid, COALESCE(dedup.transactionid, ''))),
    dedup.visitorid,
    dedup.event_type,
    dedup.itemid,
    dedup.transactionid,
    TO_TIMESTAMP(dedup."timestamp" / 1000) AT TIME ZONE 'UTC',
    (TO_TIMESTAMP(dedup."timestamp" / 1000) AT TIME ZONE 'UTC')::date,
    '{{ ds }}'::date
FROM dedup
WHERE dedup.rn = 1;
