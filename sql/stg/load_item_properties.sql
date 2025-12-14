CREATE TABLE IF NOT EXISTS stg.item_properties (
    item_id BIGINT,
    property TEXT,
    value TEXT,
    property_ts TIMESTAMP WITHOUT TIME ZONE,
    source_timestamp BIGINT,
    ingestion_date DATE NOT NULL,
    PRIMARY KEY (item_id, property)
);

WITH latest_props AS (
    SELECT
        rip.itemid,
        rip.property,
        rip.value,
        rip."timestamp",
        ROW_NUMBER() OVER (
            PARTITION BY rip.itemid, rip.property
            ORDER BY rip."timestamp" DESC
        ) AS rn
    FROM raw.item_properties rip
    WHERE rip.ingestion_date = '{{ ds }}'::date
)
INSERT INTO stg.item_properties (
    item_id,
    property,
    value,
    property_ts,
    source_timestamp,
    ingestion_date
)
SELECT
    latest_props.itemid,
    latest_props.property,
    latest_props.value,
    TO_TIMESTAMP(latest_props."timestamp" / 1000) AT TIME ZONE 'UTC',
    latest_props."timestamp",
    '{{ ds }}'::date
FROM latest_props
WHERE latest_props.rn = 1
ON CONFLICT (item_id, property) DO UPDATE
SET
    value = EXCLUDED.value,
    property_ts = EXCLUDED.property_ts,
    source_timestamp = EXCLUDED.source_timestamp,
    ingestion_date = EXCLUDED.ingestion_date;
