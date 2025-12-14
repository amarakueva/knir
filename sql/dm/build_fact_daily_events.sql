CREATE TABLE IF NOT EXISTS dm.fact_daily_events (
    date_key DATE,
    event_type TEXT,
    total_events BIGINT,
    unique_visitors BIGINT,
    unique_items BIGINT,
    PRIMARY KEY (date_key, event_type)
);

WITH impacted_days AS (
    SELECT DISTINCT event_date
    FROM stg.events
    WHERE ingestion_date = '{{ ds }}'::date
)
INSERT INTO dm.fact_daily_events (
    date_key,
    event_type,
    total_events,
    unique_visitors,
    unique_items
)
SELECT
    se.event_date AS date_key,
    se.event_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT se.visitor_id) AS unique_visitors,
    COUNT(DISTINCT se.item_id) AS unique_items
FROM stg.events se
JOIN impacted_days d ON d.event_date = se.event_date
GROUP BY se.event_date, se.event_type
ON CONFLICT (date_key, event_type) DO UPDATE
SET
    total_events = EXCLUDED.total_events,
    unique_visitors = EXCLUDED.unique_visitors,
    unique_items = EXCLUDED.unique_items;
