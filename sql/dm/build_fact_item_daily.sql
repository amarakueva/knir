CREATE TABLE IF NOT EXISTS dm.fact_item_daily (
    date_key DATE,
    item_id BIGINT,
    views BIGINT,
    add_to_cart BIGINT,
    transactions BIGINT,
    PRIMARY KEY (date_key, item_id)
);

WITH impacted_days AS (
    SELECT DISTINCT event_date
    FROM stg.events
    WHERE ingestion_date = '{{ ds }}'::date
)
INSERT INTO dm.fact_item_daily (
    date_key,
    item_id,
    views,
    add_to_cart,
    transactions
)
SELECT
    se.event_date AS date_key,
    se.item_id,
    COUNT(*) FILTER (WHERE se.event_type = 'view') AS views,
    COUNT(*) FILTER (WHERE se.event_type = 'addtocart') AS add_to_cart,
    COUNT(*) FILTER (WHERE se.event_type = 'transaction') AS transactions
FROM stg.events se
JOIN impacted_days d ON d.event_date = se.event_date
GROUP BY se.event_date, se.item_id
ON CONFLICT (date_key, item_id) DO UPDATE
SET
    views = EXCLUDED.views,
    add_to_cart = EXCLUDED.add_to_cart,
    transactions = EXCLUDED.transactions;
