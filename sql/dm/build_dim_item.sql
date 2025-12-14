CREATE TABLE IF NOT EXISTS dm.dim_item (
    item_id BIGINT PRIMARY KEY,
    category_id BIGINT,
    parent_category_id BIGINT,
    updated_at TIMESTAMP WITHOUT TIME ZONE
);

WITH item_categories AS (
    SELECT
        sip.item_id,
        NULLIF(sip.value, '')::BIGINT AS category_id,
        sip.property_ts,
        ROW_NUMBER() OVER (
            PARTITION BY sip.item_id
            ORDER BY sip.property_ts DESC
        ) AS rn
    FROM stg.item_properties sip
    WHERE sip.property = 'categoryid'
      AND sip.ingestion_date = '{{ ds }}'::date
)
INSERT INTO dm.dim_item (
    item_id,
    category_id,
    parent_category_id,
    updated_at
)
SELECT
    ic.item_id,
    ic.category_id,
    sct.parent_id,
    ic.property_ts
FROM item_categories ic
LEFT JOIN stg.category_tree sct ON ic.category_id = sct.category_id
WHERE ic.rn = 1
ON CONFLICT (item_id) DO UPDATE
SET
    category_id = EXCLUDED.category_id,
    parent_category_id = EXCLUDED.parent_category_id,
    updated_at = EXCLUDED.updated_at;
