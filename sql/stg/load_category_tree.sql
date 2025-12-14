CREATE TABLE IF NOT EXISTS stg.category_tree (
    category_id BIGINT PRIMARY KEY,
    parent_id BIGINT,
    ingestion_date DATE NOT NULL
);

INSERT INTO stg.category_tree (
    category_id,
    parent_id,
    ingestion_date
)
SELECT
    rct.categoryid,
    rct.parentid,
    COALESCE(rct.ingestion_date, '{{ ds }}'::date)
FROM raw.category_tree rct
ON CONFLICT (category_id) DO UPDATE
SET
    parent_id = EXCLUDED.parent_id,
    ingestion_date = EXCLUDED.ingestion_date;
