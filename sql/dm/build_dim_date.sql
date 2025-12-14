CREATE TABLE IF NOT EXISTS dm.dim_date (
    date_key DATE PRIMARY KEY,
    year SMALLINT,
    quarter SMALLINT,
    month SMALLINT,
    month_name TEXT,
    week_of_year SMALLINT,
    day_of_month SMALLINT,
    day_name TEXT
);

WITH impacted AS (
    SELECT DISTINCT event_date
    FROM stg.events
    WHERE ingestion_date = '{{ ds }}'::date
),
generated AS (
    SELECT DISTINCT event_date AS date_key
    FROM impacted
)
INSERT INTO dm.dim_date (
    date_key,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_month,
    day_name
)
SELECT
    generated.date_key,
    EXTRACT(YEAR FROM generated.date_key)::SMALLINT,
    EXTRACT(QUARTER FROM generated.date_key)::SMALLINT,
    EXTRACT(MONTH FROM generated.date_key)::SMALLINT,
    TO_CHAR(generated.date_key, 'Mon'),
    EXTRACT(WEEK FROM generated.date_key)::SMALLINT,
    EXTRACT(DAY FROM generated.date_key)::SMALLINT,
    TO_CHAR(generated.date_key, 'Dy')
FROM generated
ON CONFLICT (date_key) DO NOTHING;
