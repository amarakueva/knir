from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator

PG_CONN_ID = "retail_postgres"

with DAG(
    dag_id="retailrocket_full_load",
    description="One-shot full ingestion + ELT for the Retailrocket dataset",
    start_date=datetime(2015, 5, 1),
    schedule=None,  # run manually when a full reload is needed
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/sql"],
    tags=["retailrocket", "elt", "full"],
) as dag:
    start = EmptyOperator(task_id="start")

    prepare_schemas = SQLExecuteQueryOperator(
        task_id="prepare_schemas",
        conn_id=PG_CONN_ID,
        sql="setup_schemas.sql",
    )

    ingest_raw_full = BashOperator(
        task_id="ingest_raw_full",
        bash_command=(
            "python /opt/airflow/scripts/ingest_raw.py "
            "--mode full "
            "--date {{ ds }} "
            "--source-dir /opt/airflow/archive "
            "--pg-host postgres "
            "--pg-port 5432 "
            "--pg-user ${POSTGRES_USER:-airflow} "
            "--pg-password ${POSTGRES_PASSWORD:-airflow} "
            "--pg-db ${POSTGRES_DB:-airflow}"
        ),
    )

    stg_events = SQLExecuteQueryOperator(
        task_id="stg_events",
        conn_id=PG_CONN_ID,
        sql="stg/load_events.sql",
    )

    stg_item_properties = SQLExecuteQueryOperator(
        task_id="stg_item_properties",
        conn_id=PG_CONN_ID,
        sql="stg/load_item_properties.sql",
    )

    stg_category_tree = SQLExecuteQueryOperator(
        task_id="stg_category_tree",
        conn_id=PG_CONN_ID,
        sql="stg/load_category_tree.sql",
    )

    dim_date = SQLExecuteQueryOperator(
        task_id="dim_date",
        conn_id=PG_CONN_ID,
        sql="dm/build_dim_date.sql",
    )

    dim_item = SQLExecuteQueryOperator(
        task_id="dim_item",
        conn_id=PG_CONN_ID,
        sql="dm/build_dim_item.sql",
    )

    fact_daily_events = SQLExecuteQueryOperator(
        task_id="fact_daily_events",
        conn_id=PG_CONN_ID,
        sql="dm/build_fact_daily_events.sql",
    )

    fact_item_daily = SQLExecuteQueryOperator(
        task_id="fact_item_daily",
        conn_id=PG_CONN_ID,
        sql="dm/build_fact_item_daily.sql",
    )

    dq_event_counts_match = SQLCheckOperator(
        task_id="dq_event_counts_match",
        conn_id=PG_CONN_ID,
        sql="""
        WITH raw_dedup AS (
            SELECT COUNT(*) AS cnt
            FROM (
                SELECT 1
                FROM raw.events re
                WHERE re.ingestion_date = '{{ ds }}'
                GROUP BY re."timestamp", re.visitorid, re.itemid, COALESCE(re.transactionid, '')
            ) dedup
        )
        SELECT
            (SELECT COUNT(*) FROM stg.events WHERE ingestion_date = '{{ ds }}')
            =
            COALESCE((SELECT cnt FROM raw_dedup), 0);
        """,
    )

    dq_event_ts_not_null = SQLCheckOperator(
        task_id="dq_event_ts_not_null",
        conn_id=PG_CONN_ID,
        sql="""
        SELECT COUNT(*) = 0
        FROM stg.events
        WHERE ingestion_date = '{{ ds }}' AND event_ts IS NULL;
        """,
    )

    dq_fact_daily_populated = SQLCheckOperator(
        task_id="dq_fact_daily_populated",
        conn_id=PG_CONN_ID,
        sql="""
        WITH impacted AS (
            SELECT DISTINCT event_date
            FROM stg.events
            WHERE ingestion_date = '{{ ds }}'
        )
        SELECT CASE
            WHEN (SELECT COUNT(*) FROM impacted) = 0 THEN TRUE
            ELSE NOT EXISTS (
                SELECT 1
                FROM impacted i
                LEFT JOIN dm.fact_daily_events f ON f.date_key = i.event_date
                WHERE f.date_key IS NULL
            )
        END;
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> prepare_schemas >> ingest_raw_full

    ingest_raw_full >> [stg_events, stg_item_properties, stg_category_tree]

    stg_events >> dim_date
    [stg_item_properties, stg_category_tree] >> dim_item

    dim_date >> fact_daily_events
    [stg_events, dim_item] >> fact_item_daily
    stg_events >> fact_daily_events

    [stg_events, fact_daily_events] >> dq_event_counts_match
    stg_events >> dq_event_ts_not_null
    [fact_daily_events, fact_item_daily] >> dq_fact_daily_populated

    [dq_event_counts_match, dq_event_ts_not_null, dq_fact_daily_populated] >> end
