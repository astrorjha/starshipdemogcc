# core DAG

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models.baseoperator import chain
from airflow.datasets import Dataset

from include.custom_operators import SQLExecuteQueryOperator, S3ToSnowflakeOperator


@dag(
    dag_display_name="Load NY compliance data from S3 to Snowflake",
    start_date=datetime(2024, 11, 1),
    schedule=(
        Dataset("s3://compliance-reports/new_york/new_york_city/")
        & Dataset("s3://compliance-reports/new_york/long_island/")
        & Dataset("s3://compliance-reports/new_york/hudson_valley_and_catskills/")
        & Dataset("s3://compliance-reports/new_york/capital_region/")
        & Dataset("s3://compliance-reports/new_york/central_new_york/")
        & Dataset("s3://compliance-reports/new_york/mohawk_valley/")
        & Dataset("s3://compliance-reports/new_york/north_country/")
        & Dataset("s3://compliance-reports/new_york/southern_tier/")
        & Dataset("s3://compliance-reports/new_york/western_new_york/")
        & Dataset("s3://compliance-reports/new_york/finger_lakes/")
    ),
    catchup=False,
    default_args={
        "owner": "Analytics Engineering",
        "retries": 2
    },
    template_searchpath="include/sql",
    tags=["compliance", "snowflake", "s3","observe","datasets"],
)
def prepare_compliance_report_ny():

    @task
    def get_s3_keys_and_table_names(**context):
        from include.utils import fetch_s3_keys_and_table_names

        date = context["logical_date"]
        s3_keys_table_names = fetch_s3_keys_and_table_names(date)
        return s3_keys_table_names

    s3_keys_table_names = get_s3_keys_and_table_names()

    extract_load = S3ToSnowflakeOperator.partial(
        task_id="extract_load",
        snowflake_conn_id="play_to_win_snowflake_conn",
        schema="COMPLIANCE",
        stage="REPORTING_STAGE",
        file_format="csv",
        pattern="compli_*.csv",
        map_index_template="Loaded to: {{ task.table }}",
    ).expand_kwargs(s3_keys_table_names)

    transform = SQLExecuteQueryOperator(
        task_id="transform",
        conn_id="play_to_win_snowflake_conn",
        sql="generate_reports.sql",
        outlets=[Dataset("snowflake://PROD/COMPLIANCE/")],
    )

    chain(extract_load, transform)


prepare_compliance_report_ny()
