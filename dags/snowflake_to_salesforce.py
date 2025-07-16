from airflow.decorators import dag, task
from datetime import datetime
from include.custom_operators import SalesforceBulkOperator, SQLExecuteQueryOperator
from airflow.models.baseoperator import chain
from airflow.datasets import Dataset
from airflow.sensors.external_task import ExternalTaskSensor


@dag(
    dag_display_name="Compliance Report Analysis to Salesforce",
    start_date=datetime(2024, 11, 1),
    schedule=[Dataset("snowflake://PROD/COMPLIANCE/")],
    # daily at 12pm utc
    # schedule="0 12 * * *",
    catchup=False,
    default_args={
        "owner": "Analytics Engineering",
        "retries": 2
    },
    tags=["compliance", "snowflake", "salesforce","observe","datasets"],
    template_searchpath="include/sql",
)
def snowflake_to_salesforce():

    # sense_for_snowflake = ExternalTaskSensor(
    #     task_id="sense_for_snowflake",
    #     poke_interval=60,
    #     external_dag_id="prepare_compliance_report_ny",
    #     external_task_id="transform",
    #     execution_delta=timedelta(hours=5),
    #     deferrable=True,
    # )

    extract_from_snowflake = SQLExecuteQueryOperator(
        task_id="extract_from_snowflake",
        conn_id="play_to_win_snowflake_conn",
        sql="extract_compliance.sql",
        inlets=[Dataset("snowflake://PROD/COMPLIANCE/")],
    )

    @task
    def transform_data(data):
        return data

    transformed_data = transform_data(extract_from_snowflake.output)

    load_to_salesforce = SalesforceBulkOperator(
        task_id="load_to_salesforce",
        salesforce_conn_id="play_to_win_salesforce_conn",
        operation="UPSERT",
        object_name="Compliance_Report__c",
        payload={"data": transformed_data},
    )

    @task 
    def send_report_to_nyc_compliance():
        return "Report sent to NYC Compliance"

    chain(
        # sense_for_snowflake,
        extract_from_snowflake,
        transformed_data,
        load_to_salesforce,
        send_report_to_nyc_compliance()
    )


snowflake_to_salesforce()