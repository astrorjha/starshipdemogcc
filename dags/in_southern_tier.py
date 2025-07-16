from airflow.decorators import dag, task_group, task
from datetime import datetime
from airflow.datasets import Dataset
from airflow.models.param import Param


@dag(
    dag_id='in_southern_tier',
    start_date=datetime(2024, 11, 1),
    schedule="0 13,15,17,19 * * *",
    default_args={
        "owner": "Data Engineering",
        "retries": 2,
    },
    tags=['compliance','observe'],
    catchup=False,
    params={
        "counties": Param(
            ['Broome', 'Tioga', 'Chemung', 'Steuben', 'Schuyler', 'Tompkins', 'Cortland'],
            type="array",
        )
    },
)
def dag_from_config():

    @task
    def get_countries(**context):
        return context["params"]["counties"]

    @task_group
    def process_countries(county):

        @task
        def extract_data(county):
            # simulating an outage on Sundays and Tuesdays
            import time
            if datetime.now().weekday() in [6, 1]:
                for i in range(100):
                    print(f"Cannot extract data for {county} - API failure - retrying {i}...")
                    time.sleep(90)
            
            return f"Data extracted for {county}"

        @task
        def transform_data(county):

            return f"Data transformed for {county}"

        @task
        def load_data_to_s3(county):
            return f"Data loaded for {county}"
        
        _extracted_data = extract_data(county)
        _transformed_data = transform_data(_extracted_data)
        _loaded_data = load_data_to_s3(_transformed_data)

    countries = get_countries()

    processed_countries = process_countries.expand(county=countries)

    @task(outlets=[Dataset('s3://compliance-reports/new_york/southern_tier/')])
    def update_dataset():
        return "Dataset updated"

    processed_countries >> update_dataset()


dag_from_config()
