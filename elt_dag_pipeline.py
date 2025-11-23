from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import timedelta
import pendulum

# Recréer la fonction days_ago() pour Airflow 3.x
def days_ago(n):
    return pendulum.today('UTC').add(days=-n)

# Define DAG default arguments
default_args = {
    'owner': 'denis_data_engineer',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Schedule: every Friday at 11 PM
schedule = "0 23 * * 5"

# DAG definition
with DAG(
    dag_id="elt_pipeline_nyc_taxi",
    default_args=default_args,
    schedule=schedule,
    catchup=False,
    description="ELT pipeline for NYC yellow taxi data",
    tags=["nyc_taxi", "bigquery", "elt"],
) as dag:

    wait_for_last_friday = TimeDeltaSensor(
        task_id="wait_for_last_friday",
        delta=timedelta(seconds=1),
        mode="poke",
    )

    download_taxi_data = BashOperator(
        task_id="download_taxi_data",
        bash_command="""
                    gsutil cp gs://advance-path-477219-e1-data-bucket/from-git/download_taxi_data.py /tmp/download_taxi_data.py &&
                    python3 /tmp/download_taxi_data.py
                    """,
    )

    load_raw_trips_data = BashOperator(
        task_id="load_raw_trips_data",
        bash_command="""
                    gsutil cp gs://advance-path-477219-e1-data-bucket/from-git/load_raw_trips_data.py /tmp/load_raw_trips_data.py &&
                    python3 /tmp/load_raw_trips_data.py
                    """,
    )

    transform_trips_data = BashOperator(
        task_id="transform_trips_data",
        bash_command="""
                    gsutil cp gs://advance-path-477219-e1-data-bucket/from-git/transform_trips_data.py /tmp/transform_trips_data.py &&
                    python3 /tmp/transform_trips_data.py
                    """,
    )

    # Task dependencies
    wait_for_last_friday >> download_taxi_data >> load_raw_trips_data >> transform_trips_data