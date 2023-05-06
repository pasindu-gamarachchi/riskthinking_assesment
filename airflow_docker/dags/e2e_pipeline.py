from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from extraction.data_extraction import DataExtraction

default_args = {
    'owner': 'pas',
    'retry': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(default_args=default_args, dag_id='e2e_pipeline',
         start_date=datetime(2023, 5, 5), schedule_interval='@daily'
         ) as dag:
    dataExtraction = DataExtraction()
    data_extraction = PythonOperator(
        task_id='get_file',
        python_callable = dataExtraction.run
    )

    data_extraction