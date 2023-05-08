from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'pas',
    'retry': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(default_args=default_args, dag_id='e2e_pipeline',
         start_date=datetime(2023, 5, 5), schedule_interval='@daily'
         ) as dag:
    from extraction.data_extraction import DataExtraction

    dataExtraction = DataExtraction()
    data_extraction = PythonOperator(
        task_id='get_file',
        python_callable = dataExtraction.run
    )

    data_extraction

with DAG(default_args=default_args, dag_id='convert_to_parq',
         start_date=datetime(2023, 5, 5), schedule_interval='@daily', catchup=False
         ) as dag:
    from transform.transform import Transform
    data_types = {
        'Symbol': str,
        'Date': str,
        'Open': float,
        'High': float,
        'Low': float,
        'Close': float,
        'Adj Close': float,
        'Volume': float
    }
    transform = Transform(data_types)
    convert_to_parq = PythonOperator(
        task_id='to_parquet',
        python_callable = transform.run
    )

    convert_to_parq

with DAG(default_args=default_args, dag_id='feature_eng',
         start_date=datetime(2023, 5, 5), schedule_interval='@daily', catchup=False
         ) as dag:
    from feature_eng.feature_eng import FeatureEng
    feature_eng = FeatureEng()
    feature_eng = PythonOperator(
        task_id='feature_eng',
        python_callable = feature_eng.run
    )

    feature_eng