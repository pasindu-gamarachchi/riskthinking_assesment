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
    """
    This DAG defines a series of tasks that extract, transform, and train machine learning models on data. The pipeline is scheduled to run daily.

    Tasks:
        - data_extraction: Extracts data using the DataExtraction class.
        - convert_to_parq: Converts extracted data to Parquet format using the Transform class.
        - feature_eng: Performs feature engineering on the transformed data using the FeatureEng class.
        - ml_train: Trains machine learning models on the feature-engineered data using the MLmodel class.
    
    Dependencies:
        - data_extraction depends on nothing.
        - convert_to_parq depends on data_extraction.
        - feature_eng depends on convert_to_parq.
        - ml_train depends on feature_eng.
    """
    from extraction.data_extraction import DataExtraction

    dataExtraction = DataExtraction()
    data_extraction = PythonOperator(
        task_id='get_file',
        python_callable = dataExtraction.run
    )
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
        python_callable=transform.run
    )

    from feature_eng.feature_eng import FeatureEng

    feature_eng = FeatureEng()
    feature_eng = PythonOperator(
        task_id='feature_eng',
        python_callable=feature_eng.run
    )

    from ml_model.ml_model import MLmodel

    ml_model_train = MLmodel()
    ml_train = PythonOperator(
        task_id='ml_train',
        python_callable=ml_model_train.run
    )

    data_extraction >> convert_to_parq >> feature_eng >> ml_train

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

with DAG(default_args=default_args, dag_id='ml_train',
         start_date=datetime(2023, 5, 5), schedule_interval='@daily', catchup=False
         ) as dag:
    from ml_model.ml_model import MLmodel
    ml_model_train = MLmodel()
    ml_train = PythonOperator(
        task_id='ml_train',
        python_callable = ml_model_train.run
    )

    ml_train