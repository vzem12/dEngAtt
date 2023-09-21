from airflow import DAG
from airflow.operators.python import PythonOperator as PY
from zemtsov.filling_dags import 

args = {
    'owner':'zemtsov',
    'start_date':dt(2023,9,21),
    'retries':3,
    }

def filling_tabs
