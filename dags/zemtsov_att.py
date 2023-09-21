from airflow import DAG
from airflow.operators.python import PythonOperator as PY
from zemtsov.filling_dicts import filling_dicts 

args = {
  'owner':'zemtsov',
  'start_date':dt(2023,9,21),
  'retries':3,
  }

with DAG('zemtsov_filling_dicts', 
          description='Filling Users and Channels Dictionaries',
          schedule_interval='@once',
          catchup=False,
          max_active_runs=1,
          default_args = args,
          params={'labels':{'env':'prod', 'priority':'high'}}) as dag:
  filling = PY(task_id='zemtsov_filling_dicts',
               python_callable=filling_dicts,
               provide_context=True)
  filling
        
