from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime 

def extract():
    return { "data" : [1, 2, 3, 4, 5]}

def transform(**kwargs):
    data = kwargs["ti"].xcom_pull(task_ids="extract")
    return [x * 2 for x in data["data"]]

def load(**kwargs):
    transformed_data = kwargs["ti"].xcom_pull(task_ids="transform")
    print(f"Loaded data: {transformed_data}")

with DAG("workflow_using_standard_operators", schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False, tags=["learnings"]) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform, provide_context=True)
    load_task = PythonOperator(task_id="load", python_callable=load, provide_context=True)
    
    extract_task >> transform_task >> load_task

    