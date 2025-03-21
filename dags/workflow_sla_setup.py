from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime
import time
import logging 

# sla miss call back function 
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logging.warning("SLA Miss detected!")
    logging.warning(f"DAG: {dag.dag_id}")
    logging.warning(f"Tasks that missed SLA: {task_list}")
    logging.warning(f"Blocking tasks: {blocking_task_list}")
    
# Sample task function
def long_running_task():
    print("task in sleeping for 180sec")
    time.sleep(180)

with DAG(
    "workflow_sla_setup",
    schedule_interval="@daily",
    start_date=datetime(2025, 3, 19),
    catchup=False,
    sla_miss_callback=sla_miss_callback  # Attach SLA callback
) as dag:

    # Define task with SLA 
    task1 = PythonOperator(
        task_id = "long_task", 
        python_callable = long_running_task,
        sla ="0:02:00" # SLA of 2 mins
    )
    
    task1 