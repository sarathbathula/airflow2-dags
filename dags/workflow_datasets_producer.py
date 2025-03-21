from airflow.decorators import dag, task
from airflow.datasets import Dataset 
from datetime import datetime 
import pathlib 

#define a Dataset (Local file)
MY_DATASET = Dataset("/tmp/dataset.txt")

@dag(schedule = None, start_date= datetime(2025, 1, 1), catchup=False, tags=["learnings"])
def workflow_datasets_producer():
    @task(outlets=[MY_DATASET])
    def generate_dag():
        file_path = pathlib.Path("/tmp/dataset.txt")
        file_path.write_text("Hello, Airflow datasets")
        print(f"Data is written to file")
    
    generate_dag()
    
workflow_datasets_producer()