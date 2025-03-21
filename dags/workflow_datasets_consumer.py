from airflow.decorators import dag, task
from airflow.datasets import Dataset 
from datetime import datetime 
import pathlib 

#define a Dataset (Local file)
MY_DATASET = Dataset("/tmp/dataset.txt")

@dag(schedule=[MY_DATASET], start_date= datetime(2025, 1, 1), catchup=False, tags=["learnings"])
def workflow_datasets_consumer():
    @task
    def process_data():
        file_path = pathlib.Path("/tmp/dataset.txt")
        if file_path.exists():
            data = file_path.read_text()
            print(f"🔄 Processing Data: {data}")
        else:
            print("⚠️ Dataset file not found!")
    
    process_data()
    
workflow_datasets_consumer()