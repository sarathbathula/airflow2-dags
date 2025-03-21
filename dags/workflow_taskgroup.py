from airflow.decorators import dag, task, task_group
from datetime import datetime

@dag(schedule= None, start_date=datetime(2025, 1, 1), catchup=False, tags = ["learnings"])
def workflow_taskgroup():
    @task 
    def start():
        print("start called")
        return { "data" : [1, 2, 3, 4, 5] }
    
    @task_group
    def data_extraction():
        @task
        def extract():
            print("Extracting data...")

        @task
        def transform():
            print("Transforming data...")

        @task
        def load():
            print(" Loading data...")

        extract() >> transform() >> load()
        
    @task_group
    def feature_engineering():
        @task
        def extract():
            print("Extracting data...")

        @task
        def transform():
            print("Transforming data...")

        @task
        def load():
            print(" Loading data...")
        
        extract() >> transform() >> load()

    @task_group
    def model_engineering():
        @task
        def extract():
            print("Extracting data...")

        @task
        def transform():
            print("Transforming data...")

        @task
        def load():
            print(" Loading data...")

        extract() >> transform() >> load()

    @task 
    def end():
        print("end called")

    start = start()
    extraction = data_extraction()  # Group executes as one logical unit
    processing = feature_engineering()
    model = model_engineering()
    end = end()
    
    start >> extraction >> processing >> model >> end 
    
workflow_taskgroup()