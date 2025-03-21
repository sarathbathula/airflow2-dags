from airflow.decorators import dag, task 
from datetime import datetime 

@dag(schedule=None, start_date=datetime(2025,1,1), catchup=False, tags=["learnings"])
def workflow_using_taskflow_api():
    
    @task 
    def extract():
        print("extract called")
        return { "data" : [1, 2, 3, 4, 5] }
    
    @task     
    def transform(data):
        print("transform called")
        return [ x**2 for x in data["data"]]
    
    @task 
    def load(data):
        print("load is called")
        print(f"Loaded data: {data}")
        
    # extract_data = extract()
    # transform_data = transform(extract_data)
    # load(transform_data)
    
    load(transform(extract()))
    
workflow_using_taskflow_api()