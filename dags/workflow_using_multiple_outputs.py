from airflow.decorators import dag, task 
from datetime import datetime 

@dag(schedule=None, start_date=datetime(2025,1,1), catchup=False, tags=["learnings"])

def workflow_using_multiple_outputs():
    
    @task(multiple_outputs=True)
    def extract():
        print("extract called")
        return {"customer" : "Alice", "age" : "25"}

    @task
    def transform1(customer):
        print("transform 1 called")
        print(f"name: {customer}")

    @task
    def transform2(age):
        print("transform 2 called")
        print(f"age: {age}")
    
    extracted_data = extract()
    transform1(extracted_data["customer"])
    transform2(extracted_data["age"])
    
workflow_using_multiple_outputs()