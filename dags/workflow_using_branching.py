from airflow.decorators import dag, task, branch_task
from airflow.utils.trigger_rule import TriggerRule
import random
from datetime import datetime 

@dag(schedule = None, start_date=datetime(2024, 1, 1), catchup=False, tags=["learnings"])
def workflow_using_branching():
    
    @task 
    def extract_sales():
        """ Simulates extracting sales amount """
        sales = random.randint(1000, 10000)
        print(f"Sales amount: ${sales}")
        return sales 
    
    @branch_task
    def decide_path(sales : int):
        """ Branch logic based on sales """
        return "high_value_task" if sales > 5000 else "low_value_task"
    
    @task
    def high_value_task(sales : int):
        print(f"{sales} - This is high value transaction")
        
    @task    
    def low_value_task(sales : int):
        print(f"{sales} - This is low value transaction")    
    
    @task(trigger_rule="one_success")   
    def finalize():
        print("finalizing the process")
    
    # Defining dependencies 
    sales = extract_sales()
    branch = decide_path(sales)
    high = high_value_task(sales)
    low = low_value_task(sales)
    final = finalize()

    
    branch >> high >> final 
    branch >> low >> final 
    
workflow_using_branching()