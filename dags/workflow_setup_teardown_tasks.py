from airflow.decorators import dag, task, setup, teardown 
from datetime import datetime 


@dag(schedule= None, start_date=datetime(2025, 1, 1), catchup=False, tags = ["learnings"])
def workflow_setup_teardown_tasks():
    @setup
    def init_resources():
        print("Initializing processes like connection checks")
    
    @task
    def processing():
        print("Processing some activities")
    
    @teardown
    def cleanup_resources():
        print("Cleaninup the resources")

    start = init_resources()
    processing = processing()
    end = cleanup_resources()
    
    #setting dependencies
    start >> processing >> end 
    
workflow_setup_teardown_tasks()