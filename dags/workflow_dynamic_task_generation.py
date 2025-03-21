from airflow.decorators import dag, task 
from datetime import datetime 

@dag(schedule=None, start_date=datetime(2025,1, 1), catchup=False, tags=["learnings"])
def workflow_dynamic_task_generation():
    @task 
    def start():
        print("start")
        
    @task 
    def process_item(item: str):
        print(f"processing item {item}")
    
    @task 
    def end():
        print("end")
    
    start = start()
    end = end()
    
    # list of items for which tasks should be generated dynamically 
    items = [str(i) for i in range(6)]
    
    #task list 
    task_list = [ process_item(item) for item in items ]
    
    #dependency
    for i in range(len(task_list)-1):
        task_list[i] >> task_list[i+1] # Chain tasks sequentially
    
    start >> task_list[0]
    task_list[-1] >> end
    
#instantiate dag 
workflow_dynamic_task_generation = workflow_dynamic_task_generation()
    
    