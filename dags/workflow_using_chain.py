from airflow.decorators import dag, task 
from airflow.models.baseoperator import chain 
from datetime import datetime

@dag(schedule=None, start_date=datetime(2025,1,1), catchup=False, tags=["learnings"]) 
def workflow_using_chain():
    
    @task    
    def task1():
        print("this is task1")

    @task    
    def task2():
        print("this is task2")
        
    @task    
    def task3():
        print("this is task3")
        
    @task    
    def task4():
        print("this is task4")
        
    @task    
    def task5():
        print("this is task5")
    
    t1 = task1()
    t2 = task2()
    t3 = task3()
    t4 = task4()
    t5 = task5()
    
    
    chain(t1, [t2, t3], [t4, t5])
    
workflow_using_chain()