from airflow.decorators import dag, task 
from airflow.utils.edgemodifier import Label 
from datetime import datetime 

@dag(schedule=None, start_date=datetime(2025,1,1), catchup=False, tags=["learnings"])
def workflow_using_labels():
    @task 
    def ingest():
        print("ingest")
        
    @task 
    def analyse():
        print("analyse")
        
    @task 
    def check():
        print("check")
        
    @task 
    def describe():
        print("describe")
        
    @task 
    def error():
        print("error")
        
    @task 
    def save():
        print("save")
        
    @task 
    def report():
        print("report")
        
    ingest = ingest()
    analyse = analyse()
    check = check()
    describe = describe()
    error = error()
    save = save()
    report = report()
    
    #labels
    label1 = Label("No errors")
    label2 = Label("Errors Found")
    
    ingest >> analyse >> check
    check >> label1 >> save >> report
    check >> label2 >> describe >> error >> report
    
workflow_using_labels()