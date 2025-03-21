from airflow.decorators import dag, task 
from airflow.operators.python import is_venv_installed

from datetime import datetime 
import logging 

log = logging.getLogger(__name__)

if not is_venv_installed():
    log.warning("The workflow_python_virtualenv DAG requires virtualenv, please install it.")
else:
    @dag(schedule = None, start_date = datetime(2025,1,1), catchup=False, tags=['learnings'])
    def workflow_python_virtualenv():
        
        @task.virtualenv(requirements=['pandas', 'numpy'])
        def compute_heavy_task():
            import numpy as np 
            import pandas as pd 
            print("running heavy computation")
            arr = np.random.rand(10, 100)
            df = pd.Dataframe(arr)
            print(df.head())
            
            compute_heavy_task()
            
    workflow_python_virtualenv()