from airflow.decorators import dag, task
from airflow.models.baseoperator  import chain
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from datetime import datetime 

@dag(schedule = None, start_date = datetime(2025, 1, 1), catchup=False, tags=["learnings"])
def workflow_using_short_circuit():
    
    @task.short_circuit()
    def check_condition(condition):
        return condition

    ds_true = [EmptyOperator(task_id=f"true_{i}") for i in [1, 2]]
    ds_false = [EmptyOperator(task_id=f"false_{i}") for i in [1, 2]]

    condition_is_true = check_condition.override(task_id="condition_is_true")(condition=True)
    condition_is_false = check_condition.override(task_id="condition_is_false")(condition=False)

    chain(condition_is_true, *ds_true)
    chain(condition_is_false, *ds_false)
    # [END howto_operator_short_circuit]

    # [START howto_operator_short_circuit_trigger_rules]
    [task_1, task_2, task_3, task_4, task_5, task_6] = [
        EmptyOperator(task_id=f"task_{i}") for i in range(1, 7)
    ]

    task_7 = EmptyOperator(task_id="task_7", trigger_rule=TriggerRule.ALL_DONE)

    short_circuit = check_condition.override(task_id="short_circuit", ignore_downstream_trigger_rules=False)(
        condition=False
    )

    chain(task_1, [task_2, short_circuit], [task_3, task_4], [task_5, task_6], task_7)

    
workflow_using_short_circuit()