from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_dello():
	return "hello"

def print_dello2(): 
        return "hello2"


dag = DAG(dag_id = "helloo",description = "hellotest",
	schedule_interval = "0 * * * *",
	start_date=datetime(2021,5,20),catchup = False)

hello_op = PythonOperator(task_id = "hel_1",python_callable=print_dello,dag=dag)

hello_op2 = PythonOperator(task_id = "hel_2",python_callable=print_dello2,dag=dag)
hello_op >> hello_op2
