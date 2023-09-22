from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

drug_producer = """
    source /home/kiran/project/venv/bin/activate;
    cd /home/kiran/medicare/kafka_src/py/
    python3 Drug_data_producer.py;
    """
pay_producer = """
    source /home/kiran/project/venv/bin/activate;
    cd /home/kiran/medicare/kafka_src/py/
    python3 pay_data_producer.py;
    """

dag = DAG(dag_id = "drug_and_payment_producer",description = "kafka",
	schedule_interval = None,
	start_date=datetime(2021,5,20),catchup = False)

start_drug_producer = BashOperator(
    task_id='Drug_data_producer',
    bash_command=drug_producer,
    dag=dag)
    
start_pay_producer = BashOperator(
    task_id='Pay_data_producer',
    bash_command=pay_producer,
    dag=dag)
       

start_task = DummyOperator(task_id='start_task', dag=dag)
    
start_task >> start_drug_producer >> start_pay_producer


