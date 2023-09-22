from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

dag = DAG('pay_data_consumer', description='DAG to start receiving payment data',
          schedule_interval=None,
          start_date=datetime(2020, 3, 20), catchup=False)

start_task = DummyOperator(task_id='start_task', dag=dag)

commands = """
    source /home/kiran/project/venv/bin/activate;
    cd /home/kiran/medicare/src_deploy;
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 pay_data_consumer.py;
    """

   
fetch_data = BashOperator(
    task_id='start-pay-data-dumping',
    bash_command=commands,
    dag=dag)


start_task >> fetch_data
