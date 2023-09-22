from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

servers = """
    zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties;
    """

dag = DAG(dag_id = "kafka_server_1",description = "zookeeper",
	schedule_interval = None,
	start_date=datetime(2021,5,20),catchup = False)

start_server_2 = BashOperator(
    task_id='start-server',
    bash_command=servers,
    dag=dag)
    
start_task = DummyOperator(task_id='start_task', dag=dag)

start_task >> start_server_2
