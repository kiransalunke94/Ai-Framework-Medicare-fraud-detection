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
drug_data_sort = """
    source /home/kiran/project/venv/bin/activate;
    cd //home/kiran/medicare/src_deploy
    python3 drug_data_sort_deployment.py;
    """

pay_data_sort = """
    source /home/kiran/project/venv/bin/activate;
    cd //home/kiran/medicare/src_deploy
    python3 Payment_data_sort_deployment.py;
    """
    
medicines_sort = """
    source /home/kiran/project/venv/bin/activate;
    cd //home/kiran/medicare/src_deploy
    python3 medicines.py;
    """
final_merging = """
    source /home/kiran/project/venv/bin/activate;
    cd //home/kiran/medicare/src_deploy
    python3 final_merging.py;
    """

predictions = """
    source /home/kiran/project/venv/bin/activate;
    cd //home/kiran/medicare/src_deploy
    python3 Prediction_deployment.py;
    """
    
ui = """
    source /home/kiran/project/venv/bin/activate;
    cd //home/kiran/medicare/src_deploy/fraud_display
    python3 manage.py runserver localhost:8004;
    """

dag = DAG(dag_id = "medicare_pipeline",description = "kafka",
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
    
drug_sort = BashOperator(
    task_id='Drug_data_sort',
    bash_command=drug_data_sort,
    dag=dag)   

pay_sort = BashOperator(
    task_id='pay_data_sort',
    bash_command=pay_data_sort,
    dag=dag)   

medicine_sort = BashOperator(
    task_id='medicine_data_sort',
    bash_command=medicines_sort,
    dag=dag)

final_merge = BashOperator(
    task_id='Drug_pay_medicine_merge',
    bash_command=final_merging,
    dag=dag)
    
predict = BashOperator(
    task_id='Prediction_ops',
    bash_command=predictions,
    dag=dag) 
    
display = BashOperator(
    task_id='Fraud_display',
    bash_command=ui,
    dag=dag)   

start_task = DummyOperator(task_id='start_task', dag=dag)
    
start_task >> start_drug_producer >> start_pay_producer >> [drug_sort,pay_sort,medicine_sort] >> final_merge >> predict >> display


