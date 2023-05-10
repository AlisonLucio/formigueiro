from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from helpers.apis.api_compras import call_api_gov

dag_conf= call_api_gov['DAG_CONFIG']

dag = DAG(
    dag_id=dag_conf['DAG_ID'],
    default_args=dag_conf['DEFAULT_ARGS'],
    schedule_interval=dag_conf['SCHEDULE_INTERVAL'],
    catchup=dag_conf['CATCHUP']
)

dag_init = DummyOperator(task_id = 'dag_init', dag=dag)
dag_end = DummyOperator(task_id= 'dag_end', dag=dag)

task_config_list= call_api_gov['TASK_CONFIG']['TASK_LIST']

for task_index in task_config_list:

    call_function = SimpleHttpOperator(
        task_id= task_index['task_id'],
        method= task_index['method'],
        http_conn_id= task_index['http_conn_id'],
        endpoint= task_index['endpoint'],
        data= (task_index['data']),
        headers= task_index['headers'],
        dag=dag
    )

    dag_init >> call_function >> dag_end