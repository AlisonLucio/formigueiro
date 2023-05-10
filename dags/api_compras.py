from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from helpers.apis.dicts.api_compras import CALL_API_GOV, TABLE_NAME_LIST
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

dag_conf= CALL_API_GOV['DAG_CONFIG']

dag = DAG(
    dag_id=dag_conf['DAG_ID'],
    default_args=dag_conf['DEFAULT_ARGS'],
    schedule_interval=dag_conf['SCHEDULE_INTERVAL'],
    catchup=dag_conf['CATCHUP']
)

dag_init = DummyOperator(task_id = 'dag_init', dag=dag)
dag_end = DummyOperator(task_id= 'dag_end', dag=dag)

for table_name in TABLE_NAME_LIST:

    task_config_list= CALL_API_GOV['TASK_CONFIG']['PIPELINES_TABLES'][table_name]

    extration_api = SimpleHttpOperator(
        task_id= f"{task_config_list['CLOUD_FUNCTION']['task_id']}_{table_name}",
        method= task_config_list['CLOUD_FUNCTION']['method'],
        http_conn_id= task_config_list['CLOUD_FUNCTION']['http_conn_id'],
        endpoint= task_config_list['CLOUD_FUNCTION']['endpoint'],
        data= (task_config_list['CLOUD_FUNCTION']['data']),
        headers= task_config_list['CLOUD_FUNCTION']['headers'],
        dag=dag
    )

    # insert_query_job = BigQueryInsertJobOperator(
    #     task_id=f"{task_config_list['CLOUD_FUNCTION']['task_id']}_{table_name}",
    #     configuration={
    #         "query": {
    #             "query": INSERT_ROWS_QUERY,
    #             "useLegacySql": False,
    #         }
    #     },
    #     location=LOCATION,
    #     deferrable=True,
    # )


    dag_init >> extration_api >> dag_end


