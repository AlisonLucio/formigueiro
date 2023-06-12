from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers.apis.dicts.api_gov_br.api_compras_bq import CALL_API_GOV, TABLE_NAME_LIST, SERVICE_ACCOUNT, GCP_CONN_ID
from airflow.utils.task_group import TaskGroup
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
# from airflow.providers.google.cloud.operators.bigquery import

dag_conf= CALL_API_GOV['DAG_CONFIG']

with DAG(
    dag_id=dag_conf['DAG_ID'],
    default_args= dag_conf['DEFAULT_ARGS'],
    schedule_interval= dag_conf['SCHEDULE_INTERVAL'],
    catchup=dag_conf['CATCHUP'],
    tags=dag_conf['TAGS']
    ) as dag:

    dag_init = DummyOperator(task_id = 'dag_init', dag=dag)
    dag_end  = DummyOperator(task_id = 'dag_end', dag=dag)

    for table_name in TABLE_NAME_LIST:
        task_config_list = CALL_API_GOV['TASK_CONFIG']['PIPELINES_TABLES'][table_name]

        with TaskGroup(group_id=f'extration_api_{table_name}') as task_group:
            task_delete_file_incoming = GoogleCloudStorageDeleteOperator(
                task_id=f"{task_config_list['EXTRATION_API']['DELETE_FILE']['task_id']}-{table_name}",
                bucket_name=task_config_list['EXTRATION_API']['DELETE_FILE']['bucket_name'], 
                prefix = task_config_list['EXTRATION_API']['DELETE_FILE']['prefix'],
                gcp_conn_id = task_config_list['EXTRATION_API']['DELETE_FILE']['gcp_conn_id'],  
                impersonation_chain = task_config_list['EXTRATION_API']['DELETE_FILE']['impersonation_chain'],
                dag=dag
            )

            task_extration_api_to_incoming = SimpleHttpOperator(
                task_id= f"{task_config_list['EXTRATION_API']['API_ACCESS']['task_id']}-{table_name}",
                method= task_config_list['EXTRATION_API']['API_ACCESS']['method'],
                http_conn_id= task_config_list['EXTRATION_API']['API_ACCESS']['http_conn_id'],
                endpoint= task_config_list['EXTRATION_API']['API_ACCESS']['endpoint'],
                data= (task_config_list['EXTRATION_API']['API_ACCESS']['data']),
                headers= task_config_list['EXTRATION_API']['API_ACCESS']['headers'],
                dag=dag
            )     

        with TaskGroup(group_id=f'incoming_to_raw_bigquery_{table_name}') as task_group:

            # deletar arquivo da camada raw usando GoogleCloudStorageDeleteOperator
            task_delete_file_raw = GoogleCloudStorageDeleteOperator(
                task_id=f"{task_config_list['INCOMING_TO_RAW']['DELETE_FILE']['task_id']}-{table_name}",
                bucket_name=task_config_list['INCOMING_TO_RAW']['DELETE_FILE']['bucket_name'], 
                prefix = task_config_list['INCOMING_TO_RAW']['DELETE_FILE']['prefix'],
                gcp_conn_id = task_config_list['INCOMING_TO_RAW']['DELETE_FILE']['gcp_conn_id'],  
                impersonation_chain = task_config_list['INCOMING_TO_RAW']['DELETE_FILE']['impersonation_chain'],
                dag=dag
            )

            # transferir dados da incoming para raw com GCSToGCSOperator

            copy_single_file_incoming_to_raw = GCSToGCSOperator(
                task_id=f"{task_config_list['INCOMING_TO_RAW']['TRANSFER_FILE']['task_id']}-{table_name}",
                source_bucket=task_config_list['INCOMING_TO_RAW']['TRANSFER_FILE']['source_bucket'],
                source_object=task_config_list['INCOMING_TO_RAW']['TRANSFER_FILE']['source_object'],
                destination_bucket=task_config_list['INCOMING_TO_RAW']['TRANSFER_FILE']['destination_bucket'],
                destination_object=task_config_list['INCOMING_TO_RAW']['TRANSFER_FILE']['destination_object'],
            )

            # criar tabela externa do bigquery na raw usando query no Operator BigQueryInsertJobOperator
            # https://cloud.google.com/bigquery/docs/external-data-cloud-storage?hl=pt-br#sql_1

            task_incoming_to_raw = BigQueryInsertJobOperator(
                task_id=f"{task_config_list['INCOMING_TO_RAW']['CREATE_EXTERNAL_TABLE']['task_id']}-{table_name}",
                configuration=task_config_list['INCOMING_TO_RAW']['CREATE_EXTERNAL_TABLE']['configuration'],
                location=task_config_list['INCOMING_TO_RAW']['CREATE_EXTERNAL_TABLE']['location'],
            )

        # criar outro grupo de tasks para a transferencia de arquivos entre a camada raw para a trusted

        # deletar arquivo da camada trusted usando GoogleCloudStorageDeleteOperator

        # transferir dados da raw para a trusted com GCSToGCSOperator

        # criar tabela externa do bigquery na trusted usando query no Operator BigQueryInsertJobOperator
        # https://cloud.google.com/bigquery/docs/external-data-cloud-storage?hl=pt-br#sql_1
             

    dag_init >> task_delete_file_incoming >> task_extration_api_to_incoming >> \
    task_delete_file_raw >> copy_single_file_incoming_to_raw >> task_incoming_to_raw >> \
    dag_end