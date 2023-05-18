from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from helpers.apis.dicts.api_compras import CALL_API_GOV, TABLE_NAME_LIST

from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


dag_conf= CALL_API_GOV['DAG_CONFIG']

dag = DAG(
    dag_id=dag_conf['DAG_ID'],
    default_args=dag_conf['DEFAULT_ARGS'],
    schedule_interval=dag_conf['SCHEDULE_INTERVAL'],
    catchup=dag_conf['CATCHUP'],
    tags=dag_conf['TAGS']
)

dag_init = DummyOperator(task_id = 'dag_init', dag=dag)
dag_end = DummyOperator(task_id= 'dag_end', dag=dag)


for table_name in TABLE_NAME_LIST:

    task_config_list= CALL_API_GOV['TASK_CONFIG']['PIPELINES_TABLES'][table_name]

    task_delete_file_incoming = GoogleCloudStorageDeleteOperator(
        task_id=f'delete_file_incoming-{table_name}',
        bucket_name=task_config_list['DELETE_FILE']['INCOMING']['bucket_name'], 
        prefix = task_config_list['DELETE_FILE']['INCOMING']['prefix'],
        gcp_conn_id = task_config_list['DELETE_FILE']['INCOMING']['gcp_conn_id'],  
        impersonation_chain = task_config_list['DELETE_FILE']['INCOMING']['impersonation_chain'],
        dag=dag
    )

    task_extration_api = SimpleHttpOperator(
        task_id= f"extration-{table_name}",
        method= task_config_list['CLOUD_FUNCTION']['method'],
        http_conn_id= task_config_list['CLOUD_FUNCTION']['http_conn_id'],
        endpoint= task_config_list['CLOUD_FUNCTION']['endpoint'],
        data= (task_config_list['CLOUD_FUNCTION']['data']),
        headers= task_config_list['CLOUD_FUNCTION']['headers'],
        dag=dag
    )

    task_delete_file_raw = GoogleCloudStorageDeleteOperator(
        task_id=f'delete_file_raw-{table_name}',
        bucket_name=task_config_list['DELETE_FILE']['RAW']['bucket_name'], 
        prefix = task_config_list['DELETE_FILE']['RAW']['prefix'],
        gcp_conn_id = task_config_list['DELETE_FILE']['RAW']['gcp_conn_id'],
        impersonation_chain = task_config_list['DELETE_FILE']['RAW']['impersonation_chain'],
        dag=dag
    )

    task_incoming_to_raw = GCSToGCSOperator(
        task_id=f'incoming_to_raw-{table_name}',
        source_bucket=task_config_list['TRANSFER_STORAGE_DATA']['INCOMING_TO_RAW']['source_bucket'],
        source_object=task_config_list['TRANSFER_STORAGE_DATA']['INCOMING_TO_RAW']['source_object'],
        destination_bucket=task_config_list['TRANSFER_STORAGE_DATA']['INCOMING_TO_RAW']['destination_bucket'],
        destination_object=task_config_list['TRANSFER_STORAGE_DATA']['INCOMING_TO_RAW']['destination_object'],
        move_object=task_config_list['TRANSFER_STORAGE_DATA']['INCOMING_TO_RAW']['move_object'],
        dag=dag
    )

    task_delete_file_trusted = GoogleCloudStorageDeleteOperator(
        task_id=f'delete_file_trusted-{table_name}',
        bucket_name=task_config_list['DELETE_FILE']['TRUSTED']['bucket_name'], 
        prefix = task_config_list['DELETE_FILE']['TRUSTED']['prefix'],
        gcp_conn_id = task_config_list['DELETE_FILE']['TRUSTED']['gcp_conn_id'],
        impersonation_chain = task_config_list['DELETE_FILE']['TRUSTED']['impersonation_chain'],
        dag=dag
    )

    task_raw_to_trusted = GCSToGCSOperator(
        task_id=f'raw_to_trusted-{table_name}',
        source_bucket=task_config_list['TRANSFER_STORAGE_DATA']['RAW_TO_TRUSTED']['source_bucket'],
        source_object=task_config_list['TRANSFER_STORAGE_DATA']['RAW_TO_TRUSTED']['source_object'],
        destination_bucket=task_config_list['TRANSFER_STORAGE_DATA']['RAW_TO_TRUSTED']['destination_bucket'],
        destination_object=task_config_list['TRANSFER_STORAGE_DATA']['RAW_TO_TRUSTED']['destination_object'],
        move_object=task_config_list['TRANSFER_STORAGE_DATA']['RAW_TO_TRUSTED']['move_object'],
        dag=dag
    )

    task_trusted_to_bigquery = GCSToBigQueryOperator(
        task_id=f'trusted_to_bigquery-{table_name}',
        bucket=task_config_list['TRANSFER_STORAGE_DATA']['TRUSTED_TO_BIGQUERY']['bucket'],
        source_objects=task_config_list['TRANSFER_STORAGE_DATA']['TRUSTED_TO_BIGQUERY']['source_objects'],
        destination_project_dataset_table=task_config_list['TRANSFER_STORAGE_DATA']['TRUSTED_TO_BIGQUERY']['destination_project_dataset_table'],
        write_disposition=task_config_list['TRANSFER_STORAGE_DATA']['TRUSTED_TO_BIGQUERY']['write_disposition'],
        external_table=task_config_list['TRANSFER_STORAGE_DATA']['TRUSTED_TO_BIGQUERY']['external_table'],
        autodetect=task_config_list['TRANSFER_STORAGE_DATA']['TRUSTED_TO_BIGQUERY']['autodetect'],
        max_id_key=task_config_list['TRANSFER_STORAGE_DATA']['TRUSTED_TO_BIGQUERY']['max_id_key'],
        deferrable=task_config_list['TRANSFER_STORAGE_DATA']['TRUSTED_TO_BIGQUERY']['deferrable'],
        dag=dag
    )

    dag_init >> task_delete_file_incoming >> task_extration_api >> \
    task_delete_file_raw >> task_incoming_to_raw >> \
    task_delete_file_trusted >> task_raw_to_trusted >> task_trusted_to_bigquery >> dag_end



