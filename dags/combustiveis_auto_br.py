from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers.apis.dicts.db_postgresql.comb_aut_geral import TASK_CONFIG as task_config_comb_aut_geral
from helpers.apis.dicts.db_postgresql.base import DAG_CONFIG
from airflow.utils.task_group import TaskGroup
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

with DAG(
    dag_id=DAG_CONFIG['DAG_ID'],
    default_args= DAG_CONFIG['DEFAULT_ARGS'],
    schedule_interval= DAG_CONFIG['SCHEDULE_INTERVAL'],
    catchup=DAG_CONFIG['CATCHUP'],
    tags=DAG_CONFIG['TAGS']
    ) as dag:

    dag_init = DummyOperator(task_id = 'dag_init', dag=dag)
    dag_end  = DummyOperator(task_id = 'dag_end', dag=dag)

    list_task_config = [
        task_config_comb_aut_geral
    ]

    for task_config in list_task_config:
        table_name = task_config['TABLE_NAME']

        with TaskGroup(group_id=f'extration_postgres_incoming-{table_name}') as task_group:
            task_delete_file_incoming = GoogleCloudStorageDeleteOperator(
                task_id=f"delete_file_incoming-{table_name}",
                bucket_name=task_config['DELETE_FILE']['INCOMING']['bucket_name'], 
                prefix = task_config['DELETE_FILE']['INCOMING']['prefix'],
                gcp_conn_id = task_config['DELETE_FILE']['INCOMING']['gcp_conn_id'],  
                impersonation_chain = task_config['DELETE_FILE']['INCOMING']['impersonation_chain'],
                dag=dag
            )

            task_extration_api_to_incoming = PostgresToGCSOperator(
                task_id=f'extration_postgresql-{table_name}',
                bucket=task_config['POSTGRESQL']['bucket'],
                filename=task_config['POSTGRESQL']['filename'],  
                export_format=task_config['POSTGRESQL']['export_format'],
                stringify_dict=task_config['POSTGRESQL']['stringify_dict'],
                field_delimiter=task_config['POSTGRESQL']['field_delimiter'],
                postgres_conn_id=task_config['POSTGRESQL']['postgres_conn_id'], 
                use_server_side_cursor=task_config['POSTGRESQL']['use_server_side_cursor'], 
                cursor_itersize=task_config['POSTGRESQL']['cursor_itersize'],
                sql=task_config['POSTGRESQL']['sql'],
                dag=dag
                )
            
        with TaskGroup(group_id=f'transfer_data_incoming_to_raw-{table_name}') as task_group:

            task_copy_single_file_incoming_to_raw = GCSToGCSOperator(
                task_id=f"incoming_to_raw-{table_name}",
                source_bucket=task_config['TRANSFER_FILE']['incoming_to_raw']['source_bucket'],
                source_object=task_config['TRANSFER_FILE']['incoming_to_raw']['source_object'],
                destination_bucket=task_config['TRANSFER_FILE']['incoming_to_raw']['destination_bucket'],
                destination_object=task_config['TRANSFER_FILE']['incoming_to_raw']['destination_object'],
                move_object=task_config['TRANSFER_FILE']['incoming_to_raw']['move_object'],
                replace=task_config['TRANSFER_FILE']['incoming_to_raw']['replace'],
                source_object_required=task_config['TRANSFER_FILE']['incoming_to_raw']['source_object_required'],
                dag=dag
            )

        with TaskGroup(group_id=f'transfer_data_raw_to_trusted-{table_name}') as task_group:

            task_copy_single_file_raw_to_trusted = GCSToGCSOperator(
                task_id=f"raw_to_trusted-{table_name}",
                source_bucket=task_config['TRANSFER_FILE']['raw_to_trusted']['source_bucket'],
                source_object=task_config['TRANSFER_FILE']['raw_to_trusted']['source_object'],
                destination_bucket=task_config['TRANSFER_FILE']['raw_to_trusted']['destination_bucket'],
                destination_object=task_config['TRANSFER_FILE']['raw_to_trusted']['destination_object'],
                move_object=task_config['TRANSFER_FILE']['raw_to_trusted']['move_object'],
                replace=task_config['TRANSFER_FILE']['raw_to_trusted']['replace'],
                source_object_required=task_config['TRANSFER_FILE']['raw_to_trusted']['source_object_required'],
                dag=dag
            )

        with TaskGroup(group_id=f'transfer_data_trusted_bigquery-{table_name}') as task_group:
            
            task_incoming_to_raw = GCSToBigQueryOperator(
                task_id=f"trusted_to_bigquery-{table_name}",
                bucket=task_config['TRANSFER_FILE']['trusted_to_bigquery']['bucket'], 
                source_objects=task_config['TRANSFER_FILE']['trusted_to_bigquery']['source_objects'] , 
                destination_project_dataset_table=task_config['TRANSFER_FILE']['trusted_to_bigquery']['destination_project_dataset_table'] , 
                schema_fields = task_config['TRANSFER_FILE']['trusted_to_bigquery']['schema_fields'] ,
                schema_object = task_config['TRANSFER_FILE']['trusted_to_bigquery']['schema_object'], 
                schema_object_bucket = task_config['TRANSFER_FILE']['trusted_to_bigquery']['schema_object_bucket'], 
                source_format = task_config['TRANSFER_FILE']['trusted_to_bigquery']['source_format'] , 
                compression = task_config['TRANSFER_FILE']['trusted_to_bigquery']['compression'] , 
                create_disposition = task_config['TRANSFER_FILE']['trusted_to_bigquery']['create_disposition'] , 
                write_disposition= task_config['TRANSFER_FILE']['trusted_to_bigquery']['write_disposition'],  
                allow_quoted_newlines = task_config['TRANSFER_FILE']['trusted_to_bigquery']['allow_quoted_newlines'] , 
                encoding = task_config['TRANSFER_FILE']['trusted_to_bigquery']['encoding'] , 
                gcp_conn_id = task_config['TRANSFER_FILE']['trusted_to_bigquery']['gcp_conn_id'] , 
                time_partitioning = task_config['TRANSFER_FILE']['trusted_to_bigquery']['time_partitioning'] , 
                cluster_fields = task_config['TRANSFER_FILE']['trusted_to_bigquery']['cluster_fields'], 
                autodetect = task_config['TRANSFER_FILE']['trusted_to_bigquery']['autodetect'] , 
                location = task_config['TRANSFER_FILE']['trusted_to_bigquery']['location'], 
                impersonation_chain = task_config['TRANSFER_FILE']['trusted_to_bigquery']['impersonation_chain'] , 
                deferrable = task_config['TRANSFER_FILE']['trusted_to_bigquery']['deferrable'] , 
                result_timeout = task_config['TRANSFER_FILE']['trusted_to_bigquery']['result_timeout'] , 
                cancel_on_kill = task_config['TRANSFER_FILE']['trusted_to_bigquery']['cancel_on_kill'] , 
                job_id = task_config['TRANSFER_FILE']['trusted_to_bigquery']['job_id'] , 
                force_rerun = task_config['TRANSFER_FILE']['trusted_to_bigquery']['force_rerun'] , 
                reattach_states = task_config['TRANSFER_FILE']['trusted_to_bigquery']['reattach_states'] , 
            )
            
        dag_init >> task_delete_file_incoming >> task_extration_api_to_incoming >> \
        task_copy_single_file_incoming_to_raw >> task_copy_single_file_raw_to_trusted >> \
        task_incoming_to_raw >> dag_end
    