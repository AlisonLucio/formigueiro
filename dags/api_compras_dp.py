from datetime import datetime
from helpers.apis.dicts.api_gov_br.base import CALL_API_GOV
from helpers.apis.dicts.api_gov_br.api_fornecedores import TASK_CONFIG as task_config_api_fornecedores
from helpers.apis.dicts.api_gov_br.api_orgaos import TASK_CONFIG as task_config_api_orgaos
from helpers.apis.dicts.api_gov_br.api_servicos_orgaos import TASK_CONFIG as task_config_api_servicos_orgaos

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocDeleteClusterOperator, 
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator
    )
from airflow.utils.task_group import TaskGroup
import json

path_file_dict = "/opt/airflow/dags/helpers/apis/dicts/api_gov_br/"

dag_conf= CALL_API_GOV['DAG_CONFIG']

with DAG(
    dag_id=dag_conf['DAG_ID'],
    default_args=dag_conf['DEFAULT_ARGS'],
    schedule_interval=dag_conf['SCHEDULE_INTERVAL'],
    catchup=dag_conf['CATCHUP'],
    tags=dag_conf['TAGS'],
    ) as dag:


    dag_init = DummyOperator(task_id = 'dag_init', dag=dag)
    dag_end = DummyOperator(task_id= 'dag_end', dag=dag)

    list_task_config = [
        task_config_api_fornecedores, 
        task_config_api_orgaos,
        task_config_api_servicos_orgaos
        ]

    for task_config in list_task_config:
        table_name = task_config['TABLE_NAME']

        with TaskGroup(group_id=f'extration_api_{table_name}') as task_group:
            task_delete_file_incoming = GoogleCloudStorageDeleteOperator(
                task_id=f'delete_file_incoming-{table_name}',
                bucket_name=task_config['DELETE_FILE']['INCOMING']['bucket_name'], 
                prefix = task_config['DELETE_FILE']['INCOMING']['prefix'],
                gcp_conn_id = task_config['DELETE_FILE']['INCOMING']['gcp_conn_id'],  
                impersonation_chain = task_config['DELETE_FILE']['INCOMING']['impersonation_chain'],
                dag=dag
            )

            task_extration_api_to_incoming = SimpleHttpOperator(
                task_id= f"extration-{table_name}",
                method= task_config['CLOUD_FUNCTION']['method'],
                http_conn_id= task_config['CLOUD_FUNCTION']['http_conn_id'],
                endpoint= task_config['CLOUD_FUNCTION']['endpoint'],
                data= (task_config['CLOUD_FUNCTION']['data']),
                headers= task_config['CLOUD_FUNCTION']['headers'],
                dag=dag
            )

        with TaskGroup(group_id=f'incoming_to_raw_{table_name}') as task_group:

            create_cluster_incoming_to_raw = DataprocCreateClusterOperator(
                task_id=f"{task_config['DATAPROC_CONFIG']['INCOMING_TO_RAW']['CREATE_CLUSTER']['task_id']}",
                project_id=task_config['DATAPROC_CONFIG']['INCOMING_TO_RAW']['CREATE_CLUSTER']['project_id'],
                cluster_config=task_config['DATAPROC_CONFIG']['INCOMING_TO_RAW']['CREATE_CLUSTER']['cluster_config'],
                region=task_config['DATAPROC_CONFIG']['INCOMING_TO_RAW']['CREATE_CLUSTER']['region'],
                cluster_name= task_config['DATAPROC_CONFIG']['INCOMING_TO_RAW']['CREATE_CLUSTER']['cluster_name'],
                use_if_exists=False,
                dag=dag
            )

            submit_job_spark_incoming_to_raw = DataprocSubmitJobOperator(
                task_id=f'{task_config["DATAPROC_CONFIG"]["INCOMING_TO_RAW"]["SUBMIT_JOB_SPARK"]["task_id"]}', 
                job=task_config['DATAPROC_CONFIG']['INCOMING_TO_RAW']['SUBMIT_JOB_SPARK']['job'],
                region=task_config['DATAPROC_CONFIG']['INCOMING_TO_RAW']['SUBMIT_JOB_SPARK']['region'], 
                project_id=task_config['DATAPROC_CONFIG']['INCOMING_TO_RAW']['SUBMIT_JOB_SPARK']['project_id'],
                dag=dag
            )

            delete_cluster_incoming_to_raw = DataprocDeleteClusterOperator(
            task_id=f'{task_config["DATAPROC_CONFIG"]["INCOMING_TO_RAW"]["DELETE_CLUSTER"]["task_id"]}',
            project_id=task_config['DATAPROC_CONFIG']['INCOMING_TO_RAW']['DELETE_CLUSTER']['project_id'],
            cluster_name=task_config['DATAPROC_CONFIG']['INCOMING_TO_RAW']['DELETE_CLUSTER']['cluster_name'],
            region=task_config['DATAPROC_CONFIG']['INCOMING_TO_RAW']['DELETE_CLUSTER']['region'],
            dag=dag
            )   

        with TaskGroup(group_id=f'raw_to_trusted_{table_name}') as task_group:

            create_cluster_raw_to_trusted = DataprocCreateClusterOperator(
                task_id=f'{task_config["DATAPROC_CONFIG"]["RAW_TO_TRUSTED"]["CREATE_CLUSTER"]["task_id"]}',
                project_id=task_config['DATAPROC_CONFIG']['RAW_TO_TRUSTED']['CREATE_CLUSTER']['project_id'],
                cluster_config=task_config['DATAPROC_CONFIG']['RAW_TO_TRUSTED']['CREATE_CLUSTER']['cluster_config'],
                region=task_config['DATAPROC_CONFIG']['RAW_TO_TRUSTED']['CREATE_CLUSTER']['region'],
                cluster_name= task_config['DATAPROC_CONFIG']['RAW_TO_TRUSTED']['CREATE_CLUSTER']['cluster_name'],
                use_if_exists=False,
                dag=dag
            )

            submit_job_spark_raw_to_trusted = DataprocSubmitJobOperator(
                task_id=f'{task_config["DATAPROC_CONFIG"]["RAW_TO_TRUSTED"]["SUBMIT_JOB_SPARK"]["task_id"]}', 
                job=task_config['DATAPROC_CONFIG']['RAW_TO_TRUSTED']['SUBMIT_JOB_SPARK']['job'],
                region=task_config['DATAPROC_CONFIG']['RAW_TO_TRUSTED']['SUBMIT_JOB_SPARK']['region'], 
                project_id=task_config['DATAPROC_CONFIG']['RAW_TO_TRUSTED']['SUBMIT_JOB_SPARK']['project_id'],
                dag=dag
            )

            delete_cluster_raw_to_trusted = DataprocDeleteClusterOperator(
                task_id=f'{task_config["DATAPROC_CONFIG"]["RAW_TO_TRUSTED"]["DELETE_CLUSTER"]["task_id"]}',
                project_id=task_config['DATAPROC_CONFIG']['RAW_TO_TRUSTED']['DELETE_CLUSTER']['project_id'],
                cluster_name=task_config['DATAPROC_CONFIG']['RAW_TO_TRUSTED']['DELETE_CLUSTER']['cluster_name'],
                region=task_config['DATAPROC_CONFIG']['RAW_TO_TRUSTED']['DELETE_CLUSTER']['region'],
                dag=dag
            )   

        with TaskGroup(group_id=f'trusted_to_bigquery_{table_name}') as task_group:

            task_trusted_to_bigquery = GCSToBigQueryOperator(
                task_id=task_config['TRUSTED_TO_BIGQUERY']['task_id'],
                bucket=task_config['TRUSTED_TO_BIGQUERY']['bucket'] , 
                source_objects =task_config['TRUSTED_TO_BIGQUERY']['source_objects'], 
                destination_project_dataset_table =task_config['TRUSTED_TO_BIGQUERY']['destination_project_dataset_table'], 
                source_format = task_config['TRUSTED_TO_BIGQUERY']['source_format'] , 
                compression = task_config['TRUSTED_TO_BIGQUERY']['compression'] , 
                create_disposition = task_config['TRUSTED_TO_BIGQUERY']['create_disposition'] , 
                write_disposition= task_config['TRUSTED_TO_BIGQUERY']['write_disposition'] , 
                field_delimiter = task_config['TRUSTED_TO_BIGQUERY']['field_delimiter'] , 
                quote_character = task_config['TRUSTED_TO_BIGQUERY']['quote_character'] , 
                allow_jagged_rows = task_config['TRUSTED_TO_BIGQUERY']['allow_jagged_rows'] , 
                encoding = task_config['TRUSTED_TO_BIGQUERY']['encoding'] , 
                gcp_conn_id = task_config['TRUSTED_TO_BIGQUERY']['gcp_conn_id'] , 
                time_partitioning = task_config['TRUSTED_TO_BIGQUERY']['time_partitioning'] , #https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.table.TimePartitioning
                cluster_fields = task_config['TRUSTED_TO_BIGQUERY']['cluster_fields'] , 
                autodetect = task_config['TRUSTED_TO_BIGQUERY']['autodetect'] , 
                location = task_config['TRUSTED_TO_BIGQUERY']['location'] , 
                impersonation_chain = task_config['TRUSTED_TO_BIGQUERY']['impersonation_chain'] , 
                result_timeout = task_config['TRUSTED_TO_BIGQUERY']['result_timeout'] , 
                dag=dag
            )

        dag_init >> \
        task_delete_file_incoming >> task_extration_api_to_incoming >> create_cluster_incoming_to_raw >> \
        submit_job_spark_incoming_to_raw >> delete_cluster_incoming_to_raw >> create_cluster_raw_to_trusted >> \
        submit_job_spark_raw_to_trusted >> delete_cluster_raw_to_trusted >> task_trusted_to_bigquery >> \
        dag_end