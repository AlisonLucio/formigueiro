from airflow.models import Variable
from helpers.utils.general_config import PathsDataLake
from google.cloud import bigquery

# constantes de uso geral

PROJECT_ID='video-aulas-ed'
DATASET_ID_INCOMING='incoming'
DATASET_ID_RAW='raw'
DATASET_ID_TRUSTED='trusted'
LOCATION='us-central1'
SERVICE_ACCOUNT=Variable.get('SERVICE_ACCOUNT')
GCP_CONN_ID='gcp_conn_id'
GOOGLE_CLOUD_DEFAULT='google_cloud_default'

# constantes com os nomes das tabelas
TABLE='comb-aut-geral'

# configuração das pastas no datalake
path_dl = PathsDataLake(change_file_type='csv', change_table_name=TABLE, change_source_type='postgresql', change_file_extension='csv', flow_technology='gcs_operator')

# Constantes com endereço do datalake
PATH_SAVE_FILE_TABLE_INCOMING=path_dl.change_file_path(change_layer=DATASET_ID_INCOMING)
PATH_SAVE_FILE_TABLE_RAW=path_dl.change_file_path(change_layer=DATASET_ID_RAW)
PATH_SAVE_FILE_TABLE_TRUSTED=path_dl.change_file_path(change_layer=DATASET_ID_TRUSTED)


TASK_CONFIG = {
    "ZONE": LOCATION,
    "TABLE_NAME": TABLE,
    "DELETE_FILE":{
        'INCOMING':{
            'bucket_name':PROJECT_ID,
            'prefix':PATH_SAVE_FILE_TABLE_INCOMING,
            'gcp_conn_id': GCP_CONN_ID,
            'impersonation_chain': SERVICE_ACCOUNT
        },
    },
    "POSTGRESQL":{
        'bucket':PROJECT_ID,
        'filename':f'{PATH_SAVE_FILE_TABLE_INCOMING}{TABLE}.csv',
        'export_format':'csv',
        'stringify_dict':'csv',
        'field_delimiter':',',
        'postgres_conn_id':'postgres_conn_id',
        'use_server_side_cursor':False,
        'cursor_itersize':2000,
        'sql':'SELECT * FROM combustiveis_automotivos WHERE last_update = '\
            "CAST('{{macros.datetime.strptime(ts_nodash, '%Y%m%dT%H%M%S').strftime('%Y/%m/%d')}}' AS DATE)",
    },
    "TRANSFER_FILE":{
        'incoming_to_raw':{
            'source_bucket':PROJECT_ID,
            'source_object':PATH_SAVE_FILE_TABLE_INCOMING,
            'destination_bucket':PROJECT_ID,
            'destination_object':PATH_SAVE_FILE_TABLE_RAW,
            'move_object': True,
            'replace': True,
            'source_object_required': True
        },
        'raw_to_trusted':{
            'source_bucket':PROJECT_ID,
            'source_object':PATH_SAVE_FILE_TABLE_RAW,
            'destination_bucket':PROJECT_ID,
            'destination_object':PATH_SAVE_FILE_TABLE_TRUSTED,
            'move_object': False,
            'replace': True,
            'source_object_required': True            
        },
        'trusted_to_bigquery':{
            'bucket':PROJECT_ID , 
            'source_objects': f'{PATH_SAVE_FILE_TABLE_TRUSTED}{TABLE}.csv', 
            'destination_project_dataset_table': f'{PROJECT_ID}.postgresql.{TABLE}', 
            'schema_fields' : None ,
            'schema_object' : None , 
            'schema_object_bucket' : None , 
            'source_format' : 'csv' , 
            'compression' : 'GZIP' , 
            'create_disposition' : 'CREATE_IF_NEEDED' , 
            'write_disposition': 'WRITE_APPEND' , 
            'allow_quoted_newlines' : False , 
            'encoding' : 'UTF-8' , 
            'gcp_conn_id' : 'gcp_conn_id' , 
            'time_partitioning' : {'last_update':'date'} , 
            'cluster_fields' : 'last_update' , 
            'autodetect' : True , 
            'location' : LOCATION, 
            'impersonation_chain' : SERVICE_ACCOUNT , 
            'deferrable' : False , 
            'result_retry' : '2' ,
            'result_timeout' : None , 
            'cancel_on_kill' : True , 
            'job_id' : None , 
            'force_rerun' : True , 
            'reattach_states' : None , 
            'project_id' : PROJECT_ID
        },
    }
}