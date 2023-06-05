from helpers.utils.general_config import get_cluster_name
from helpers.utils.general_config import get_file_check_path

def get_cluster_config():
    return {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 4096},
    },
}


def get_job_spark(project_id, layer, table_name, pyspark_file, file_origem_path, file_destination_path, \
                  app_name, change_source_type=None, change_file_type=None, change_table_name=None, schema_incoming=None,\
                  schema_raw=None, schema_trusted=None):
    return {
    "reference": {"project_id": project_id},
    "placement": {"cluster_name": get_cluster_name(project_id=project_id, layer=layer, table_name=table_name)},
    "pyspark_job": {"main_python_file_uri": pyspark_file, \
        'args': [
            f'gs://{project_id}/{file_origem_path}' , \
            f'gs://{project_id}/{file_destination_path}',\
            f'{app_name}_{table_name}',\
            f'{get_file_check_path(change_source_type=change_source_type, change_file_type=change_file_type, change_table_name=change_table_name)}',
        ]
    }
    }