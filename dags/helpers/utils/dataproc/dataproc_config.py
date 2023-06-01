from helpers.utils.general_config import get_cluster_name

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
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}


def get_job_spark(project_id, layer, table_name, pyspark_file, file_origem_path, file_destination_path):
    return {
    "reference": {"project_id": project_id},
    "placement": {"cluster_name": get_cluster_name(project_id=project_id, layer=layer, table_name=table_name)},
    "pyspark_job": {"main_python_file_uri": pyspark_file, 'args': [
        f'gs://{project_id}/{file_origem_path}' , \
        f'gs://{project_id}/{file_destination_path}'  ,\
        f'incoming_to_raw_{table_name}']},
    }