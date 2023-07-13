from helpers.utils.general_config import PathsDataLake

def get_cluster_config(boot_size_type):
    size_dict = {
        "small": {
            "master_config":{
                "boot_disk_size_gb": 1024
            },
            "worker_config":{
                "boot_disk_size_gb": 4096
            }
        },
        "medium":{
            "master_config":{
                "boot_disk_size_gb": 1024
            },
            "worker_config":{
                "boot_disk_size_gb": 8192
            }
        },
        "large":{
            "master_config":{
                "boot_disk_size_gb": 1024
            },
            "worker_config":{
                "boot_disk_size_gb": 16384
            }
        }
    }

    return {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", 
                        "boot_disk_size_gb": size_dict[boot_size_type]['master_config']['boot_disk_size_gb']
                        },
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", 
                        "boot_disk_size_gb": size_dict[boot_size_type]['worker_config']['boot_disk_size_gb']
                        },
    },
}