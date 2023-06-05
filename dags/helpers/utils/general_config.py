def change_file_path_incoming(change_layer:str, change_file_type:str, change_table_name:str, change_file_extension:str=None):
    return f'layer_{change_layer}/source_type_api/file_type_{change_file_type}/{change_table_name}/'\
            'prefdate={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y%m%d")}}/'\
           f'{change_table_name}.{change_file_extension}' 

def change_file_path_raw_and_trusted(change_layer:str, change_file_type:str, change_table_name:str):
    return f'layer_{change_layer}/source_type_api/file_type_{change_file_type}/{change_table_name}/'\
            'prefdate={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y%m%d")}}/'\
           f'{change_table_name}' 

def get_file_check_path(change_source_type:str, change_file_type:str, change_table_name:str):
    return f'layer_raw/source_type_{change_source_type}/file_type_{change_file_type}/{change_table_name}/'\
           f'prefdate=*/{change_table_name}/*.csv'


def get_cluster_name(project_id : str, layer:str, table_name:str):
    return f'{project_id}.{layer}.{table_name}'.replace('_', '-').replace('.','-')