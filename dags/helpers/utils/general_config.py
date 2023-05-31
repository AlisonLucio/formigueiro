def change_file_path(change_layer:str, change_file_type:str, change_table_name:str, change_file_extension:str):
    return f'layer_{change_layer}/source_type_api/file_type_{change_file_type}/{change_table_name}/'\
            'year={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y")}}/'\
            'mounth={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%m")}}/'\
            'day={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%d")}}/'\
            'hour={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%H")}}/'\
            'minute={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%M")}}/'\
            'prefdate={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y%m%d")}}/'\
            f'{change_table_name}'

def get_cluster_name(project_id : str, layer:str, table_name:str):
    return f'{project_id}.{layer}.{table_name}'.replace('_', '-').replace('.','-')