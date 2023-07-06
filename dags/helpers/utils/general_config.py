class PathsDataLake:
    def __init__(self, change_file_type:str, change_table_name:str, change_source_type:str='api',
                 flow_technology:str = 'dataproc', change_file_extension:str=None):
        
        self.change_file_type=change_file_type
        self.change_table_name=change_table_name
        self.change_source_type=change_source_type
        self.flow_technology = flow_technology
        self.change_file_extension =change_file_extension

    def __path_file(self, change_layer):
        return f'layer_{change_layer}/source_type_{self.change_source_type}/flow_technology_{self.flow_technology}'\
               f'/file_type_{self.change_file_type}/{self.change_table_name}/'

    def change_file_path(self, change_layer):
        if self.flow_technology == 'dataproc':

            return f'{self.__path_file(change_layer=change_layer)}'\
                    'prefdate={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y%m%d")}}/'\
                   f'{self.change_table_name}.{self.change_file_extension}' 
        
        elif self.flow_technology == 'gcs_operator':
            return f'{self.__path_file(change_layer=change_layer)}'\
                    'year={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y")}}/'\
                    'mounth={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%m")}}/'\
                    'day={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%d")}}/'\
                    'prefdate={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y%m%d")}}/'

    def get_file_check_path(self,change_layer:str):
        return f'{self.__path_file(change_layer=change_layer)}prefdate=*/*.csv'


    def get_cluster_name(self, project_id : str, layer:str):
        return f'{project_id}.{layer}.{self.change_table_name}-'\
            '{{ds_nodash}}'\
            .replace('.','-')