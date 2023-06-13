class QuerysBigquery:
    def __init__(self, project_id, layer_destination, layer_origin, table_name):
        self.project_id=project_id
        self.layer_destination=layer_destination
        self.layer_origin=layer_origin
        self.table_name=table_name

    def __where_partition(self):
        return ' WHERE year={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y")}}'\
               ' AND mounth={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%m")}}'\
               ' AND day={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%d")}}'\
               ' AND prefdate={{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y%m%d")}}'

    def delete_partition(self):
        return f'DELETE FROM {self.project_id}.{self.layer_destination}.{self.table_name} {self.__where_partition()}' 

    def file_transfer_bigquery(self):
        return f'CREATE TABLE IF NOT EXISTS {self.project_id}.{self.layer_destination}.{self.table_name} AS '\
               f'SELECT DISTINCT * FROM {self.project_id}.{self.layer_origin}.{self.table_name} {self.__where_partition()}'
    
    def create_external_table(self, path_file):
        return f'CREATE EXTERNAL TABLE `{self.project_id}.{self.layer_destination}.{self.table_name}`'\
                    'OPTIONS ('\
                        'format ="csv", '\
                        f'uris = ["gs://{path_file}/"]'\
                    ');'