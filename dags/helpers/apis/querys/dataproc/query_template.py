ingestion_date='{{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y%m%d")}}'


def delete(table_name_complete):
    return f'DELETE FROM {table_name_complete} WHERE EXISTS \
                (\
                    SELECT checksum FROM {table_name_complete} '\
                    f'WHERE ingestion_date = {ingestion_date}\
                );'
