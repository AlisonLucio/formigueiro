year='{{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y")}}'
mounth='{{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%m")}}'
day='{{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%d")}}'
prefdate='{{macros.datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y%m%d")}}'


def delete(table_name_complete):
    return f'DELETE FROM {table_name_complete} WHERE EXISTS (SELECT * FROM {table_name_complete} '\
        f'WHERE year= {year} AND mounth = {mounth} AND day = {day} AND prefdate = {prefdate});'


def insert(table_name_complete):
    return f'CREATE TABLE IF NOT EXISTS {table_name_complete} AS '\
         f'SELECT * FROM {table_name_complete} ' \
         f'WHERE year = {year} AND mounth = {mounth} AND day = {day} AND prefdate = {prefdate};'

def delete_and_insert(table_name_complete) -> str:
    return f'{delete(table_name_complete)} {insert(table_name_complete)}' 