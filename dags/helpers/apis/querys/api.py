def incoming_to_raw(table_name):
    return f"CREATE TABLE raw.{{table_name}} AS \
        SELECT * FROM incoming.{{ table_name }}"