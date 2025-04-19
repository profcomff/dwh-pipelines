import os

def get_sql_code(filename):
    directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(directory, 'info.sql')
    with open(file_path, 'r') as f:
        sql_code = f.read()
    return f"```SQL\n{sql_code}\n```"
