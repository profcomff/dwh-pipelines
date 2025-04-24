import os


def get_sql_code(filename, dir):
    file_path = os.path.join(dir, filename)
    with open(file_path, "r") as f:
        sql_code = f.read()
    return f"```SQL\n{sql_code}\n```"
