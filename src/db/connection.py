import pyodbc


def create_connection(conn_str: str):
    return pyodbc.connect(conn_str, autocommit=False)