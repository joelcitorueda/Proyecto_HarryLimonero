import pyodbc


def get_connection():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"Server={{localhost}};"
        f"Database={{TerminalTarijaGold}};"
        "Trusted_Connection=yes;"
    )

def get_connection2():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"Server={{localhost}};"
        f"Database={{TerminalTarijaDB}};"
        "Trusted_Connection=yes;"
    )

