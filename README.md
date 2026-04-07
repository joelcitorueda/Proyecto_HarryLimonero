---- INSTALAR PYTHON ----

Version: 3.14.3


---- ACTUALIZAR PIP ----

python.exe -m pip install --upgrade pip


---- INSTALAR DEPENDENCIAS ----

pip install pyodbc pandas numpy requests sqlalchemy streamlit plotly pytest streamlit


---- EJECUTAR TerminalTarijaDB.sql dentro de SQLServer ----


---- Cadena de conexion Capa Bronze archivo extract_sql.py ----

# ──────────────────────────────────────────────
# CONFIGURACIÓN DE CONEXIÓN
# ──────────────────────────────────────────────
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=TerminalTarijaDB;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

---- EJECUTAR CAPA BRONZE ----

python bronze/run_bronze.py


---- EJECUTAR CAPA SILVER ----

python silver/run_silver.py


---- Cadena de conexion Capa Gold archivo load_gold.py ----

# Cadena de conexión SQLAlchemy para SQL Server con Windows Auth
ENGINE_GOLD = (
    "mssql+pyodbc://localhost/TerminalTarijaGold"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&Trusted_Connection=yes"
    "&TrustServerCertificate=yes"
)


---- CAPA GOLDEN ----

EJECUTAR create_gold_schema.sql dentro de SQLServer

python python gold/load_gold.py

EJECUTAR kpis.sql dentro de SQLServer


---- Cadena de conexion Dashboard archivo db.py ----

Conexion DB Gold

def get_connection():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"Server={{localhost}};"
        f"Database={{TerminalTarijaGold}};"
        "Trusted_Connection=yes;"
    )

Conexion DB Datos Crudos

def get_connection2():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"Server={{localhost}};"
        f"Database={{TerminalTarijaDB}};"
        "Trusted_Connection=yes;"
    )



---- DASHBOARD ----

python -m streamlit run dashboard/app.py