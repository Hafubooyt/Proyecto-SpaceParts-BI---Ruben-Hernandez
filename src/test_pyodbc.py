# ------------------------------------------------------------
# Script: test_pyodbc.py (buscar tabla de ventas)
# Autor: Ruben Hernandez
#
# Este script me ayuda a descubrir el nombre exacto
# de las tablas relacionadas con ventas/órdenes/facturas.
# ------------------------------------------------------------

import os
import pyodbc
from dotenv import load_dotenv
from utils.logger import get_logger

# Cargar variables desde .env
load_dotenv()
driver = os.getenv("ODBC_DRIVER")
server = os.getenv("AZURE_SQL_SERVER")
user = os.getenv("AZURE_SQL_USER")
password = os.getenv("AZURE_SQL_PASSWORD")
database = os.getenv("AZURE_SQL_DB")

# Cadena de conexión ODBC
conn_str = (
    f"DRIVER={{{driver}}};"
    f"SERVER={server};"
    f"UID={user};PWD={password};"
    f"DATABASE={database};"
    f"Encrypt=yes;TrustServerCertificate=yes"
)

logger = get_logger("TEST-PYODBC")

try:
    cnxn = pyodbc.connect(conn_str, timeout=10)
    logger.info(f"Conexion exitosa con la base: {database}")
    cur = cnxn.cursor()

    # Buscar tablas relacionadas con ventas
    logger.info("Buscando tablas posibles de ventas (Sales/Order/Invoice/Transaction)...")
    cur.execute("""
    SELECT TABLE_SCHEMA, TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME LIKE '%Sale%'
       OR TABLE_NAME LIKE '%Order%'
       OR TABLE_NAME LIKE '%Invoice%'
       OR TABLE_NAME LIKE '%Transaction%'
    """)
    rows = cur.fetchall()
    for r in rows:
        logger.info(f"- Posible tabla de ventas: {r.TABLE_SCHEMA}.{r.TABLE_NAME}")

    cnxn.close()

except Exception as e:
    logger.error("Error buscando tabla de ventas", exc_info=True)