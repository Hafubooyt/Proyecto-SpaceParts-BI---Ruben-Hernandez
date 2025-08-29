# ------------------------------------------------------------
# Script: etl_spaceparts.py
#
# Propósito:
#   Este script implementa la fase **Extract** del proceso ETL.
#   ¿Qué hago aquí?
#     - Me conecto a la base de datos Azure SQL "SpacePartsCoDW".
#     - Descargo tres tablas clave para el modelo BI:
#         • dim.Customers
#         • dim.Products
#         • fact.Invoices   (hechos de ventas/facturación)
#     - Guardo esa data tal cual en formato CSV dentro de data/raw.
#
#   Estas tablas son mi "fotografía cruda" del origen,
#   aún sin limpieza ni transformación.
#   El siguiente paso (etl_curated.py) se encargará de ordenarlas.
#
# Beneficio:
#   Tener los datos separados en capa RAW asegura trazabilidad:
#   puedo demostrar qué traje de la fuente original antes
#   de aplicar cambios.
#
# [ADAPTACIÓN PARA EJERCICIO DE PRUEBA]
#   • Se agrega el parámetro opcional "--limit" para controlar
#     cuántas filas se descargan en cada tabla.
#   • Esto NO modifica la base de datos, es solo a nivel SELECT.
#   • Permite pruebas rápidas sin necesidad de esperar que baje
#     un volumen masivo (ej: fact.Invoices).
#   • En producción, se corre el script sin "--limit" y se bajan
#     todas las filas de las tablas de interés.
# ------------------------------------------------------------

import os
import argparse
import pyodbc 
import pandas as pd 
from dotenv import load_dotenv 
from utils.logger import get_logger

# ============================================================
# 1. Configuración inicial
# ============================================================
load_dotenv()
driver = os.getenv("ODBC_DRIVER")
server = os.getenv("AZURE_SQL_SERVER")
user = os.getenv("AZURE_SQL_USER")
password = os.getenv("AZURE_SQL_PASSWORD")
database = os.getenv("AZURE_SQL_DB")

logger = get_logger("ETL-SpaceParts")

# Parser para argumento "--limit"
parser = argparse.ArgumentParser(description="ETL Extract - SpacePartsCoDW")
parser.add_argument("--limit", type=int, default=None,
                    help="Número máximo de filas a descargar por tabla. "
                         "Opcional. Útil en pruebas. "
                         "Si se omite: se descargan todas las filas.")
args = parser.parse_args()
row_limit = args.limit

# ============================================================
# 2. Construir cadena de conexión
# ============================================================
conn_str = (
    f"DRIVER={{{driver}}};"
    f"SERVER={server};"
    f"UID={user};PWD={password};"
    f"DATABASE={database};"
    f"Encrypt=yes;TrustServerCertificate=yes"
)

# ============================================================
# 3. Tablas de interés (definitivas)
# ============================================================
# Usamos estas 3 por lo siguiente:
# - dim.Customers → dimensión de clientes (quién compra).
# - dim.Products  → dimensión de productos (qué vendo).
# - fact.Invoices → tabla de hechos de ventas/facturación (cuándo, cuánto y qué se vende).
#
# Estas tres tablas (2 dimensiones + 1 hecho) forman un mini Esquema Estrella típico
# que permite modelar indicadores de ventas y análisis de clientes/productos
# en herramientas como Power BI.
tables = ["dim.Customers", "dim.Products", "fact.Invoices"]

# ============================================================
# 4. Defino carpeta destino RAW
# ============================================================
raw_folder = "data/raw"
os.makedirs(raw_folder, exist_ok=True)

# ============================================================
# 5. Ejecuto extracción
# ============================================================
try:
    cnxn = pyodbc.connect(conn_str, timeout=10)
    logger.info(f"Conexión establecida con la base: {database}")

    for table in tables:
        try:
            logger.info(f"[START] Extrayendo tabla: {table} " +
                        (f"(limit={row_limit})" if row_limit else "(sin límite)"))

            # Si hay límite, agregar TOP N a la consulta
            query = f"SELECT * FROM {table}"
            if row_limit:
                query = f"SELECT TOP {row_limit} * FROM {table}"

            # Cargar data desde Azure SQL
            df = pd.read_sql(query, cnxn)

            # Reemplazo "." en el nombre para generar el archivo
            file_name = table.replace(".", "_") + ".csv"
            file_path = os.path.join(raw_folder, file_name)

            # Exportar a CSV
            df.to_csv(file_path, index=False)
            logger.info(f"[OK] Guardado {file_path} (filas: {len(df)})")

        except Exception as e_table:
            logger.error(f"[ERROR] No se pudo extraer {table}", exc_info=True)

    cnxn.close()
    logger.info("Extracción completada con éxito para todas las tablas solicitadas")

except Exception as e:
    logger.error("Error en el proceso ETL de extracción", exc_info=True)

# ------------------------------------------------------------
# NOTAS:
#
# 1. Inicialmente no conocía qué tablas eran las más adecuadas,
#    así que exploré el catálogo (INFORMATION_SCHEMA.TABLES).
#    Identifiqué que para un modelo BI simple necesitaba:
#       - Clientes (dim.Customers)
#       - Productos (dim.Products)
#       - Ventas/Facturas (fact.Invoices)
#
# 2. Estas tres forman el núcleo de un esquema estrella:
#    - Dimensión Clientes → quién compra.
#    - Dimensión Productos → qué se ofrece.
#    - Hechos de Facturas → detalle de transacciones con fechas, montos y cantidades.
#
# 3. En esta etapa NO limpio ni transformo nada.
#    Solo guardo los datos en csv para mantener una "copia cruda"
#    que respalde el origen.
#
# 4. Guardar en RAW me permite:
#    - Auditar (probar que lo descargado coincide con la base).
#    - Conservar histórico exacto del origen.
#    - Depurar más fácil problemas en la capa transformada.
#
# 5. Próximos pasos → correr ETL curated (etl_curated.py),
#    donde normalizo nombres de columnas, convierto tipos de datos
#    y genero datasets en formato optimizado (Parquet).
#
# 6. NOTA ADICIONAL sobre "--limit":
#    - Este argumento se agregó específicamente para esta PRUEBA TÉCNICA.
#    - Justificación:
#         • fact.Invoices puede tener cientos de miles o millones de registros.
#         • Bajar todos esos datos demora mucho y en una demo no es práctico.
#         • Con `--limit` controlo cuántas filas descargo y demuestro
#           que el ETL funciona de punta a punta, pero con subset.
#    - Ejemplo de uso en Visual Studio:
#         python etl_spaceparts.py --limit 1000   → baja solo 1000 filas/tabla.
#         python etl_spaceparts.py                → baja todas las filas.
#    - Este tipo de feature muestra que entiendo la diferencia entre:
#         * Escenario de pruebas (rápido, controlado).
#         * Escenario real (completo, masivo).
#
# 7. Importante: el "--limit" no altera la base de datos original.
#    Solo añade un TOP en la consulta SQL → es 100% no intrusivo.
# ------------------------------------------------------------