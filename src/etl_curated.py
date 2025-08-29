# ------------------------------------------------------------
# Script: etl_curated.py
#
# Propósito:
#   Este script toma los archivos crudos (raw) descargados de la base,
#   los limpia y normaliza para generar datasets en la carpeta "curated".
#
#   Ejemplo de lo que hago:
#   - Estandarizo nombres de columnas (snake_case).
#   - Elimino nulos o duplicados.
#   - Convierto tipos de datos (ej: texto a fecha, precio a numérico).
#   - Guardo resultados tanto en CSV como en Parquet (más eficiente).
#
# Beneficio:
#   Estos archivos "curated" ya están listos para conectarse en 
#   Microsoft Fabric o Power BI como modelo analítico de BI.
#
# [ADAPTACIÓN PARA LA PRUEBA TÉCNICA]
#   • Agrego el argumento opcional "--limit" para procesar solo un subconjunto de filas.
#   • Esto acelera la demo, evitando transformar datasets completos si son muy grandes.
#   • En producción se omite "--limit" y se procesan todas las filas.
# ------------------------------------------------------------

import os
import argparse
import pandas as pd   
from utils.logger import get_logger

# ============================================================
# 1. Configuración de carpetas
# ============================================================
RAW_FOLDER = os.path.join("data", "raw")
CURATED_FOLDER = os.path.join("data", "curated")
os.makedirs(CURATED_FOLDER, exist_ok=True)

# Instancia del logger (para registrar mensajes en consola + archivo)
logger = get_logger("ETL-Curated")

# ============================================================
# 2. Parser para argumento --limit
# ============================================================
parser = argparse.ArgumentParser(description="ETL Curated - SpacePartsCoDW")
parser.add_argument("--limit", type=int, default=None,
                    help="Número máximo de filas a procesar por tabla (opcional, útil en pruebas).")
args = parser.parse_args()
row_limit = args.limit

# ============================================================
# 3. Lista de tablas (esperadas desde RAW)
# ============================================================
tables = ["dim_Customers", "dim_Products", "fact_Invoices"]

# ------------------------------------------------------------
# Función de perfilado rápido de calidad
# Me sirve para obtener un vistazo inmediato de:
#   - cantidad de filas y columnas
#   - cuántos nulos hay en total y en % por columna
#   - duplicados
# Esto es oro para contar la historia de "antes/después"
# en mi sustentación final.
# ------------------------------------------------------------
def quick_profile(df):
    profile = {
        "rows": len(df),
        "cols": df.shape[1],
        "null_total": int(df.isna().sum().sum()),
        "null_percent_by_col": (df.isna().mean() * 100).round(2).to_dict(),
        "duplicates": int(df.duplicated().sum())
    }
    return profile

# ============================================================
# 4. Proceso tabla por tabla
# ============================================================
for table in tables:
    raw_path = os.path.join(RAW_FOLDER, f"{table}.csv")
    curated_path_csv = os.path.join(CURATED_FOLDER, f"{table}_curated.csv")
    curated_path_parquet = os.path.join(CURATED_FOLDER, f"{table}_curated.parquet")

    if not os.path.exists(raw_path):
        logger.warning(f"[SKIP] No existe {raw_path}. Me salto {table}")
        continue

    logger.info(f"[START] Procesando {table} desde {raw_path} "
                f"(limit={row_limit if row_limit else 'ALL'})")

    # --------------------------------------------------------
    # Paso 1: Cargo el CSV crudo (RAW)
    # --------------------------------------------------------
    try:
        df = pd.read_csv(raw_path, encoding="utf-8-sig")

        # Aplico límite si fue indicado
        if row_limit:
            df = df.head(row_limit)

    except Exception as e:
        logger.error(f"[ERROR] No pude leer {raw_path}: {repr(e)}")
        continue

    # --------------------------------------------------------
    # Paso 2: Transformaciones específicas según tabla
    # --------------------------------------------------------

    if table == "dim_Customers":
        # Estandarizo nombres de columnas a snake_case
        df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]

        # Renombro columnas claves (más amigables para BI)
        rename_map = {
            "customerid": "id_cliente",
            "firstname": "nombre",
            "lastname": "apellido"
        }
        df = df.rename(columns={c: rename_map[c] for c in df.columns if c in rename_map})

        # Elimino filas sin clave de cliente
        if "id_cliente" in df.columns:
            df = df.dropna(subset=["id_cliente"])
            df["id_cliente"] = df["id_cliente"].astype(int)

    elif table == "dim_Products":
        # Estandarizo nombres de columnas
        df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]

        # Normalizo precios a numérico
        if "price" in df.columns:
            df["price"] = pd.to_numeric(df["price"], errors="coerce").round(2)

    elif table == "fact_Invoices":
        # =====================================================
        # Transformaciones de la tabla de hechos (Fact Invoices)
        # =====================================================
        df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]

        # ------------------------------------------
        # Paso: Identifico y convierto columna de fecha
        # ------------------------------------------
        date_cols = [c for c in df.columns if "date" in c or "orderdate" in c or "invoice_date" in c]
        if date_cols:
            col = date_cols[0]
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df = df.dropna(subset=[col])
            df["date_iso"] = df[col].dt.strftime("%Y-%m-%d")

        # ------------------------------------------
        # Paso: Limpieza de campos numéricos
        # ------------------------------------------
        if "quantity" in df.columns:
            df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
        if "unitprice" in df.columns:
            df["unitprice"] = pd.to_numeric(df["unitprice"], errors="coerce").round(2)

        # Valores originales que venían negativos → los normalizo
        if "net_invoice_value" in df.columns:
            df["net_invoice_value"] = pd.to_numeric(df["net_invoice_value"], errors="coerce").fillna(0)
            df["net_invoice_value"] = df["net_invoice_value"].abs()

        if "net_invoice_cogs" in df.columns:
            df["net_invoice_cogs"] = pd.to_numeric(df["net_invoice_cogs"], errors="coerce").fillna(0)
            df["net_invoice_cogs"] = df["net_invoice_cogs"].abs()

        # ------------------------------------------
        # Paso: Columnas derivadas para BI (más claridad)
        # ------------------------------------------
        # Gross sales = quantity * unit price
        if "quantity" in df.columns and "unitprice" in df.columns:
            df["gross_invoice_value"] = (df["quantity"] * df["unitprice"]).round(2)
        else:
            # fallback por si no vienen columnas separadas
            df["gross_invoice_value"] = df.get("net_invoice_value", 0)

        # Profit = Gross invoice - COGS (ya positivizados)
        if "gross_invoice_value" in df.columns and "net_invoice_cogs" in df.columns:
            df["profit"] = df["gross_invoice_value"] - df["net_invoice_cogs"]

    # --------------------------------------------------------
    # Paso 3: Perfilado rápido (útil para storytelling demo)
    # --------------------------------------------------------
    profile = quick_profile(df)
    logger.info(f"Perfil [{table}] -> filas: {profile['rows']}, "
                f"columnas: {profile['cols']}, "
                f"nulos totales: {profile['null_total']}, "
                f"duplicados: {profile['duplicates']}")

    # --------------------------------------------------------
    # Paso 4: Guardar resultados (CSV + Parquet)
    # --------------------------------------------------------
    try:
        df.to_csv(curated_path_csv, index=False, encoding="utf-8-sig")
        df.to_parquet(curated_path_parquet, index=False)
        logger.info(f"[OK] Guardado {curated_path_csv} y {curated_path_parquet}")
    except Exception as e:
        logger.warning(f"[WARN] Parquet falló: {repr(e)}. Solo guardo CSV.")
        df.to_csv(curated_path_csv, index=False, encoding="utf-8-sig")

    # --------------------------------------------------------
    # Paso extra: Exportar perfilado de calidad
    # --------------------------------------------------------
    try:
        # Rutas de guardado
        profile_json_path = os.path.join(CURATED_FOLDER, f"profile_{table}.json")
        profile_csv_path = os.path.join(CURATED_FOLDER, f"profile_{table}.csv")

        # Exportar JSON
        import json
        with open(profile_json_path, "w", encoding="utf-8") as f:
            json.dump(profile, f, indent=2, ensure_ascii=False)

        # Exportar CSV (conviertiendo dict a DataFrame)
        pd.DataFrame([profile]).to_csv(profile_csv_path, index=False, encoding="utf-8-sig")

        logger.info(f"[OK] Perfilado exportado -> {profile_json_path}, {profile_csv_path}")
    except Exception as e:
        logger.warning(f"[WARN] No pude exportar perfilado de {table}: {repr(e)}")

logger.info("[END] ETL curated completado correctamente.")

# ------------------------------------------------------------
# NOTAS:
# 
# 1. Este script es el "T" de ETL → limpia y normaliza lo extraído.
# 2. Manejo 3 tablas claves de un modelo estrella (Clientes, Productos, Facturas).
# 3. Conservo CSV para trazabilidad y genero Parquet optimizado.
# 4. En fact_Invoices ahora corrijo valores negativos con abs().
# 5. Agrego derivadas: gross_invoice_value y profit → facilitan el modelo en BI.
# 6. El argumento "--limit" permite demos más rápidas sin necesidad de procesar todo.
# 7. Guardado en carpeta raíz "data/curated", separando bien capas RAW/Curated.
# 8. **Cambio importante (data quality): Detectamos que los campos 
#    net_invoice_value y net_invoice_cogs venían negativos desde la fuente 
#    (probablemente por notas de crédito o diseño del sistema). 
#    Se normalizan con ABS() para asegurar que las métricas de BI 
#    (ventas, COGS, profit, márgenes) se calculen correctamente.**
# ------------------------------------------------------------