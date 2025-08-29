# ------------------------------------------------------------
# Script: main_etl.py
#
# Propósito:
#   Este script actúa como "orquestador" del pipeline ETL completo.
#   En una sola ejecución lanzo:
#
#     1. Extracción desde Azure SQL → data/raw (etl_spaceparts.py)
#     2. Transformación/Limpieza → data/curated (etl_curated.py)
#     3. Simulación de carga (LOAD) → data/load (copia los resultados finales)
#
#   Así no tengo que ejecutar varios comandos, sino un solo "pipeline".
#
# Beneficio:
#   • Automatizo el flujo End-to-End (Extract → Transform → Load).
#   • Puedo pasar un solo argumento "--limit" que afecte a todos los pasos.
#   • Es más cómodo para demos y entornos productivos.
#
# Ejemplo de ejecución:
#   python src/main_etl.py --limit 1000
#   python src/main_etl.py
# ------------------------------------------------------------

import argparse
import subprocess
import os
import sys
import shutil
from utils.logger import get_logger

logger = get_logger("Main-ETL")

# ------------------------------------------------------------
# Parser para --limit (opcional)
# ------------------------------------------------------------
parser = argparse.ArgumentParser(description="Pipeline Maestro ETL - SpacePartsCoDW")
parser.add_argument("--limit", type=int, default=None,
                    help="Número máximo de filas a procesar (se aplica en extracción y transformación)")
args = parser.parse_args()

# ------------------------------------------------------------
# Definir ruta base de src (para ubicar los sub-scripts)
# ------------------------------------------------------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
etl_extract_path = os.path.join(BASE_DIR, "etl_spaceparts.py")
etl_curated_path = os.path.join(BASE_DIR, "etl_curated.py")

# ------------------------------------------------------------
# Función auxiliar: ejecutar un sub-script
# ------------------------------------------------------------
def run_step(script_path, limit=None):
    # Usar siempre el mismo intérprete que ejecutó este script (sys.executable)
    command = [sys.executable, script_path]
    if limit:
        command += ["--limit", str(limit)]

    logger.info(f"Ejecutando: {' '.join(command)}")

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        logger.info(f"[OK] {os.path.basename(script_path)} completado.")
        # Mostrar logs del subproceso
        print(result.stdout)
    else:
        logger.error(f"[ERROR] {os.path.basename(script_path)} falló.")
        print(result.stderr)

# ------------------------------------------------------------
# Orquestación paso a paso
# ------------------------------------------------------------
logger.info("=== INICIO PIPELINE ETL - SpaceParts ===")

# Paso 1: Extracción
run_step(etl_extract_path, limit=args.limit)

# Paso 2: Transformación
run_step(etl_curated_path, limit=args.limit)

# Paso 3: Simulación de carga
logger.info("=== START LOAD PHASE ===")

# Definir carpeta load
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))
CURATED_DIR = os.path.join(DATA_DIR, "curated")
LOAD_DIR = os.path.join(DATA_DIR, "load")
os.makedirs(LOAD_DIR, exist_ok=True)

# Copiar todos los archivos de curated a load
for file in os.listdir(CURATED_DIR):
    src = os.path.join(CURATED_DIR, file)
    dst = os.path.join(LOAD_DIR, file)
    try:
        shutil.copy2(src, dst)
        logger.info(f"[LOAD] Copiado {file} → {LOAD_DIR}")
    except Exception as e:
        logger.error(f"[ERROR-LOAD] No se pudo copiar {file}: {repr(e)}")

logger.info("=== LOAD PHASE COMPLETADA ===")

# ------------------------------------------------------------
# Fin del pipeline
# ------------------------------------------------------------
logger.info("=== PIPELINE ETL COMPLETADO ===")