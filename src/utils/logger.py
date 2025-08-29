# ------------------------------------------------------------
# Script: logger.py
#
# Este archivo es un "ayudante". Configura un sistema de logs
# para que todo lo que ocurra en el ETL quede registrado en:
#   1. La consola (pantalla, en vivo).
#   2. Un archivo en disco (logs/etl.log).
#
# Así tengo trazabilidad del proceso: sé qué corrió,
# a qué hora, y si hubo errores.
# ------------------------------------------------------------

import logging
import os

def get_logger(name="ETL", log_file="logs/etl.log"):
    """
    Devuelve un objeto logger configurado para este proyecto.
    
    - name: permite distinguir de qué script viene el mensaje
    - log_file: a dónde se guardará el log en disco
    
    El logger soporta niveles de mensajes: INFO, ERROR, WARNING.
    """
    # Me aseguro que la carpeta "logs" exista
    os.makedirs("logs", exist_ok=True)

    # Creo un "logger" con el nombre que yo quiera (ej: ETL-SpaceParts)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Evitar que se duplique la salida de logs si ya se definió antes
    if logger.handlers:
        return logger

    # Handler para mandar a archivo
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    # Handler para mandar a consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Defino formato uniforme: fecha, nombre, nivel, mensaje
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Agrego ambos destinos al logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger