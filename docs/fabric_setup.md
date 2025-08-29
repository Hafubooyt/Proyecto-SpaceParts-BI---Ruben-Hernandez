# Configuración en la nube

## 1. Contexto
El pipeline ETL fue diseñado para correr de manera local en carpetas `/data/raw`, `/data/curated` y `/data/load`.  
Sin embargo, en un entorno de producción en la nube, estas capas deben almacenarse en **servicios gestionados de datos** para asegurar seguridad, escalabilidad e integración con Microsoft Fabric / Power BI.

---

## 2. Arquitectura propuesta en la nube (Azure)

```text
Azure SQL Database  →  Extract (Python)  →  Azure Data Lake Gen2 (RAW)
                                                  │
                                                  ▼
                                     Transform (Python + pandas/pyarrow)
                                                  │
                                                  ▼
                                             Data Lake (CURATED)
                                                  │
                                                  ▼
                                         Microsoft Fabric Lakehouse (LOAD)
                                                  │
                                                  ▼
                                             Power BI Dataset