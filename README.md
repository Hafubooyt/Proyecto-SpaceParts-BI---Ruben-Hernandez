# Proyecto SpaceParts BI | Ruben Hernandez

Este proyecto implementa un flujo **end-to-end de Business Intelligence** para la base de datos pública **SpaceParts**.  
El objetivo es simular el trabajo de un **Analista BI**, pasando por todas las fases: extracción, transformación, modelado y visualización.

---

## Contenido del repositorio

- **`data/raw/`** → datos originales descargados de la fuente  
- **`data/curated/`** → datos transformados y listos para análisis  
- **`src/`** → scripts en Python para procesos ETL y utilidades  
- **`sql/`** → consultas SQL usadas para exploración y validación  
- **`docs/`** → documentación, notas y diagramas de arquitectura  
- **`reports/`** → dashboards y reportes en Power BI  

---

## Flujo del proyecto

1. **Extracción (Extract):** conexión a la base *SpaceParts*, descarga de tablas en formato CSV/SQL.  
2. **Transformación (Transform):** limpieza de datos, normalización de fechas y creación de llaves.  
3. **Carga (Load):** almacenamiento en la capa *curated*, organizada para análisis BI.  
4. **Modelado:** diseño de un modelo en estrella optimizado para Power BI.  
5. **Visualización:** desarrollo de dashboards interactivos en Power BI.  

---

## Requisitos

- Python 3.10 o superior  
- Power BI Desktop  
- Librerías principales: `pandas`, `sqlalchemy`, `python-dotenv`  

---

## Nota de prueba
Este es un commit de prueba para demostrar el control de versiones en Git.
