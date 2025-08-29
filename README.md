# Proyecto SpaceParts BI | Ruben Hernandez

Este proyecto implementa un flujo **end-to-end de Business Intelligence** para la base de datos pública **SpaceParts**.  
El objetivo es simular el trabajo de un **Senior BI Analyst**, pasando por todas las fases: **extracción → transformación → modelado → visualización**, incluyendo prácticas de **control de calidad de datos y manejo de GitFlow**.  

---

## 📂 Contenido del repositorio

- **`data/raw/`** → datos originales descargados de la fuente  
- **`data/curated/`** → datos transformados y listos para análisis (CSV/Parquet) + perfiles de calidad (JSON/CSV)  
- **`src/`** → scripts en Python para procesos ETL y utilidades  
- **`sql/`** → consultas SQL usadas para exploración y validación  
- **`docs/`** → documentación, notas y diagramas de arquitectura (`etl_flow.md`, `fabric_setup.md`)  
- **`reports/`** → dashboards y reportes en Power BI  
- **`logs/`** → registros de ejecución del ETL  

---

## ⚙️ Flujo del proyecto

1. **Extracción (Extract):**  
   - `etl_spaceparts.py` conecta a la base SpaceParts y descarga tablas a **/data/raw**.  

2. **Transformación (Transform):**  
   - `etl_curated.py` realiza limpieza de datos, normalización de columnas y tipos.  
   - Corrección de **valores negativos en fact_Invoices** → transformados con `ABS()`.  
   - Creación de columnas derivadas:  
     - `gross_invoice_value = quantity * unitprice`  
     - `profit = gross_invoice_value - net_invoice_cogs`  
   - **Perfilado automático:** cada tabla genera `profile_<tabla>.json` y `profile_<tabla>.csv` con:  
     - filas, columnas, nulos, duplicados y % de nulos por columna.  

3. **Carga (Load):**  
   - `main_etl.py` orquesta todo el pipeline.  
   - Archivos finales en formato **CSV y Parquet** en la capa **curated**, para análisis en Fabric/Power BI.  

4. **Modelado de datos (Power BI):**  
   - Modelo estrella con `fact_Invoices` + `dim_Customers` + `dim_Products`.  
   - Relaciones optimizadas y jerarquías.  
   - Medidas DAX clave:  
     - `Total Sales`, `Total COGS`, `Total Profit`, `Profit Margin %`  
     - `Total Deliveries`, `Total Customers`, `Total Products`, `Avg Sales per Customer`  

5. **Visualización (Power BI):**  
   - KPIs ejecutivos (ventas totales, profit, margen %, clientes y productos únicos, entregas, ticket promedio).  
   - Gráficos temporales (Sales vs Profit, Sales vs COGS/Freight).  
   - Breakdown **Top Customers** y **Top Products**.  
   - Donut de participación en ventas por **Sub Brand**.  
   - Tabla detalle con totales.  

6. **Nube (Microsoft Fabric):**  
   - Datasets **Curated** y **Profiling** publicados en un **Lakehouse**.  
   - Conexión Power BI → Fabric Lakehouse.  
   - Refresh automático configurado.  

---

## 📦 Requisitos

- Python **3.10 o superior**  
- Power BI Desktop  
- Dependencias en `requirements.txt`:  
  - `pandas`, `pyodbc`, `python-dotenv`, `sqlalchemy`, `pyarrow`  

---

## 🔀 Git Flow

Se utiliza un flujo de ramas estilo **GitFlow**:
- `main` → rama estable de despliegue/presentación.  
- `develop` → rama de integración.  
- `feature/*` y `fix/*` → ramas específicas de desarrollo y corrección.  

Ejemplo de commits:
```bash
feat(etl): exportar perfilado de calidad en JSON y CSV para cada tabla
docs(sql): agregar script de exploración y validación de datos
fix(etl): normalizar valores negativos en fact_Invoices
