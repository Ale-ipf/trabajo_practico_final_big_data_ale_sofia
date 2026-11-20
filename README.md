# Pipeline ETL de movimientos de stock — SuperAhorro

## Ejecutar el ETL

La fuente original es `data/stock.sqlite`, tabla `movimientos`. El pipeline la lee sin modificarla. Con Python y los paquetes de `requirements.txt` instalados:

```powershell
env\Scripts\python.exe pipeline.py
```

El comando reconstruye las tablas de `stock.duckdb` y muestra las respuestas de negocio. Para ejecutar una etapa por separado:

```powershell
env\Scripts\python.exe pipeline.py --step extract
env\Scripts\python.exe pipeline.py --step validate
env\Scripts\python.exe pipeline.py --step clean
env\Scripts\python.exe pipeline.py --step finalize
env\Scripts\python.exe pipeline.py --step queries
```

## Estructura

- `pipeline.py`: extracción desde SQLite, validación, limpieza y unión.
- `consultas/consultas.sql`: las cinco preguntas de negocio.
- `consultas/`: SQL auxiliar y reglas aplicadas.
- `stock.duckdb`: base generada por el pipeline.
- `dags/dag_pipeline.py`: DAG diario de Airflow.
- `informe.md` / `informe.pdf`: informe del trabajo.

## Airflow en WSL2/Linux

Airflow no corre de forma nativa en Windows. En WSL2, instalar Airflow en el entorno de Linux y mantener el repositorio accesible desde WSL. Definir `ETL_PROJECT_ROOT` con la ruta Linux absoluta del proyecto para que el DAG encuentre `pipeline.py`, `data/stock.sqlite` y la base DuckDB. Copiar `dags/dag_pipeline.py` a la carpeta `dags/` de Airflow y asegurar permisos de lectura sobre la fuente y de escritura sobre la carpeta del proyecto. El DAG se programa con `0 2 * * *` y zona `America/Argentina/Buenos_Aires`.
