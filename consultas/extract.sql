-- Alternativa DuckDB que requiere la extensión sqlite.
-- pipeline.py realiza este paso con sqlite3 de Python para evitar depender
-- de instalar extensiones en tiempo de ejecución.
INSTALL sqlite;
LOAD sqlite;
ATTACH 'data/stock.sqlite' AS fuente (TYPE SQLITE);
CREATE OR REPLACE TABLE raw_movimientos AS
SELECT * FROM fuente.movimientos;
DETACH fuente;
