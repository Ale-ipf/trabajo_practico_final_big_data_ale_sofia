"""ETL de movimientos de stock de SuperAhorro: SQLite -> DuckDB.

La base fuente se conserva intacta en data/stock.sqlite.
Ejecutar todo: python pipeline.py
Etapas para Airflow: extract, validate, clean, finalize, queries.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parent
SOURCE_DB = ROOT / "data" / "stock.sqlite"
DB_PATH = Path(os.environ.get("DUCKDB_PATH", ROOT / "stock.duckdb"))
FIELDS = [
    "id_movimiento", "fecha", "id_producto", "producto", "categoria",
    "sucursal", "tipo_movimiento", "cantidad", "costo_unitario", "proveedor",
]
TYPES = {
    "id_movimiento": "VARCHAR",
    "fecha": "DATE",
    "id_producto": "VARCHAR",
    "producto": "VARCHAR",
    "categoria": "VARCHAR",
    "sucursal": "VARCHAR",
    "tipo_movimiento": "VARCHAR",
    "cantidad": "INTEGER",
    "costo_unitario": "DOUBLE",
    "proveedor": "VARCHAR",
}
BRANCHES = {"Sucursal Norte", "Sucursal Sur", "Sucursal Centro"}
MOVEMENT_TYPES = {"venta", "ingreso", "merma"}

# Catálogo consistente presente en la fuente. Permite recuperar producto,
# categoría, proveedor y costo cuando esos datos están ausentes.
PRODUCTS = {
    "P01": {"producto": "Leche 1L", "categoria": "Lacteos", "costo": 1100.0, "proveedor": "LaSerenisima"},
    "P02": {"producto": "Yogur", "categoria": "Lacteos", "costo": 650.0, "proveedor": "LaSerenisima"},
    "P03": {"producto": "Arroz 1kg", "categoria": "Almacen", "costo": 1800.0, "proveedor": "Molinos"},
    "P04": {"producto": "Fideos 500g", "categoria": "Almacen", "costo": 1200.0, "proveedor": "Molinos"},
    "P05": {"producto": "Gaseosa 2L", "categoria": "Bebidas", "costo": 2300.0, "proveedor": "CocaCola"},
    "P06": {"producto": "Agua 1.5L", "categoria": "Bebidas", "costo": 950.0, "proveedor": "CocaCola"},
    "P07": {"producto": "Manzana kg", "categoria": "Frutas", "costo": 2100.0, "proveedor": "Verdurama"},
    "P08": {"producto": "Banana kg", "categoria": "Frutas", "costo": 1700.0, "proveedor": "Verdurama"},
    "P09": {"producto": "Detergente", "categoria": "Limpieza", "costo": 2800.0, "proveedor": "Ala"},
    "P10": {"producto": "Lavandina", "categoria": "Limpieza", "costo": 1500.0, "proveedor": "Ala"},
}


def connect() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def extract(conn: duckdb.DuckDBPyConnection) -> None:
    """Importa movimientos desde SQLite y conserva los valores de sus columnas."""
    if not SOURCE_DB.is_file():
        raise FileNotFoundError(f"No se encuentra la base fuente: {SOURCE_DB}")
    with sqlite3.connect(SOURCE_DB) as source:
        df = pd.read_sql_query("SELECT * FROM movimientos ORDER BY rowid", source)
    if list(df.columns) != FIELDS:
        raise ValueError(f"Columnas inesperadas en movimientos: {list(df.columns)}")
    conn.register("_source_df", df)
    casts = {
        "id_movimiento": "VARCHAR", "fecha": "VARCHAR", "id_producto": "VARCHAR",
        "producto": "VARCHAR", "categoria": "VARCHAR", "sucursal": "VARCHAR",
        "tipo_movimiento": "VARCHAR", "cantidad": "INTEGER", "costo_unitario": "DOUBLE",
        "proveedor": "VARCHAR",
    }
    select = ", ".join(f'CAST("{field}" AS {kind}) AS "{field}"' for field, kind in casts.items())
    conn.execute(f"CREATE OR REPLACE TABLE raw_movimientos AS SELECT {select} FROM _source_df")
    conn.unregister("_source_df")
    print(f"raw_movimientos: {len(df)} filas cargadas desde data/stock.sqlite")


def _text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    return value or None


def _parse_date(value: Any) -> date | None:
    value = _text(value)
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _parse_integer(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else None


def _parse_number(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalise(raw: dict[str, Any]) -> dict[str, Any]:
    product_id = (_text(raw.get("id_producto")) or "").upper() or None
    return {
        "id_movimiento": _text(raw.get("id_movimiento")),
        "fecha": _parse_date(raw.get("fecha")),
        "id_producto": product_id,
        "producto": _text(raw.get("producto")),
        "categoria": _text(raw.get("categoria")),
        "sucursal": _text(raw.get("sucursal")),
        "tipo_movimiento": (_text(raw.get("tipo_movimiento")) or "").lower() or None,
        "cantidad": _parse_integer(raw.get("cantidad")),
        "costo_unitario": _parse_number(raw.get("costo_unitario")),
        "proveedor": _text(raw.get("proveedor")),
    }


def _validation_reasons(row: dict[str, Any], duplicate: bool) -> list[str]:
    errors: list[str] = []
    if not row["id_movimiento"]:
        errors.append("id_movimiento_no_nulo")
    if duplicate:
        errors.append("id_movimiento_unico")
    if row["fecha"] is None:
        errors.append("fecha_valida")
    product_id = row["id_producto"]
    catalog = PRODUCTS.get(product_id or "")
    if catalog is None:
        errors.append("id_producto_permitido")
    else:
        if row["producto"] != catalog["producto"]:
            errors.append("producto_coherente_con_id")
        if row["categoria"] != catalog["categoria"]:
            errors.append("categoria_coherente_con_producto")
        if row["proveedor"] != catalog["proveedor"]:
            errors.append("proveedor_coherente_con_producto")
    if row["sucursal"] not in BRANCHES:
        errors.append("sucursal_permitida")
    if row["tipo_movimiento"] not in MOVEMENT_TYPES:
        errors.append("tipo_movimiento_permitido")
    if row["cantidad"] is None or row["cantidad"] <= 0:
        errors.append("cantidad_entera_positiva")
    if row["costo_unitario"] is None or row["costo_unitario"] <= 0:
        errors.append("costo_unitario_positivo")
    return errors


def _replace_typed_table(conn: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]]) -> None:
    df = pd.DataFrame(rows, columns=FIELDS)
    conn.register("_stage_df", df)
    select = ", ".join(f'CAST("{field}" AS {kind}) AS "{field}"' for field, kind in TYPES.items())
    conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT {select} FROM _stage_df")
    conn.unregister("_stage_df")


def validate(conn: duckdb.DuckDBPyConnection) -> None:
    raw = conn.execute("SELECT * FROM raw_movimientos ORDER BY rowid").fetchdf()
    if raw.empty:
        raise ValueError("raw_movimientos no tiene filas; ejecute primero la extracción")
    ids = raw["id_movimiento"].map(lambda value: _text(value) or "")
    counts = ids.value_counts()
    duplicates = {key for key, count in counts.items() if key and count > 1}
    valid_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    for raw_row in raw.to_dict(orient="records"):
        normalized = _normalise(raw_row)
        reasons = _validation_reasons(normalized, normalized["id_movimiento"] in duplicates)
        if reasons:
            rejected_rows.append({**{field: raw_row[field] for field in FIELDS}, "motivo_rechazo": ";".join(reasons)})
        else:
            valid_rows.append(normalized)
    _replace_typed_table(conn, "validos", valid_rows)
    rejected = pd.DataFrame(rejected_rows, columns=FIELDS + ["motivo_rechazo"])
    conn.register("_rejected_df", rejected)
    conn.execute("CREATE OR REPLACE TABLE rechazados AS SELECT * FROM _rejected_df")
    conn.unregister("_rejected_df")
    print(f"validos: {len(valid_rows)} | rechazados: {len(rejected_rows)}")
    if rejected_rows:
        print("Rechazos por regla (los motivos pueden superponerse):")
        counts_by_rule = rejected["motivo_rechazo"].str.split(";").explode().value_counts()
        for rule, count in counts_by_rule.items():
            print(f"  {rule}: {count}")


def _repair(raw: dict[str, Any]) -> dict[str, Any]:
    row = _normalise(raw)
    catalog = PRODUCTS.get(row["id_producto"] or "")
    if catalog:
        row["producto"] = catalog["producto"]
        row["categoria"] = catalog["categoria"]
        row["proveedor"] = catalog["proveedor"]
        if row["costo_unitario"] is None:
            row["costo_unitario"] = catalog["costo"]
    if row["cantidad"] is not None and row["cantidad"] < 0:
        row["cantidad"] = abs(row["cantidad"])
    if row["sucursal"]:
        row["sucursal"] = row["sucursal"].strip()
    return row


def clean(conn: duckdb.DuckDBPyConnection) -> None:
    rejected = conn.execute("SELECT * FROM rechazados ORDER BY rowid").fetchdf()
    candidates: list[dict[str, Any]] = []
    if not rejected.empty:
        for raw_row in rejected[FIELDS].to_dict(orient="records"):
            row = _repair(raw_row)
            if not _validation_reasons(row, duplicate=False):
                candidates.append(row)
    cleaned = pd.DataFrame(candidates, columns=FIELDS)
    if not cleaned.empty:
        cleaned = cleaned.drop_duplicates(subset=["id_movimiento"], keep="first")
    rows = cleaned.to_dict(orient="records")
    _replace_typed_table(conn, "rechazados_limpios", rows)
    print(f"rechazados_limpios: {len(rows)} movimientos recuperados")


def finalize(conn: duckdb.DuckDBPyConnection) -> None:
    sql_path = ROOT / "consultas" / "unir.sql"
    conn.execute(sql_path.read_text(encoding="utf-8"))
    counts = conn.execute("""
        SELECT (SELECT COUNT(*) FROM raw_movimientos),
               (SELECT COUNT(*) FROM validos),
               (SELECT COUNT(*) FROM rechazados),
               (SELECT COUNT(*) FROM rechazados_limpios),
               (SELECT COUNT(*) FROM tabla_final),
               (SELECT COUNT(DISTINCT id_movimiento) FROM tabla_final)
    """).fetchone()
    print("Conteos raw, validos, rechazados, recuperados, final, IDs únicos:", counts)


def run_queries(conn: duckdb.DuckDBPyConnection) -> None:
    path = ROOT / "consultas" / "consultas.sql"
    statements = [part.strip() for part in path.read_text(encoding="utf-8").split(";") if part.strip()]
    for i, statement in enumerate(statements, start=1):
        result = conn.execute(statement).fetchdf()
        conn.register(f"_query_{i}", result)
        conn.execute(f"CREATE OR REPLACE TABLE resultado_consulta_{i} AS SELECT * FROM _query_{i}")
        conn.unregister(f"_query_{i}")
        print(f"\nConsulta {i}:\n{result.to_string(index=False)}")


def run_step(step: str) -> None:
    with connect() as conn:
        if step == "all":
            extract(conn)
            validate(conn)
            clean(conn)
            finalize(conn)
            run_queries(conn)
        elif step == "extract":
            extract(conn)
        elif step == "validate":
            validate(conn)
        elif step == "clean":
            clean(conn)
        elif step == "finalize":
            finalize(conn)
        elif step == "queries":
            run_queries(conn)
        else:
            raise ValueError(f"Etapa desconocida: {step}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline ETL de movimientos de stock")
    parser.add_argument("--step", choices=["all", "extract", "validate", "clean", "finalize", "queries"], default="all")
    run_step(parser.parse_args().step)
