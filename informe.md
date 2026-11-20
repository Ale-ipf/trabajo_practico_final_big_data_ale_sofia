# Informe ETL — SuperAhorro

## Trabajo realizado

Se implementó un pipeline idempotente en `pipeline.py` que lee la tabla `movimientos` de `data/stock.sqlite` sin modificarla. En `stock.duckdb` genera `raw_movimientos`, `validos`, `rechazados`, `rechazados_limpios` y `tabla_final`. El origen tiene 2.575 filas y 2.500 IDs distintos.

## Calidad y limpieza

Se aplicaron 11 reglas nombradas: ID no nulo y único; fecha válida; ID de producto permitido; producto, categoría y proveedor coherentes con el producto; sucursal y tipo de movimiento permitidos; cantidad entera positiva; costo unitario positivo.

Se detectaron fechas `DD/MM/YYYY`, sucursales y tipos de movimiento con espacios o mayúsculas, datos de producto/categoría/costo faltantes, cantidades negativas e IDs repetidos. Se normalizaron las fechas y cadenas, se corrigieron las cantidades negativas por su magnitud y se recuperaron producto, categoría, proveedor y costo faltantes usando el ID de producto como referencia. Los IDs repetidos se consolidaron. Los motivos pueden superponerse.

| Etapa | Filas |
|---|---:|
| `raw_movimientos` | 2.575 |
| `validos` | 1.936 |
| `rechazados` | 639 |
| `rechazados_limpios` | 564 |
| `tabla_final` (IDs únicos) | 2.500 |

Motivos registrados: 152 filas con categoría faltante/incoherente, 148 filas pertenecientes a IDs duplicados, 130 sin costo unitario válido, 127 sin producto válido y 100 con cantidad no positiva.

## Respuestas de negocio

**Unidades vendidas por categoría:** Lacteos 11.060; Limpieza 10.952; Frutas 10.202; Bebidas 10.123; Almacen 9.564.

**Unidades de merma por sucursal:** Centro 2.861; Sur 2.462; Norte 2.395.

**Gasto en ingresos por proveedor:** Ala $8.193.400; Verdurama $6.797.300; CocaCola $5.753.000; Molinos $4.762.800; LaSerenisima $3.065.450.

**Producto más vendido:** Lavandina, 5.901 unidades.

**Movimientos de enero a junio por año:** 2024: enero 117, febrero 109, marzo 110, abril 118, mayo 126, junio 97. 2025: enero 103, febrero 90, marzo 95, abril 91, mayo 103, junio 107.

## Orquestación

El DAG `pipeline_stock_superahorro` encadena extracción, validación, limpieza, unión y consultas con horario `0 2 * * *` (todos los días a las 2:00, zona `America/Argentina/Buenos_Aires`). Para ejecutarlo se requiere Airflow en Linux/WSL2 y configurar `ETL_PROJECT_ROOT`.
