-- Une registros validados con filas recuperadas y prioriza validos por ID.
CREATE OR REPLACE TABLE tabla_final AS
WITH union_etapas AS (
    SELECT id_movimiento, fecha, id_producto, producto, categoria, sucursal,
           tipo_movimiento, cantidad, costo_unitario, proveedor, 1 AS prioridad
    FROM validos
    UNION ALL
    SELECT id_movimiento, fecha, id_producto, producto, categoria, sucursal,
           tipo_movimiento, cantidad, costo_unitario, proveedor, 2 AS prioridad
    FROM rechazados_limpios
    WHERE NOT EXISTS (
        SELECT 1 FROM validos v
        WHERE v.id_movimiento = rechazados_limpios.id_movimiento
    )
), unicos AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id_movimiento ORDER BY prioridad) AS rn
    FROM union_etapas
)
SELECT id_movimiento, fecha, id_producto, producto, categoria, sucursal,
       tipo_movimiento, cantidad, costo_unitario, proveedor
FROM unicos
WHERE rn = 1;
