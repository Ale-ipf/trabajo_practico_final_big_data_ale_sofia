-- Reglas aplicadas por nombre en pipeline.py:
-- id_movimiento_no_nulo, id_movimiento_unico, fecha_valida,
-- id_producto_permitido, producto_coherente_con_id,
-- categoria_coherente_con_producto, proveedor_coherente_con_producto,
-- sucursal_permitida, tipo_movimiento_permitido,
-- cantidad_entera_positiva, costo_unitario_positivo.
-- Un registro se acepta solo si no falla ninguna regla.

SELECT regla, COUNT(*) AS filas_rechazadas
FROM (
    SELECT UNNEST(STRING_SPLIT(motivo_rechazo, ';')) AS regla
    FROM rechazados
)
GROUP BY regla
ORDER BY filas_rechazadas DESC, regla;
