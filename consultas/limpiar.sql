-- La normalización y recuperación se hace en pandas en pipeline.py:
-- fechas DD/MM/YYYY, espacios y mayúsculas, producto/categoría/proveedor/costo
-- faltantes deducibles de id_producto, y cantidades negativas por su magnitud.
-- Los registros no reparables se conservan en rechazados.
SELECT COUNT(*) AS filas_recuperadas
FROM rechazados_limpios;
