-- Consultas de negocio sobre tabla_final (SuperAhorro).

-- 1. Unidades vendidas por categoría.
SELECT categoria, SUM(cantidad) AS unidades_vendidas
FROM tabla_final
WHERE tipo_movimiento = 'venta'
GROUP BY categoria
ORDER BY unidades_vendidas DESC;

-- 2. Unidades de merma por sucursal.
SELECT sucursal, SUM(cantidad) AS unidades_merma
FROM tabla_final
WHERE tipo_movimiento = 'merma'
GROUP BY sucursal
ORDER BY sucursal;

-- 3. Gasto en ingresos de mercadería por proveedor.
SELECT proveedor, ROUND(SUM(cantidad * costo_unitario), 2) AS gasto_ars
FROM tabla_final
WHERE tipo_movimiento = 'ingreso'
GROUP BY proveedor
ORDER BY gasto_ars DESC;

-- 4. Producto más vendido en unidades.
SELECT producto, SUM(cantidad) AS unidades_vendidas
FROM tabla_final
WHERE tipo_movimiento = 'venta'
GROUP BY producto
ORDER BY unidades_vendidas DESC, producto
LIMIT 1;

-- 5. Movimientos de enero a junio, desglosados por año y mes.
SELECT EXTRACT(YEAR FROM fecha)::INTEGER AS anio,
       EXTRACT(MONTH FROM fecha)::INTEGER AS mes,
       COUNT(*) AS cantidad_movimientos
FROM tabla_final
WHERE EXTRACT(MONTH FROM fecha) BETWEEN 1 AND 6
GROUP BY anio, mes
ORDER BY anio, mes;
