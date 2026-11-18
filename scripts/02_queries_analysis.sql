-- FLEETLOGIX — OCHO CONSULTAS SELECCIONADAS DEL AVANCE 2
-- Consultas originales sin cambios; consultar el notebook para 12 y observaciones de la consulta 9.

-- Query 1: Contar vehículos por tipo
SELECT 
    vehicle_type,
    COUNT(*) as cantidad
FROM vehicles
GROUP BY vehicle_type
ORDER BY cantidad DESC;

-- Query 2: Listar conductores con licencia próxima a vencer
SELECT 
    first_name,
    last_name,
    license_number,
    license_expiry
FROM drivers
WHERE license_expiry < CURRENT_DATE + INTERVAL '30 days'
ORDER BY license_expiry;

-- Query 4: Total de entregas por ciudad destino en los últimos 2 meses
SELECT 
    r.destination_city,
    COUNT(DISTINCT t.trip_id) as total_viajes,
    COUNT(d.delivery_id) as total_entregas,
    SUM(d.package_weight_kg) as peso_total_kg
FROM routes r
INNER JOIN trips t ON r.route_id = t.route_id
INNER JOIN deliveries d ON t.trip_id = d.trip_id
WHERE t.departure_datetime >= CURRENT_DATE - INTERVAL '60 days'
GROUP BY r.destination_city
ORDER BY total_entregas DESC;

-- Query 5: Conductores activos con cantidad de viajes completados
SELECT 
    d.driver_id,
    d.first_name || ' ' || d.last_name as nombre_completo,
    d.license_expiry,
    COUNT(t.trip_id) as viajes_totales,
    SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END) as viajes_completados
FROM drivers d
LEFT JOIN trips t ON d.driver_id = t.driver_id
WHERE d.status = 'active'
GROUP BY d.driver_id, d.first_name, d.last_name, d.license_expiry
HAVING COUNT(t.trip_id) > 0
ORDER BY viajes_completados DESC;

-- Query 6: Promedio de entregas por conductor en los últimos 6 meses
SELECT 
    dr.driver_id,
    dr.first_name || ' ' || dr.last_name as conductor,
    COUNT(DISTINCT t.trip_id) as total_viajes,
    COUNT(d.delivery_id) as total_entregas,
    ROUND(COUNT(d.delivery_id)::NUMERIC / NULLIF(COUNT(DISTINCT t.trip_id), 0), 2) as promedio_entregas_por_viaje,
    ROUND(COUNT(d.delivery_id)::NUMERIC / 180, 2) as promedio_entregas_diarias
FROM drivers dr
INNER JOIN trips t ON dr.driver_id = t.driver_id
INNER JOIN deliveries d ON t.trip_id = d.trip_id
WHERE t.departure_datetime >= CURRENT_DATE - INTERVAL '6 months'
    AND t.status = 'completed'
GROUP BY dr.driver_id, dr.first_name, dr.last_name
HAVING COUNT(DISTINCT t.trip_id) >= 10
ORDER BY promedio_entregas_por_viaje DESC;

-- Query 8: Análisis de entregas retrasadas por día de la semana
SELECT 
    TO_CHAR(d.scheduled_datetime, 'Day') as dia_semana,
    EXTRACT(DOW FROM d.scheduled_datetime) as num_dia,
    COUNT(*) as total_entregas,
    COUNT(CASE 
        WHEN d.delivered_datetime > d.scheduled_datetime + INTERVAL '30 minutes' 
        THEN 1 
    END) as entregas_retrasadas,
    ROUND(100.0 * COUNT(CASE 
        WHEN d.delivered_datetime > d.scheduled_datetime + INTERVAL '30 minutes' 
        THEN 1 
    END) / COUNT(*), 2) as porcentaje_retrasos,
    AVG(EXTRACT(EPOCH FROM (d.delivered_datetime - d.scheduled_datetime)) / 60) as minutos_promedio_diferencia
FROM deliveries d
WHERE d.delivery_status = 'delivered'
    AND d.scheduled_datetime >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY dia_semana, num_dia
ORDER BY num_dia;

-- Query 9: Costo de mantenimiento por kilómetro recorrido
WITH vehicle_metrics AS (
    SELECT 
        v.vehicle_id,
        v.vehicle_type,
        v.license_plate,
        COUNT(DISTINCT t.trip_id) as total_viajes,
        SUM(r.distance_km) as km_totales,
        SUM(m.cost) as costo_mantenimiento_total,
        COUNT(DISTINCT m.maintenance_id) as cantidad_mantenimientos
    FROM vehicles v
    LEFT JOIN trips t ON v.vehicle_id = t.vehicle_id
    LEFT JOIN routes r ON t.route_id = r.route_id
    LEFT JOIN maintenance m ON v.vehicle_id = m.vehicle_id
    WHERE t.status = 'completed'
    GROUP BY v.vehicle_id, v.vehicle_type, v.license_plate
)
SELECT 
    vehicle_type,
    COUNT(vehicle_id) as cantidad_vehiculos,
    SUM(total_viajes) as viajes_totales,
    SUM(km_totales) as kilometros_totales,
    SUM(costo_mantenimiento_total) as costo_total_mantenimiento,
    ROUND(SUM(costo_mantenimiento_total) / NULLIF(SUM(km_totales), 0), 2) as costo_por_km,
    ROUND(AVG(costo_mantenimiento_total / NULLIF(cantidad_mantenimientos, 0)), 2) as costo_promedio_por_mantenimiento
FROM vehicle_metrics
WHERE km_totales > 0 AND costo_mantenimiento_total > 0
GROUP BY vehicle_type
ORDER BY costo_por_km DESC;

-- Query 10: Ranking de conductores por eficiencia usando Window Functions
WITH conductor_metricas AS (
    SELECT 
        d.driver_id,
        d.first_name || ' ' || d.last_name as nombre,
        COUNT(DISTINCT t.trip_id) as viajes,
        COUNT(DISTINCT del.delivery_id) as entregas,
        AVG(t.fuel_consumed_liters / NULLIF(r.distance_km, 0)) * 100 as consumo_100km,
        COUNT(CASE WHEN del.delivered_datetime <= del.scheduled_datetime THEN 1 END)::NUMERIC / 
            NULLIF(COUNT(del.delivery_id), 0) * 100 as puntualidad_pct
    FROM drivers d
    JOIN trips t ON d.driver_id = t.driver_id
    JOIN routes r ON t.route_id = r.route_id
    LEFT JOIN deliveries del ON t.trip_id = del.trip_id
    WHERE t.departure_datetime >= CURRENT_DATE - INTERVAL '3 months'
    GROUP BY d.driver_id, d.first_name, d.last_name
    HAVING COUNT(DISTINCT t.trip_id) >= 20
)
SELECT 
    nombre,
    viajes,
    entregas,
    ROUND(consumo_100km, 2) as consumo_100km,
    ROUND(puntualidad_pct, 2) as puntualidad_pct,
    RANK() OVER (ORDER BY puntualidad_pct DESC) as rank_puntualidad,
    RANK() OVER (ORDER BY consumo_100km ASC) as rank_eficiencia,
    RANK() OVER (ORDER BY entregas DESC) as rank_productividad,
    (RANK() OVER (ORDER BY puntualidad_pct DESC) + 
     RANK() OVER (ORDER BY consumo_100km ASC) + 
     RANK() OVER (ORDER BY entregas DESC)) / 3.0 as score_promedio
FROM conductor_metricas
ORDER BY score_promedio ASC
LIMIT 20;
