-- FLEETLOGIX | Cinco índices documentados en Avance 2
-- CREATE INDEX IF NOT EXISTS: ejecutar SOLO si el índice no existe y tras revisar pg_indexes.
-- Nombres aquí propuestos: los nombres reales del servidor pueden ser diferentes.

-- Consultas: 4, 6, 10
CREATE INDEX IF NOT EXISTS idx_deliveries_trip_id ON public.deliveries (trip_id);

-- Consultas: 8, 12
CREATE INDEX IF NOT EXISTS idx_deliveries_scheduled_datetime ON public.deliveries (scheduled_datetime);

-- Consultas: 5, 6, 10
CREATE INDEX IF NOT EXISTS idx_trips_driver_id ON public.trips (driver_id);

-- Consultas: 9
CREATE INDEX IF NOT EXISTS idx_trips_vehicle_id ON public.trips (vehicle_id);

-- Consultas: 9
CREATE INDEX IF NOT EXISTS idx_maintenance_vehicle_id ON public.maintenance (vehicle_id);

-- Verificación (no modifica datos):
SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename IN ('deliveries','trips','maintenance') ORDER BY tablename, indexname;
