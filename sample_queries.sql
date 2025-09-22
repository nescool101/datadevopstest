-- Consultas de Ejemplo para Amazon Athena
-- Pipeline de Datos AWS CDK

-- =====================================================
-- CONFIGURACIÓN INICIAL
-- =====================================================

-- 1. Seleccionar el workgroup correcto
-- En la consola de Athena, seleccionar: data-pipeline-workgroup

-- 2. Verificar que la base de datos existe
SHOW DATABASES;

-- 3. Usar la base de datos del pipeline
USE data_pipeline_db;

-- 4. Listar tablas disponibles
SHOW TABLES;

-- =====================================================
-- CONSULTAS BÁSICAS
-- =====================================================

-- Consulta 1: Ver estructura de la tabla
DESCRIBE users_csv;

-- Consulta 2: Contar total de registros
SELECT COUNT(*) as total_users 
FROM users_csv;

-- Consulta 3: Ver primeros 10 registros
SELECT * 
FROM users_csv 
LIMIT 10;

-- Consulta 4: Información básica de usuarios
SELECT 
    id,
    name,
    username,
    email,
    phone
FROM users_csv
ORDER BY id;

-- =====================================================
-- CONSULTAS DE ANÁLISIS
-- =====================================================

-- Consulta 5: Usuarios por ciudad
SELECT 
    address_city,
    COUNT(*) as user_count
FROM users_csv
GROUP BY address_city
ORDER BY user_count DESC;

-- Consulta 6: Dominios de email más comunes
SELECT 
    SPLIT_PART(email, '@', 2) as email_domain,
    COUNT(*) as count
FROM users_csv
GROUP BY SPLIT_PART(email, '@', 2)
ORDER BY count DESC;

-- Consulta 7: Información de empresas
SELECT 
    company_name,
    company_catchphrase,
    COUNT(*) as employee_count
FROM users_csv
GROUP BY company_name, company_catchphrase
ORDER BY employee_count DESC;

-- Consulta 8: Usuarios con sitios web
SELECT 
    name,
    email,
    website,
    company_name
FROM users_csv
WHERE website IS NOT NULL AND website != ''
ORDER BY name;

-- =====================================================
-- CONSULTAS GEOGRÁFICAS
-- =====================================================

-- Consulta 9: Distribución geográfica
SELECT 
    address_city,
    address_zipcode,
    CAST(address_geo_lat AS DOUBLE) as latitude,
    CAST(address_geo_lng AS DOUBLE) as longitude,
    COUNT(*) as user_count
FROM users_csv
GROUP BY address_city, address_zipcode, address_geo_lat, address_geo_lng
ORDER BY user_count DESC;

-- Consulta 10: Usuarios en el hemisferio norte
SELECT 
    name,
    address_city,
    CAST(address_geo_lat AS DOUBLE) as latitude
FROM users_csv
WHERE CAST(address_geo_lat AS DOUBLE) > 0
ORDER BY latitude DESC;

-- =====================================================
-- CONSULTAS AVANZADAS
-- =====================================================

-- Consulta 11: Análisis de códigos postales
SELECT 
    SUBSTR(address_zipcode, 1, 2) as zip_prefix,
    COUNT(*) as count,
    ARRAY_AGG(DISTINCT address_city) as cities
FROM users_csv
WHERE address_zipcode IS NOT NULL
GROUP BY SUBSTR(address_zipcode, 1, 2)
ORDER BY count DESC;

-- Consulta 12: Usuarios con información completa
SELECT 
    name,
    email,
    phone,
    address_city,
    company_name
FROM users_csv
WHERE name IS NOT NULL 
    AND email IS NOT NULL 
    AND phone IS NOT NULL 
    AND address_city IS NOT NULL 
    AND company_name IS NOT NULL;

-- Consulta 13: Estadísticas de longitud de nombres
SELECT 
    MIN(LENGTH(name)) as min_name_length,
    MAX(LENGTH(name)) as max_name_length,
    AVG(LENGTH(name)) as avg_name_length,
    STDDEV(LENGTH(name)) as stddev_name_length
FROM users_csv
WHERE name IS NOT NULL;

-- =====================================================
-- CONSULTAS DE VALIDACIÓN DE DATOS
-- =====================================================

-- Consulta 14: Verificar calidad de datos
SELECT 
    'Total Records' as metric,
    COUNT(*) as value
FROM users_csv

UNION ALL

SELECT 
    'Records with Email' as metric,
    COUNT(*) as value
FROM users_csv
WHERE email IS NOT NULL AND email != ''

UNION ALL

SELECT 
    'Records with Phone' as metric,
    COUNT(*) as value
FROM users_csv
WHERE phone IS NOT NULL AND phone != ''

UNION ALL

SELECT 
    'Records with Complete Address' as metric,
    COUNT(*) as value
FROM users_csv
WHERE address_street IS NOT NULL 
    AND address_city IS NOT NULL 
    AND address_zipcode IS NOT NULL;

-- Consulta 15: Detectar posibles duplicados
SELECT 
    email,
    COUNT(*) as duplicate_count
FROM users_csv
GROUP BY email
HAVING COUNT(*) > 1;

-- =====================================================
-- CONSULTAS CON PARTICIONES (si aplica)
-- =====================================================

-- Consulta 16: Si los datos están particionados por fecha
-- (Descomenta si tu estructura incluye particiones)
/*
SELECT 
    year,
    month,
    day,
    COUNT(*) as records_per_day
FROM users_csv
WHERE year = '2024' AND month = '01'
GROUP BY year, month, day
ORDER BY day;
*/

-- =====================================================
-- CONSULTAS DE EXPORTACIÓN
-- =====================================================

-- Consulta 17: Crear vista para análisis frecuente
CREATE OR REPLACE VIEW user_summary AS
SELECT 
    id,
    name,
    email,
    address_city as city,
    company_name as company,
    CASE 
        WHEN CAST(address_geo_lat AS DOUBLE) > 0 THEN 'Northern Hemisphere'
        ELSE 'Southern Hemisphere'
    END as hemisphere
FROM users_csv;

-- Consulta 18: Usar la vista creada
SELECT 
    hemisphere,
    COUNT(*) as user_count
FROM user_summary
GROUP BY hemisphere;

-- =====================================================
-- NOTAS DE USO
-- =====================================================

/*
NOTAS IMPORTANTES:

1. Workgroup: Asegúrate de usar 'data-pipeline-workgroup'
2. Base de datos: 'data_pipeline_db'
3. Tabla principal: 'users_csv' (generada por el Crawler)

4. Para ejecutar consultas:
   - Ir a AWS Console > Athena
   - Seleccionar el workgroup correcto
   - Copiar y pegar las consultas
   - Ejecutar una por una

5. Costos:
   - Athena cobra por datos escaneados
   - Usar LIMIT para consultas de prueba
   - Considerar particionamiento para datasets grandes

6. Optimización:
   - Usar formatos columnares (Parquet) para mejor rendimiento
   - Particionar datos por fecha si es necesario
   - Comprimir datos para reducir costos

7. Troubleshooting:
   - Si no ves tablas: ejecutar el Glue Crawler primero
   - Si hay errores de permisos: verificar roles IAM
   - Si no hay datos: verificar que Lambda se ejecutó correctamente
*/