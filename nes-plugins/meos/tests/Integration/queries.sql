-- Query 1 (Geofencing) --------------------------------------------------------
-- Location-Based Alert Filtering
-- The system determines whether a train is within a maintenance area. 
-- If confirmed, alerts such as speed violations or equipment malfunctions are filtered out, 
-- as they are redundant during maintenance.

WITH time_windows AS (
        SELECT 
            FLOOR(timestamp/30)*30 as window_start,
            FLOOR(timestamp/30)*30 + 30 as window_end
        FROM sncb)
SELECT 
    window_start,
    window_end,
    SUM(speed) as total_speed
FROM 
    sncb
    JOIN time_windows ON timestamp BETWEEN window_start AND window_end
WHERE 
    teintersects(
        ST_Point(longitude, latitude),
        ST_MakeEnvelope(maintenance area)
    ) = 1
    AND (Code1 != 0 OR Code2 != 0)
GROUP BY 
    window_start,
    window_end
ORDER BY 
    window_start;

-- Query 2 (Geofencing) --------------------------------------------------------
-- Location-Based Noise Monitoring
-- The query constantly monitors the sound levels outside the train
-- using speed and break pressure data.

WITH time_windows AS (
    SELECT 
        FLOOR(timestamp/0.02)*0.02 as window_start,
        FLOOR(timestamp/0.02)*0.02 + 0.02 as window_end
    FROM sncb
)

SELECT 
    latitude,
    longitude,
    timestamp,
    speed,
    PCFA_mbar
FROM 
    sncb s
    JOIN time_windows tw ON s.timestamp BETWEEN tw.window_start AND tw.window_end
WHERE 
    tpointatstbox(
        ST_MakeEnvelope(/* bbox coordinates */),
        ST_Point(longitude, latitude)
    ) = 1
    AND speed > 0 
    AND PCFA_mbar > 0
    AND (speed * 0.5 + PCFA_mbar * 0.5) > 10
GROUP BY 
    tw.window_start,
    latitude,
    longitude,
    timestamp,
    speed,
    PCFA_mbar
ORDER BY 
    timestamp;


-- Query 3 (Geofencing) --------------------------------------------------------
-- Dynamic Speed Limit
-- We can suggest speed restrictions dynamically, adapting to specific zones, 
-- such as curves and other construction zones.

-- First query to filter high risk areas
WITH filtered_areas AS (
    SELECT 
        timestamp as highriskArea_timestamp,
        latitude,
        longitude
    FROM highriskArea
    WHERE 
        ST_DWithin(
            ST_Point(longitude, latitude),
            ST_MakeEnvelope(),
            timestamp
        ) = 1
        AND timestamp > 0
),
    -- Define time windows
    time_windows AS (
        SELECT 
            timestamp as window_start,
            timestamp + 30 as window_end
        FROM sncb
        WHERE timestamp % 10 = 0  -- 10 second slide
    )

-- Main query with join and window
SELECT 
    s.timestamp,
    s.id,
    s.speed,
    s.Vbat
FROM 
    sncb s
    JOIN filtered_areas fa ON s.timestamp = fa.highriskArea_timestamp
    JOIN time_windows tw ON s.timestamp BETWEEN tw.window_start AND tw.window_end
WHERE 
    ABS(s.latitude - fa.latitude) <= :slackDegrees
    AND ABS(s.longitude - fa.longitude) <= :slackDegrees
GROUP BY 
    tw.window_start,
    s.timestamp,
    s.id,
    s.speed,
    s.Vbat
ORDER BY 
    s.timestamp;

-- Query 4 (Geofencing) ------------------------------------------------------
-- Weather-Based Speed Zones
-- The system uses integrated weather data to suggest speed limits for 
-- conditions such as heavy rain or fog, maintaining safety operations.

WITH weather_conditions AS (
    SELECT 
        timestamp,
        gps_lat,
        gps_lon,
        15.0 as adjusted_speed_limit  -- Fixed speed limit for adverse conditions
    FROM weather
    WHERE 
        ST_DWithin(
            ST_Point(gps_lon, gps_lat),
            ST_MakeEnvelope(/* area coordinates */),
            timestamp
        ) = 1
        AND (temperature < 12.0 OR temperature > 16.0)
        AND (
            snowfall >= 0.0 OR
            wind_speed > 15.0 OR
            precipitation > 0.1
        )
),
time_windows AS (
    SELECT 
        FLOOR(timestamp/0.01)*0.01 as window_start,  -- 10ms windows
        FLOOR(timestamp/0.01)*0.01 + 0.01 as window_end
    FROM sncb
)
SELECT 
    s.timestamp,
    s.device_id,
    s.gps_speed,
    w.adjusted_speed_limit
FROM 
    sncb s
    JOIN weather_conditions w ON s.timestamp = w.timestamp
    JOIN time_windows tw 
        ON s.timestamp BETWEEN tw.window_start AND tw.window_end
WHERE 
    ST_Contains(
        ST_MakeEnvelope(/* region coordinates */),
        ST_Point(w.gps_lon, w.gps_lat)
    )
GROUP BY 
    tw.window_start,
    s.timestamp,
    s.device_id,
    s.gps_speed,
    w.adjusted_speed_limit
ORDER BY 
    s.timestamp;


-- Query 5 (CEP) --------------------------------------------------------
-- Battery Monitoring
-- monitoring battery usage to prevent overheating and excessive discharge. 
-- It queries nearby workshops.


WITH workshops_data AS (
    SELECT 
        id,
        latitude,
        longitude,
        timestamp
    FROM workshops
),

time_windows AS (
    SELECT 
        FLOOR(timestamp/30)*30 as window_start,
        FLOOR(timestamp/30)*30 + 60 as window_end
    FROM gps
)

SELECT 
    g.timestamp,
    g.id,
    g.Vbat,
    g.speed,
    g.latitude as gps_latitude,
    g.longitude as gps_longitude,
    w.latitude as workshop_latitude,
    w.longitude as workshop_longitude
FROM 
    gps g
    JOIN workshops_data w ON g.timestamp >= w.timestamp
    JOIN time_windows tw ON g.timestamp BETWEEN tw.window_start AND tw.window_end
WHERE 
    ST_Distance(
        ST_Point(g.longitude, g.latitude),
        ST_MakeEnvelope(w.longitude, w.latitude, w.longitude, w.latitude)
    ) < 3
    AND ABS(g.latitude - w.latitude) <= 0.5
    AND ABS(g.longitude - w.longitude) <= 0.5
    AND g.Vbat < 30.0
    AND g.speed > 0.0
GROUP BY 
    tw.window_start,
    g.timestamp,
    g.id,
    g.Vbat,
    g.speed,
    g.latitude,
    g.longitude,
    w.latitude,
    w.longitude
ORDER BY 
    g.timestamp;


-- Query 6 (CEP) --------------------------------------------------------
-- Heavy Load of Passengers
-- Detect a heavy load of passengers by monitoring train sensors. 
-- These sensors include weight detectors that provide real-time data for analysis. 
-- The system can adjust temperature controls and lighting to maintain optimal conditions 
-- and resource waste in response to heavy loads. 

WITH passenger_counts AS (
    SELECT 
        timestamp,
        id,
        (PCFA_mbar + PCFF_mbar + PCF1_mbar + PCF2_mbar) / 4.0 as passenger_count
    FROM gps
    WHERE (PCFA_mbar + PCFF_mbar + PCF1_mbar + PCF2_mbar) / 4.0 > 3.15
),

time_windows AS (
    SELECT 
        FLOOR(timestamp/0.02)*0.02 as window_start,
        FLOOR(timestamp/0.02)*0.02 + 0.02 as window_end
    FROM gps
)

SELECT 
    g.timestamp,
    g.id,
    p.passenger_count,
    20.0 as adjusted_temp,
    80.0 as adjusted_light
FROM 
    gps g
    JOIN passenger_counts p ON g.timestamp = p.timestamp
    JOIN time_windows tw ON g.timestamp BETWEEN tw.window_start AND tw.window_end
WHERE 
    tpointatstbox(
        ST_MakeEnvelope(/* bbox coordinates */),
        ST_Point(g.longitude, g.latitude)
    ) = 1
    AND g.speed > -1
GROUP BY 
    tw.window_start,
    g.timestamp,
    g.id,
    p.passenger_count
ORDER BY 
    g.timestamp;

-- Query 7 (CEP) --------------------------------------------------------
-- Query Unscheduled Stops
-- The system compares the train’s real-time GPS location with known station and workshop zones. 
-- If the train’s status is stopped outside these zones, the stop is flagged as unscheduled.
-- This alert helps operators act fast, whether they need to send help, investigate mechanical problems, 
-- or address other safety concerns. It also prevents unauthorized halts that could disrupt the timetable and affect service reliability.

WITH non_workshop_points AS (
    SELECT *
    FROM gps
    WHERE NOT tpointatsworkshop(
        (SELECT ST_Union(ST_MakeEnvelope(lon1, lat1, lon2, lat2))
         FROM workshops),
        ST_Point(longitude, latitude)
    )
),

time_windows AS (
    SELECT 
        FLOOR(timestamp/30)*30 as window_start,
        FLOOR(timestamp/30)*30 + 30 as window_end
    FROM gps
)

SELECT 
    g.timestamp,
    g.id,
    g.speed,
    g.longitude,
    g.latitude
FROM 
    gps g
    JOIN non_workshop_points nw ON g.timestamp = nw.timestamp
    JOIN time_windows tw ON g.timestamp BETWEEN tw.window_start AND tw.window_end
WHERE 
    g.speed < 0.002
GROUP BY 
    tw.window_start,
    g.timestamp,
    g.id,
    g.speed,
    g.longitude,
    g.latitude
ORDER BY 
    g.timestamp;

-- Query 8 (CEP) --------------------------------------------------------
-- Monitoring Emergency Brake Usage and Brake Pressures
-- Trains rely on the correct brake pressure to stop safely. 
-- If the pressure is out of range, there is a higher risk of mechanical damage and unsafe braking distances.
-- In parallel, frequent emergency brake use can point to hazards or driver errors. 
-- Collecting these two data points in real-time and mapping them to the train’s location, 
-- the system can detect patterns such as repeated emergency brakes in specific track segments or persistent low-pressure readings on steep inclines. 
-- When these anomalies appear, the system sends an alert to help staff take prompt action. 
-- This approach ensures safer operations and targeted maintenance in unusual brake activity areas

WITH pressure_anomalies AS (
    SELECT *
    FROM gps
    WHERE Code1 > 0 
    AND (
        PCFA_mbar < 1.9 OR PCFA_mbar > 3.0 OR
        PCFF_mbar < 1.4 OR PCFF_mbar > 2.1 OR
        PCF1_mbar < 0.8 OR PCF1_mbar > 1.3 OR
        PCF2_mbar < 1.0 OR PCF2_mbar > 1.6
    )
),

time_windows AS (
    SELECT 
        FLOOR(timestamp/0.2)*0.2 as window_start,
        FLOOR(timestamp/0.2)*0.2 + 0.2 as window_end
    FROM gps
)

SELECT 
    g.timestamp,
    g.id,
    g.speed,
    g.latitude,
    g.longitude
FROM 
    gps g
    JOIN pressure_anomalies p ON g.timestamp = p.timestamp
    JOIN time_windows tw ON g.timestamp BETWEEN tw.window_start AND tw.window_end
WHERE 
    ST_DWithin(
        ST_Point(g.longitude, g.latitude),
        ST_MakeEnvelope(/* area coordinates */),
        g.timestamp
    ) = 1
GROUP BY 
    tw.window_start,
    g.timestamp,
    g.id,
    g.speed,
    g.latitude,
    g.longitude
ORDER BY 
    g.timestamp
LIMIT 20;