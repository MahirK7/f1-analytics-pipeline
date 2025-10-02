-- ============================
-- Views for F1 Analytics (session-aware, Race-only)
-- ============================

-- Drop old views if they exist
DROP VIEW IF EXISTS v_driver_avg_laptime;
DROP VIEW IF EXISTS v_driver_avg_by_compound;
DROP VIEW IF EXISTS v_stint_summary;
DROP VIEW IF EXISTS v_pitstops;
DROP VIEW IF EXISTS v_tyre_degradation;
DROP VIEW IF EXISTS v_best_laps;
DROP VIEW IF EXISTS v_avg_gap_to_leader;
DROP VIEW IF EXISTS v_session_summary;

-- 1) Average lap times by compound (per session)
CREATE OR REPLACE VIEW v_driver_avg_by_compound AS
SELECT
    s.year,
    s.track,
    s.session_type,
    d.full_name AS driver,
    l.compound,
    ROUND(AVG(l.lap_time_ms), 2) AS avg_lap_time_ms
FROM laps l
JOIN drivers d  ON l.driver_id  = d.driver_id
JOIN sessions s ON l.session_id = s.session_id
WHERE l.lap_time_ms IS NOT NULL
  AND s.session_type = 'Race'
GROUP BY s.year, s.track, s.session_type, d.full_name, l.compound;

-- 2) Average lap time per driver (per session, all tyres)
CREATE OR REPLACE VIEW v_driver_avg_laptime AS
SELECT
    s.year,
    s.track,
    s.session_type,
    d.full_name AS driver,
    d.team,
    ROUND(AVG(l.lap_time_ms), 2) AS avg_lap_time_ms
FROM laps l
JOIN drivers d  ON l.driver_id  = d.driver_id
JOIN sessions s ON l.session_id = s.session_id
WHERE l.lap_time_ms IS NOT NULL
  AND s.session_type = 'Race'
GROUP BY s.year, s.track, s.session_type, d.full_name, d.team;

-- 3) Stint summary
CREATE OR REPLACE VIEW v_stint_summary AS
SELECT
    s.year,
    s.track,
    s.session_type,
    d.full_name AS driver,
    l.stint_id,
    l.compound,
    COUNT(l.lap_number)              AS total_laps,
    ROUND(AVG(l.lap_time_ms), 2)     AS avg_lap_time_ms,
    MIN(l.lap_time_ms)               AS best_lap_time_ms,
    MAX(l.lap_time_ms)               AS worst_lap_time_ms,
    MIN(l.lap_number)                AS stint_start_lap,
    MAX(l.lap_number)                AS stint_end_lap
FROM laps l
JOIN drivers d  ON l.driver_id  = d.driver_id
JOIN sessions s ON l.session_id = s.session_id
WHERE l.stint_id IS NOT NULL
  AND l.lap_time_ms IS NOT NULL
  AND s.session_type = 'Race'
GROUP BY s.year, s.track, s.session_type, d.full_name, l.stint_id, l.compound;

-- 4) Tyre degradation
CREATE OR REPLACE VIEW v_tyre_degradation AS
SELECT
    s.year,
    s.track,
    s.session_type,
    d.full_name AS driver,
    l.compound,
    l.stint_id,
    COUNT(*)                      AS laps_in_stint,
    MIN(l.lap_number)             AS stint_start_lap,
    MAX(l.lap_number)             AS stint_end_lap,
    ROUND(AVG(l.lap_time_ms), 2)  AS avg_lap_time_ms,
    MIN(l.lap_time_ms)            AS best_lap_time_ms,
    MAX(l.lap_time_ms)            AS worst_lap_time_ms
FROM laps l
JOIN drivers d  ON l.driver_id  = d.driver_id
JOIN sessions s ON l.session_id = s.session_id
WHERE l.lap_time_ms IS NOT NULL
  AND s.session_type = 'Race'
GROUP BY s.year, s.track, s.session_type, d.full_name, l.compound, l.stint_id;

-- 5) Best lap per driver
CREATE OR REPLACE VIEW v_best_laps AS
SELECT
    s.year,
    s.track,
    s.session_type,
    d.full_name AS driver,
    bl.best_lap_time_ms,
    l.compound   AS best_lap_compound,
    l.lap_number AS best_lap_number
FROM (
    SELECT
        l.session_id,
        l.driver_id,
        MIN(l.lap_time_ms) AS best_lap_time_ms
    FROM laps l
    JOIN sessions s ON l.session_id = s.session_id
    WHERE l.lap_time_ms IS NOT NULL
      AND s.session_type = 'Race'
    GROUP BY l.session_id, l.driver_id
) bl
JOIN laps    l ON l.session_id = bl.session_id
              AND l.driver_id = bl.driver_id
              AND l.lap_time_ms = bl.best_lap_time_ms
JOIN drivers d ON d.driver_id  = bl.driver_id
JOIN sessions s ON s.session_id = bl.session_id;

-- 6) Average gap to leader
CREATE OR REPLACE VIEW v_avg_gap_to_leader AS
SELECT
    s.year,
    s.track,
    s.session_type,
    d.full_name AS driver,
    ROUND(AVG(l.gap_to_leader_ms), 2) AS avg_gap_to_leader_ms
FROM laps l
JOIN drivers d  ON l.driver_id  = d.driver_id
JOIN sessions s ON l.session_id = s.session_id
WHERE l.gap_to_leader_ms IS NOT NULL
  AND s.session_type = 'Race'
GROUP BY s.year, s.track, s.session_type, d.full_name;

-- 7) Session summary
CREATE OR REPLACE VIEW v_session_summary AS
SELECT
    s.year,
    s.track,
    s.session_type,
    d.full_name AS driver,
    COUNT(l.lap_number)          AS total_laps,
    MIN(l.lap_time_ms)           AS best_lap_time_ms,
    ROUND(AVG(l.lap_time_ms), 2) AS avg_lap_time_ms
FROM sessions s
JOIN laps l    ON s.session_id = l.session_id
JOIN drivers d ON l.driver_id  = d.driver_id
WHERE l.lap_time_ms IS NOT NULL
  AND s.session_type = 'Race'
GROUP BY s.year, s.track, s.session_type, d.full_name;

-- 8) Pit stops
CREATE OR REPLACE VIEW v_pitstops AS
WITH stint_first_lap AS (
    SELECT
        l.session_id,
        l.driver_id,
        l.stint_id,
        MIN(l.lap_number) AS pit_lap
    FROM laps l
    JOIN sessions s ON l.session_id = s.session_id
    WHERE l.stint_id IS NOT NULL
      AND s.session_type = 'Race'
    GROUP BY l.session_id, l.driver_id, l.stint_id
)
SELECT
    s.year,
    s.track,
    s.session_type,
    d.full_name AS driver,
    sfl.pit_lap AS pit_lap,
    l.compound  AS new_compound,
    sfl.stint_id
FROM stint_first_lap sfl
JOIN laps    l ON l.session_id = sfl.session_id
              AND l.driver_id  = sfl.driver_id
              AND l.stint_id   = sfl.stint_id
              AND l.lap_number = sfl.pit_lap
JOIN drivers d ON d.driver_id   = sfl.driver_id
JOIN sessions s ON s.session_id = sfl.session_id
ORDER BY s.year, s.track, d.full_name, sfl.stint_id;
