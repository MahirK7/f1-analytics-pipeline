create or replace view tyre_degradation as
select
    d.full_name,
    l.session_id,
    l.stint_id,
    l.compound,
    -- degradation rate in ms per lap (avg slope)
    (max(l.lap_time_ms) - min(l.lap_time_ms)) / nullif(max(l.lap_number) - min(l.lap_number),0) 
        as degradation_ms_per_lap,
    count(*) as laps_in_stint,
    min(l.lap_number) as stint_start_lap,
    max(l.lap_number) as stint_end_lap
from laps l
join drivers d on l.driver_id = d.driver_id
join sessions s on l.session_id = s.session_id
where s.session_type = 'Race'
  and l.lap_time_ms is not null
group by d.full_name, l.session_id, l.stint_id, l.compound;
