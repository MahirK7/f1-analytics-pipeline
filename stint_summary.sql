create or replace view stint_summary as
select
    d.full_name,
    s.session_id,
    l.stint_id,
    l.compound,
    count(*) as laps_in_stint,
    avg(l.lap_time_ms) as avg_stint_lap_ms
from laps l
join drivers d on l.driver_id = d.driver_id
join sessions s on l.session_id = s.session_id
where s.session_type = 'Race'
group by d.full_name, s.session_id, l.stint_id, l.compound;
