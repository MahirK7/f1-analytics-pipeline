create or replace view driver_avg_by_compound as
select
    d.full_name,
    s.session_id,
    l.compound,
    avg(l.lap_time_ms) as avg_lap_time_ms
from laps l
join drivers d on l.driver_id = d.driver_id
join sessions s on l.session_id = s.session_id
where s.session_type = 'Race'
group by d.full_name, s.session_id, l.compound;
