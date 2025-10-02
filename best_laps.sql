create or replace view best_laps as
select
    d.full_name,
    s.session_id,
    min(l.lap_time_ms) as best_lap_ms
from laps l
join drivers d on l.driver_id = d.driver_id
join sessions s on l.session_id = s.session_id
where s.session_type = 'Race'
group by d.full_name, s.session_id;
