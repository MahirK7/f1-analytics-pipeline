create or replace view avg_gap_to_leader as
select
    d.full_name,
    s.session_id,
    avg(l.gap_to_leader_ms) as avg_gap_ms
from laps l
join drivers d on l.driver_id = d.driver_id
join sessions s on l.session_id = s.session_id
where s.session_type = 'Race' and l.gap_to_leader_ms is not null
group by d.full_name, s.session_id;
