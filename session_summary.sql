create or replace view session_summary as
select
    s.session_id,
    s.year,
    s.track,
    count(distinct l.driver_id) as drivers_count,
    count(*) as total_laps
from sessions s
left join laps l on s.session_id = l.session_id
where s.session_type = 'Race'
group by s.session_id, s.year, s.track;
