create or replace table word0 as
select
    first(t.id) as rawtext_id,
    first(t.line_id) as line_id
from
    rawtext as t
inner join
    pageline as l
    on
        t.line_id = l.id
group by
    l.line_num_tot
order by
    l.line_num_tot;
