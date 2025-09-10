create or replace table word0 (
    rawtext_id int references rawtext (id),
    line_id int references pageline (id),
);

insert into word0 (
    rawtext_id,
    line_id
)
select
    first(t.id) as rawtext_id,
    first(t.line_id) as line_id
from
    rawtext as t
inner join
    pageline as l
    on
        t.line_id = l.id
left join
    page p
    on
        l.page_id = p.id
where
    p.page_number > 5
group by
    l.line_num_tot
order by
    l.line_num_tot;
