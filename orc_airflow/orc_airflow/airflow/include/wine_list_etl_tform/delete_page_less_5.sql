/*
* The downstream ETL was designed without including the text from pages 1 - 5 and therefore 
* their inclusion currently breaks the pipeline. A short term solution is to delete these lines 
* after desired information is extracted and progress continues.
*/
begin;

delete from pageline l
using
    page p
where
    l.page_id = p.id
    and
    p.page_number < 6
;
-- select * from pageline l left join page p on p.id = l.page_id
-- order by line_num_tot;
delete from rawtext t
using
    page p
where
    t.page_id = p.id
    and
    p.page_number < 6
;
select *
from rawtext t
left join page p on p.id = t.page_id
left join pageline l on t.line_id = l.id
order by l.line_num_tot;

delete from page
where page_number < 6;

rollback;
