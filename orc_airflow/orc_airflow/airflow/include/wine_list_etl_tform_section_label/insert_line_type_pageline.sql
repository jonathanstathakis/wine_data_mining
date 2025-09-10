/*
* adds line type to pageline based on sectionLabel table.
* lines are either header or body.
*/


insert into PAGELINETYPE (
    LINE_ID,
    LINE_TYPE
)
select
    L.ID as LINE_ID,
    case when S.SECTION_TYPE is null then 'body' else 'header' end
        as LINE_TYPE
from
    PAGELINE L
left join
    SECTIONLABEL S
    on
        L.ID = S.LINE_ID
order by
    L.LINE_NUM_TOT;

select * from PAGELINETYPE;
