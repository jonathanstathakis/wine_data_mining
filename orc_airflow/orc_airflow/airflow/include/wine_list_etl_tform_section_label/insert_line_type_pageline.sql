/*
* adds line type to pageline based on sectionLabel table.
* lines are either header or body.
*/

alter table PAGELINE add column LINE_TYPE varchar;
update PAGELINE L
set LINE_TYPE = X.LINE_TYPE
from (
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
        L.LINE_NUM_TOT
) X
where
    X.LINE_ID = L.ID
;
select
    LINE_NUM_TOT,
    FULL_LINE_TEXT,
    LINE_TYPE
from PAGELINE
order by
    LINE_NUM_TOT
;
