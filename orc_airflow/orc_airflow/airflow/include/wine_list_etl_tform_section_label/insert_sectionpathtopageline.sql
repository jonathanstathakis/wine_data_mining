insert into SECTIONPATHTOPAGELINE (
    LINE_ID,
    SECTIONPATH_ID
)
select
    SL.LINE_ID,
    S.ID
from
    SECTIONPATH S
left join
    SECTIONPATHLOADING SL
    on
        S.PATH = SL.PATH
;

-- test results

-- select
--     case
--         when count(*) > 0 then 'ok' else error('sectionPathtoPageLine is empty')
--     end
-- from
--     SECTIONPATHTOPAGELINE;
--
-- select
--     case
--         when count(*) = 0 then 'ok'
--         else
--             error('expect no null pageline.sectionpath_id')
--     end
-- from
--     SECTIONPATHTOPAGELINE
-- where
--     SECTIONPATH_ID is null;
