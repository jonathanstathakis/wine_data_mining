insert into SECTIONPATH (PATH, DOC_ID)
select distinct
    S.PATH,
    D.ID as DOC_ID
from
    SECTIONPATHLOADING S
cross join
    DOC D
;
