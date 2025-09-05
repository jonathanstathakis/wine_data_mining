create or replace temp table SUBSUBSECTIONSTAGING as
with LISTSUBSUBSECTION as (
    select distinct
        SUBSUBSECTION,
        SECTION,
        SUBSECTION,
        LINE_NUM_TOT
    from
        WINE_LIST
    order by
        LINE_NUM_TOT
)

select * from LISTSUBSUBSECTION;

insert into SUBSUBSECTION (
    SECTION, SUBSECTION, SUBSUBSECTION, SUBSUBSECTION_ORDER
)
select

    SECTION,
    SUBSECTION,
    SUBSUBSECTION,
    row_number() over (
        order by LINE_NUM_TOT
    ) as SUBSUBSECTION_ORDER
from SUBSUBSECTIONSTAGING
on conflict (SUBSUBSECTION_ID) do update
    set
        SUBSUBSECTION_ORDER = EXCLUDED.SUBSUBSECTION_ORDER;

-- rollback;
