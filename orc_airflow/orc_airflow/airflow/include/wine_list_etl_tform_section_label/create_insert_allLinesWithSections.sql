create or replace table ALLLINESWITHSECTIONS_NULL (
    LINE_ID int not null unique,
    LINE_NUM_TOT int not null unique,
    SECTION varchar,
    SUBSECTION varchar,
    SUBSUBSECTION varchar
);


insert into ALLLINESWITHSECTIONS_NULL
select
    L.ID as LINE_ID,
    L.LINE_NUM_TOT,
    S.SECTION,
    S.SUBSECTION,
    S.SUBSUBSECTION
from
    PAGELINE_BTB L
left join
    SECTIONLABELWIDE S
    on
        L.ID = S.LINE_ID
order by
    L.LINE_NUM_TOT
;
