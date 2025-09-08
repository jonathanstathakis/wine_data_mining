/*
* creates a denormalised table with the wine text and section headers
* for use in decomposing the pages into their component columnar fields
* on a page by page basis in downstream queries.
*/
create or replace table EXTRACTEDWORDS (
    PAGE_NUM int not null,
    LINE_NUM_TOT int not null,
    PAGE_LINE_NUM int not null,
    MERGED_TEXT varchar not null,
    TEXT varchar not null,
    X0 double not null,
    X1 double not null,
    WIDTH double not null,
    SECTION varchar not null,
    SUBSECTION varchar,
    SUBSUBSECTION varchar
);


insert into EXTRACTEDWORDS (
    PAGE_NUM,
    LINE_NUM_TOT,
    PAGE_LINE_NUM,
    MERGED_TEXT,
    TEXT,
    X0,
    X1,
    WIDTH,
    SECTION,
    SUBSECTION,
    SUBSUBSECTION
)
select
    P.PAGE_NUMBER as PAGE_NUM,
    L.LINE_NUM_TOT,
    L.PAGE_LINE_NUM as LINE_NUM,
    L.FULL_LINE_TEXT as MERGED_TEXT,
    T.TEXT,
    T.X0,
    T.X1,
    T.WIDTH,
    COALESCE(STRING_SPLIT(S.PATH, '/')[1], '') as SECTION,
    COALESCE(STRING_SPLIT(S.PATH, '/')[2], '') as SUBSECTION,
    COALESCE(STRING_SPLIT(S.PATH, '/')[3], '') as SUBSECTION
from
    PAGELINE L
left join
    PAGE P
    on
        L.PAGE_ID = P.ID
left join
    RAWTEXT T
    on
        L.ID = T.LINE_ID
left join
    SECTIONPATH S
    on
        L.SECTIONPATH_ID = S.ID
where
    L.LINE_TYPE = 'body'
    and
    LINE_NUM_TOT <> 264 -- PTO emoji line
    and merged_text.regexp_matches('\d{4}|NV')
order by
    P.PAGE_NUMBER asc,
    L.LINE_NUM_TOT asc,
    T.X0
;
select * from EXTRACTEDWORDS;
