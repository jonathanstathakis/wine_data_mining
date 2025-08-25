/*
* bespoke decomp query for dry spanish sherry - no vintage column
*/

create temp table EXTRACTEDWORDS as (
    select
        A.LINE_NUM_TOT,
        A.PAGE_NUM,
        B.LINE_NUM,
        A.MERGED_TEXT,
        B.TEXT,
        B.X0,
        B.X1,
        B.WIDTH
    from WINELISTLINES A
    join LINE_NUMBERED_PAGES B
        on
            A.LINE_NUM = B.LINE_NUM
            and A.PAGE_NUM = B.PAGE_NUM
    where
        A.PAGE_NUM = 27
        and A.SUBSECTION = 'Sherry, Spain'
        and SUBSUBSECTION = 'Dry'
    order by A.LINE_NUM, B.X0
)
;

create or replace table DRYSHERRY (
    LINE_NUM_TOT int primary key,
    PROD_WINE_NAME varchar,
    GEO_INT varchar,
    VOL varchar default '750',
    PRICE int
);

insert into DRYSHERRY (LINE_NUM_TOT)
select distinct LINE_NUM_TOT
from EXTRACTEDWORDS
order by LINE_NUM_TOT;
--
-- producer_winename. 
update DRYSHERRY A set
    PROD_WINE_NAME = B.PROD_WINE_NAME
from (
    select
        LINE_NUM_TOT,
        string_agg(TEXT, ' ') as PROD_WINE_NAME
    from
        (
            select
                LINE_NUM_TOT,
                TEXT
            from
                EXTRACTEDWORDS
            where
                X1 < 369
        )
    group by
        LINE_NUM_TOT
) B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;
--
--geo int
update DRYSHERRY A set
    GEO_INT = B.GEO_INT
from (
    select
        LINE_NUM_TOT,
        string_agg(TEXT, ' ') as GEO_INT
    from
        (
            select
                LINE_NUM_TOT,
                MERGED_TEXT,
                TEXT
            from
                EXTRACTEDWORDS
            where
                X0 > 360 and X1 < 550
        )
    group by
        LINE_NUM_TOT
) B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;
--
--
--volume
update DRYSHERRY A set
    VOL = B.TEXT
from
    EXTRACTEDWORDS B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT
    and
    (X0 > 550 and X1 < 630)
;
--
-- price
update DRYSHERRY A set
    PRICE = text.replace(',', '')
from EXTRACTEDWORDS B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT
    and
    X0 > 630;
--
-- select *
from
    DRYSHERRY
;
