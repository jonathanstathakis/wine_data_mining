/*
Using the content lines identified in wineListLines backtrack to get the raw pdf word data
then start identifying the columns.
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
    from WINELISTLINES as A
    inner join LINE_NUMBERED_PAGES as B
        on
            A.LINE_NUM = B.LINE_NUM
            and A.PAGE_NUM = B.PAGE_NUM
    where
        A.PAGE_NUM not in (6, 7, 9, 27)
        or
        A.PAGE_NUM = 9 and SUBSECTION <> 'Riesling'
        or
        A.PAGE_NUM = 27 and SUBSECTION <> 'Sherry, Spain'
        or
        A.PAGE_NUM = 27
        and SUBSECTION = 'Sherry, Spain'
        and SUBSUBSECTION = 'Sweet'
    order by A.LINE_NUM, B.X0
);

-- add price column and fill through self-join
create or replace table COLUMNDECOMP (
    LINE_NUM_TOT int primary key,
    VINTAGE varchar,
    PROD_WINE_NAME varchar,
    GEO_INT varchar,
    VOL varchar default '750',
    PRICE int
);

insert into COLUMNDECOMP (LINE_NUM_TOT)
select distinct LINE_NUM_TOT
from EXTRACTEDWORDS
order by LINE_NUM_TOT;


-- vintage.
update COLUMNDECOMP A set
    VINTAGE = TEXT
from
    EXTRACTEDWORDS as B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT
    and
    B.X1 < 105;


-- producer_winename. Appears to start after 121.
update COLUMNDECOMP A set
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
                X0 > 105 and X1 < 369
        )
    group by
        LINE_NUM_TOT
) as B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;

--geo int
update COLUMNDECOMP A set
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
) as B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;


--volume
update COLUMNDECOMP A set
    VOL = B.TEXT
from
    EXTRACTEDWORDS as B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT
    and
    (X0 > 550 and X1 < 630);

-- price
update COLUMNDECOMP A set
    PRICE = text.replace(',', '')
from EXTRACTEDWORDS as B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT
    and
    X0 > 630;

select *
from
    COLUMNDECOMP;
