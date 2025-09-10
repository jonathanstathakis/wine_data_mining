/* Using the content lines identified in wineListLines backtrack to get the raw pdf word data
then start identifying the columns.
*/


-- add price column and fill through self-join
create or replace table COLUMNDECOMP (
    LINE_NUM_TOT int primary key,
    VINTAGE varchar,
    PROD_WINE_NAME varchar,
    GEO_INT varchar,
    VOL varchar default '750',
    PRICE int,
    merged_text varchar
);

insert into COLUMNDECOMP (LINE_NUM_TOT)
select distinct LINE_NUM_TOT
from extractedWord
where
        PAGE_NUM not in (6, 7, 9, 27)
        or PAGE_NUM > 5 -- restrict to bottles.
        or
        PAGE_NUM = 9 and SUBSECTION <> 'Riesling'
        or
        PAGE_NUM = 27 and SUBSECTION <> 'Sherry, Spain'
        or
        PAGE_NUM = 27
        and SUBSECTION = 'Sherry, Spain'
        and SUBSUBSECTION = 'Sweet'
order by LINE_NUM_TOT;

update COLUMNDECOMP A
set merged_text = ltxt.line_text
from
    pageLine l
left join
    lineText ltxt
on
    l.id = ltxt.line_id
where
    a.line_num_tot = l.line_num_tot;
      

-- vintage.
update COLUMNDECOMP A set
    VINTAGE = TEXT
from
    extractedWord as B
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
                 extractedWord
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
                extractedWord
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
     extractedWord as B
 where
     A.LINE_NUM_TOT = B.LINE_NUM_TOT
     and
     (X0 > 550 and X1 < 630);

 -- price
 update COLUMNDECOMP A set
     PRICE = text.replace(',', '')
 from extractedWord as B
 where
     A.LINE_NUM_TOT = B.LINE_NUM_TOT
     and
     X0 > 630;

select *
from
    COLUMNDECOMP;
