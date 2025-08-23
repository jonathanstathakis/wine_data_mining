
/*
* bespoke query for page 7 australian sparkling. v similar to champagnes but no dyness.
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
        A.PAGE_NUM = 7
    and a.SUBSECTION = 'Australia'
    order by A.LINE_NUM, B.X0
)
;
-- select * from EXTRACTEDWORDS;
-- -- add price column and fill through self-join
create or replace table COLUMNDECOMPPAGE6 (
    LINE_NUM_TOT int primary key,
    VINTAGE varchar default '',
    BASE_YEAR varchar default '',
    PROD_WINE_NAME varchar default '',
    DRYNESS varchar default '',
    DISGORGE_DATE varchar default '',
    GEO_INT varchar default '',
    VOL varchar default '750',
    PRICE int,
);
insert into COLUMNDECOMPPAGE6 (LINE_NUM_TOT)
select distinct LINE_NUM_TOT
from EXTRACTEDWORDS
order by LINE_NUM_TOT;


-- vintage.
-- several different forms to manage.
-- NV finishes at 92 but have to handle base year
-- vintage wine vintage ends at < 105.
-- maybe a double decomp
-- this doesnt appear to capture base year subtext at all..
-- possibly because it extends beyond 105. useful..
-- base year ends at 106. range 91 > 107
update COLUMNDECOMPPAGE6 A set
    VINTAGE = TEXT
from
    EXTRACTEDWORDS B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT
    and
    B.X1 < 105
;


--base year
-- base year ends at 106. range 91 > 107
update COLUMNDECOMPPAGE6 A set
    BASE_YEAR = TEXT
from
    EXTRACTEDWORDS B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT
    and X0 > 91 and X1 < 107
;


-- producer_winename. Appears to start after 121.
update columnDecompPage6 a set
prod_wine_name = b.prod_wine_name
from (
select
  line_num_tot,
  string_agg(text, ' ') as prod_wine_name 
from
  (
    select
        line_num_tot,
        text
    from
        extractedWords
    where
        x0 > 115  and x1 < 365)
group by
  line_num_tot
) b
where
a.line_num_tot = b.line_num_tot;



----dryness
--update columnDecompPage6 a set
--dryness = case when b.text = 'Extra' then 'Extra-Brut' else b.text END
--from (
--select
--  line_num_tot,
--  string_agg(text, ' ') as text 
--from
--  (
--    select
--        line_num_tot,
--        text
--    from
--        extractedWords
--    where
--      x0 > 360
--    and
--      x0 < 370 
--    and 
--      x1 < 445
--  )
--group by
--  line_num_tot
--) b
--where
--a.line_num_tot = b.line_num_tot;


--disgorge date
UPDATE columnDecompPage6 AS a
SET disgorge_date = b.text
FROM (
    SELECT
        line_num_tot,
        string_agg(text, ' ').replace(')','').trim() AS text
    FROM extractedWords
    WHERE x0 > 360
      AND x1 < 500
and
    regexp_matches(text, '.*\d{4}.*')
    GROUP BY line_num_tot
) AS b
WHERE a.line_num_tot = b.line_num_tot;

--
--geo int
update columnDecompPage6 a set
geo_int = b.geo_int
from (
select
  line_num_tot,
  string_agg(text, ' ') as geo_int
from
  (
    select
        line_num_tot,
        merged_text,
        text
    from
        extractedWords
    where
        x0 > 490 and x1 < 650)
group by
  line_num_tot
) b
where
a.line_num_tot = b.line_num_tot;

-- select *
-- from
--     EXTRACTEDWORDS
-- where
-- x0 > 490
-- ;

-- --volume
-- update columnDecompPage6 a set
-- vol = b.text
-- from
--    extractedWords  b
-- where
--   a.line_num_tot = b.line_num_tot
-- and 
--  (x0 > 550 and x1 < 630)
-- ;
--
-- price
update columnDecompPage6 a set
price = text.replace(',','')
   from extractedWords b 
where
  a.line_num_tot = b.line_num_tot
and
  x0 > 630;

select * from COLUMNDECOMPPAGE6;
