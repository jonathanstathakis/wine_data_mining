/*
bespoke decomp query for page 6 and page 7 champagnes.
*/

-- -- add price column and fill through self-join
create or replace table colDecoChampagne (
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
insert into colDecoChampagne (LINE_NUM_TOT)
select distinct LINE_NUM_TOT
from extractedWord
    where
        PAGE_NUM = 6
    OR
        page_num = 7 and subsection = 'Champagne'
order by LINE_NUM_TOT;


-- vintage.
-- several different forms to manage.
-- NV finishes at 92 but have to handle base year
-- vintage wine vintage ends at < 105.
-- maybe a double decomp
-- this doesnt appear to capture base year subtext at all..
-- possibly because it extends beyond 105. useful..
-- base year ends at 106. range 91 > 107
update colDecoChampagne A set
    VINTAGE = TEXT
from
    extractedWord B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT
    and
    B.X1 < 105
;


--base year
update colDecoChampagne A set
    BASE_YEAR = TEXT
from
    extractedWord B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT
    and X0 > 91 and X1 < 107
;


-- producer_winename.
update colDecoChampagne a set
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
        extractedWord
    where
        x0 > 115  and x1 < 365)
group by
  line_num_tot
) b
where
a.line_num_tot = b.line_num_tot;

--dryness
update colDecoChampagne a set
dryness = case when b.text = 'Extra' then 'Extra-Brut' else b.text END
from (
select
  line_num_tot,
  string_agg(text, ' ') as text 
from
  (
    select
        line_num_tot,
        text
    from
        extractedWord
    where
      x0 > 360
    and
      x0 < 370 
    and 
      x1 < 445
  )
group by
  line_num_tot
) b
where
a.line_num_tot = b.line_num_tot;


--disgorge date
UPDATE colDecoChampagne AS a
SET disgorge_date = b.text
FROM (
    SELECT
        line_num_tot,
        string_agg(text, ' ').replace(')','').trim() AS text
    FROM extractedWord
    WHERE x0 > 390
      AND x1 < 500
and
    regexp_matches(text, '.*\d{4}.*')
    GROUP BY line_num_tot
) AS b
WHERE a.line_num_tot = b.line_num_tot;

--geo int
update colDecoChampagne a set
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
        extractedWord
    where
        x0 > 490 and x1 < 660)
group by
  line_num_tot
) b
where
a.line_num_tot = b.line_num_tot;

-- --volume
-- update colDecoChampagne a set
-- vol = b.text
-- from
--    extractedWord  b
-- where
--   a.line_num_tot = b.line_num_tot
-- and 
--  (x0 > 550 and x1 < 630)
-- ;
--
-- price
update colDecoChampagne a set
price = text.replace(',','')
   from extractedWord b 
where
  a.line_num_tot = b.line_num_tot
and
  x0 > 630;

select * from colDecoChampagne;
