/*
* bespoke query for page 7 australian sparkling. v similar to champagnes but no dyness.
*/

create or replace table colDecoPage7AusSpk (
    LINE_NUM_TOT int primary key,
    VINTAGE varchar default '',
    BASE_YEAR varchar default '',
    PROD_WINE_NAME varchar default '',
    DISGORGE_DATE varchar default '',
    GEO_INT varchar default '',
    VOL varchar default '750',
    PRICE int,
);
insert into colDecoPage7AusSpk (LINE_NUM_TOT)
select distinct LINE_NUM_TOT
from extractedWord
    where
        PAGE_NUM = 7
    and SUBSECTION = 'Australia'
order by LINE_NUM_TOT;


-- vintage.
update colDecoPage7AusSpk A set
    VINTAGE = TEXT
from
    extractedWord B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT
    and
    B.X1 < 105
;


--base year
update colDecoPage7AusSpk A set
    BASE_YEAR = TEXT
from
    extractedWord B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT
    and X0 > 91 and X1 < 107
;


update colDecoPage7AusSpk a set
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

----dryness
--update colDecoPage7AusSpk a set
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
--        extractedWord
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
UPDATE colDecoPage7AusSpk AS a
SET disgorge_date = b.text
FROM (
    SELECT
        line_num_tot,
        string_agg(text, ' ').replace(')','').trim() AS text
    FROM extractedWord
    WHERE x0 > 360
      AND x1 < 500
and
    regexp_matches(text, '.*\d{4}.*')
    GROUP BY line_num_tot
) AS b
WHERE a.line_num_tot = b.line_num_tot;

--
--geo int
update colDecoPage7AusSpk a set
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
        x0 > 490 and x1 < 650)
group by
  line_num_tot
) b
where
a.line_num_tot = b.line_num_tot;

-- --volume
-- update colDecoPage7AusSpk a set
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
update colDecoPage7AusSpk a set
price = text.replace(',','')
   from extractedWord b 
where
  a.line_num_tot = b.line_num_tot
and
  x0 > 630;

select * from colDecoPage7AusSpk;
