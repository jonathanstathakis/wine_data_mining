/*
Using the content lines identified in wineListLines backtrack to get the raw pdf word data
then start identifying the columns.

price x0 is >639.
*/


-- 06 will need work (different format)
-- 07 will need work (different formats)
-- 08,
-- 09 excluding riesling (diferent format)
-- 10
-- 11
-- 12
-- 13
-- 14
-- 15
-- 16
-- 17
-- 18
-- 19
-- 20 
-- 21 
-- 22
-- 23
-- 24 
-- 25
-- 26
-- 27 will need work (different format)

/*
* dont run on pages: 6, 7, 9 (riesling), 27
*/

/*
* we will consume extractedWords, deleting rows as we go.
*/

create temp table extractedWords as (
select 
  a.line_num_tot,
  a.page_num,
  b.line_num,
  a.merged_text,
  b.text,
  b.x0,
  b.x1,
  b.width, 
  from WINELISTLINES A
join LINE_NUMBERED_PAGES B
    on
        A.LINE_NUM = B.LINE_NUM
        and A.PAGE_NUM = B.PAGE_NUM
where 
A.PAGE_NUM not in (6,7,9,27)
OR
  A.PAGE_NUM = 9 AND subsection <> 'Riesling'
order by A.LINE_NUM, b.x0)
;

-- add price column and fill through self-join
create or replace table columnDecomp (
line_num_tot int primary key,
vintage varchar,
prod_wine_name varchar,
geo_int varchar,
vol varchar default '750',
price int
);

insert into columnDecomp (line_num_tot) select distinct line_num_tot from extractedWords order by line_num_tot;


-- vintage.
update columnDecomp a set
  vintage = text
 from
 extractedWords b
 where
 a.line_num_tot = b.line_num_tot
and
b.x1 < 105
;




-- producer_winename. Appears to start after 121.
update columnDecomp a set
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
        x0 > 105  and x1 < 369)
group by
  line_num_tot
) b
where
a.line_num_tot = b.line_num_tot;

--geo int
update columnDecomp a set
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
        x0 > 360 and x1 < 550)
group by
  line_num_tot
) b
where
a.line_num_tot = b.line_num_tot;


--volume
update columnDecomp a set
vol = b.text
from
   extractedWords  b
where
  a.line_num_tot = b.line_num_tot
and 
 (x0 > 550 and x1 < 630)
;

-- price
update columnDecomp a set
price = text.replace(',','')
   from extractedWords b 
where
  a.line_num_tot = b.line_num_tot
and
  x0 > 630;

select 
  * 
from 
columnDecomp
;


