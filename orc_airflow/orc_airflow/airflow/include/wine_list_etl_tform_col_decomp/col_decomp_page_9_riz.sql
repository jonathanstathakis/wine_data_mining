/*
* bespoke query for page 9 German riesling.
*/

create or replace table colDecoPage9Riz (
    LINE_NUM_TOT int primary key,
    VINTAGE varchar default '',
    PROD_WINE_NAME varchar default '',
    GEO_INT varchar default '',
    class varchar default '',
    VOL varchar default '750',
    PRICE int,
);
insert into colDecoPage9Riz (LINE_NUM_TOT)
select distinct LINE_NUM_TOT
from extractedWord
    where
        PAGE_NUM = 9
    and SUBSECTION = 'Riesling'
order by LINE_NUM_TOT;

-- vintage.
update colDecoPage9Riz A set
    VINTAGE = TEXT
from
    extractedWord B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT
    and
    B.X1 < 105
;



 -- producer_winename. Appears to start after 121.
 update colDecoPage9Riz a set
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
         x0 > 115  
      and x1 < 365
  )
 group by
   line_num_tot
 ) b
 where
 a.line_num_tot = b.line_num_tot;

 --geo int
 update colDecoPage9Riz a set
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
         x0 > 365 and x1 < 450)
 group by
   line_num_tot
 ) b
 where
 a.line_num_tot = b.line_num_tot;

--class
update colDecoPage9Riz a set
class = b.text
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
        x0 > 450 and x1 < 600)
group by
  line_num_tot
) b
where
a.line_num_tot = b.line_num_tot;

 --volume
 update colDecoPage9Riz a set
 vol = b.text
 from
    extractedWord  b
 where
   a.line_num_tot = b.line_num_tot
 and 
  (x0 > 560 and x1 < 660)
 ;

-- price
update colDecoPage9Riz a set
price = text.replace(',','')
   from extractedWord b 
where
  a.line_num_tot = b.line_num_tot
and
  x0 > 630;

select * from colDecoPage9Riz;
