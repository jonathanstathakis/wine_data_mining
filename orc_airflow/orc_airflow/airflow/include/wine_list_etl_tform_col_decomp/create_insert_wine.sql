/*
* combine the tables formed in the columnar decompositions into 1 table.
*
*/
create or replace temp table WINE_LOADING (
    LINE_NUM_TOT int primary key,
    VINTAGE varchar default '' not null,
    BASE_YEAR varchar default '' not null,
    PROD_WINE_NAME varchar default '' not null,
    DRYNESS varchar default '' not null,
    DISGORGE_DATE varchar default '' not null,
    GEO_INT varchar default '' not null,
    CLASS varchar default '' not null,
    VOL varchar default '' not null,
    PRICE int
);

/* populate line_num_tot as per other tables */

insert into WINE_LOADING (LINE_NUM_TOT)
select distinct LINE_NUM_TOT
from extractedWord
order by LINE_NUM_TOT;

-- champagne
update WINE_LOADING A set
    VINTAGE = B.VINTAGE,
    BASE_YEAR = B.BASE_YEAR,
    PROD_WINE_NAME = B.PROD_WINE_NAME,
    DRYNESS = B.DRYNESS,
    DISGORGE_DATE = B.DISGORGE_DATE,
    GEO_INT = B.GEO_INT,
    VOL = B.VOL,
    PRICE = B.PRICE
from
    COLDECOCHAMPAGNE B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;

-- dry sherry
update WINE_LOADING A set
    PROD_WINE_NAME = B.PROD_WINE_NAME,
    GEO_INT = B.GEO_INT,
    VOL = B.VOL,
    PRICE = B.PRICE
from
    DRYSHERRY B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;

-- aus sparkling
update WINE_LOADING A set
    VINTAGE = B.VINTAGE,
    BASE_YEAR = B.BASE_YEAR,
    PROD_WINE_NAME = B.PROD_WINE_NAME,
    DISGORGE_DATE = B.DISGORGE_DATE,
    GEO_INT = B.GEO_INT,
    VOL = B.VOL,
    PRICE = B.PRICE
from
    COLDECOPAGE7AUSSPK B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;

-- german riesling
update WINE_LOADING A set
    VINTAGE = B.VINTAGE,
    PROD_WINE_NAME = B.PROD_WINE_NAME,
    GEO_INT = B.GEO_INT,
    CLASS = B.CLASS,
    VOL = B.VOL,
    PRICE = B.PRICE
from
    COLDECOPAGE9RIZ B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;

-- everything else
update WINE_LOADING A set
    VINTAGE = B.VINTAGE,
    PROD_WINE_NAME = B.PROD_WINE_NAME,
    GEO_INT = B.GEO_INT,
    VOL = B.VOL,
    PRICE = B.PRICE
from
    COLUMNDECOMP B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;

create or replace sequence wine_seq;


create or replace table WINE (
    id int primary key default nextval('wine_seq'),
    line_id int references pageLine(id),
    VINTAGE varchar default '' not null,
    BASE_YEAR varchar default '' not null,
    PROD_WINE_NAME varchar default '' not null,
    DRYNESS varchar default '' not null,
    DISGORGE_DATE varchar default '' not null,
    GEO_INT varchar default '' not null,
    CLASS varchar default '' not null,
    VOL varchar default '' not null,
    PRICE int
);

insert into WINE (
line_id,
vintage,
base_year,
prod_wine_name,
dryness,
disgorge_date,
geo_int,
class,
vol,
price
)

select
    l.id as line_id,
    c.vintage,
    c.base_year,
    c.prod_wine_name,
    c.dryness,
    c.disgorge_date,
    c.geo_int,
    c.class,
    c.vol,
    c.price
from
    WINE_LOADING c
left join
    pageLine l
on
    c.line_num_tot = l.line_num_tot
order by
    c.line_num_tot
;

select case
         when count(*) > 0 then 'ok'
         else error('Expected at least one row')
       end
from wine;
select * from extractedWord;

