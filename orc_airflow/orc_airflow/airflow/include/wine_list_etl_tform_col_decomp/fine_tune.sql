/*
* manual cleanup of certain wines who cannot be decomposed correctly.
* and general fields such as base_year etc, conversion to correct data types.
* as a form of data validation.
* */

begin;


-- base year
alter table WINES add column BASE_YEAR_ varchar;
update colDecoWines a set
  base_year_ = b.base_year
from 
    colDecoWines b
where
  a.line_num_tot = b.line_num_tot;
alter table colDecoWines drop column base_year;
alter table colDecoWines add column base_year integer;
update colDecoWines a
    set
      -- have to use case to handle empty string casting to integer null
      base_year = CASE
                      WHEN
                          b.base_year_ = '' 
                      then 
                          null
                      ELSE
                          b.base_year_.replace('(','').replace(')','').trim()::integer
                      END
                        from
                            colDecoWines b
                        where
                            a.line_num_tot = b.line_num_tot;
alter table colDecoWines drop column base_year_;

create temp view colDecoWinesWithSections
as
select
  a.*,
  b.section,
  b.subsection,
  b.subsubsection,
  b.page_num
from
  colDecoWines a
join
  wineListLines b
on
  a.line_num_tot = b.line_num_tot;

-- decompose wine_prod_name
-- will require subsection-specific logic.
-- -- primarily around red and white varietal subsection.
-- and dessert wines (they list variety)
create temp table baseWineProdName (
  line_num_tot int primary key,
  producer varchar default '',
  cuvee_name varchar default ''
);

insert into baseWineProdName (line_num_tot,
  producer,
 cuvee_name
  )
with baseCleanUp as (
select
    section,
    line_num_tot,
    prod_wine_name,
    PROD_WINE_NAME.split('‘') as split_prod,
    trim(split_prod[1]) as producer,
    trim(split_prod[2]) as cuvee_name_,
    case when cuvee_name_ is null then '' else cuvee_name_.replace('’','').trim() End as cuvee_name

from
    colDecoWinesWithSections
where
    section != 'Dessert Wine'
and
    subsection != 'White Varietals'
and
    subsection != 'Red Varietals')

select
  line_num_tot,
  producer,
  cuvee_name
from
  baseCleanUp
;

-- dessert wine
create or replace table dessertWineExtract (
line_num_tot int primary key,
producer varchar default '',
cuvee_name varchar default '',
variety varchar default '',
);

insert into dessertWineExtract (
  line_num_tot,
  producer,
  cuvee_name,
  variety
  )
  with dessertExt as (
select
  line_num_tot,
  prod_wine_name,
  prod_wine_name.split(',') as split_variety,
  split(split_variety[1]::varchar,'‘') as split_cuvee,
  split_cuvee[1]::varchar as producer_,
  split_cuvee[2]::varchar as cuvee_name_,
  split_variety[2]::varchar as variety_,
  producer_.trim() as producer,
  ifnull(cuvee_name_,'').replace('’','').trim() as cuvee_name,
  ifnull(variety_,'').trim() as  variety,

from
  colDecoWinesWithSections
where
  section = 'Dessert Wine')
select
    line_num_tot,
    producer,
    cuvee_name,
    variety
from
    dessertExt
;
            
-- white varieties
create or replace table whiteVarExtract (
line_num_tot int primary key,
producer varchar default '',
cuvee_name varchar default '',
variety varchar default '',
);

insert into whiteVarExtract (
line_num_tot,
  producer,
  cuvee_name,
  variety
)
with whiteVarExt as (
select
  line_num_tot,
  subsection,
  prod_wine_name,
  prod_wine_name.split(',') as split_variety,
  split_variety[1]::varchar as variety_,
  split(split_variety[2]::varchar,'‘') as split_producer_cuvee,
  split_producer_cuvee[1]::varchar as producer_,
  split_producer_cuvee[2]::varchar as cuvee_,
  variety_.lower().trim() as variety,
  producer_.lower().trim() as producer,
  ifnull(cuvee_,'').lower().replace('’','').trim() as cuvee_name
from
  colDecoWinesWithSections
  where
subsection = 'White Varietals'
)

select
  line_num_tot,
  producer,
  cuvee_name,
  variety
from
  whiteVarExt;
  
select * from whiteVarExtract;

-- TODO: complete extraction of red varietals and then joining.
;


rollback;
