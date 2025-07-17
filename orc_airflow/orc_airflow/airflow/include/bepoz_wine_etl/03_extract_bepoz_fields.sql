create or replace table bp_extracted as (
select
  product_id,
  _comment
      .lower ()
      .replace
      ('sagiovese',
       'sangiovese')
      .replace ('viogner', 'viognier')
      .replace ('mouvedre', 'mourvedre')
      as comm_corr,
  regexp_extract(
        comm_corr,
        '\((\d{3,4})ml\)',
        1,
        'i'
    ).trim().nullif('')::int as ext_vol,
        regexp_extract(comm_corr, '(\d{4}|NV)', 1, 'i').nullif('') as ext_vintage,
  comm_corr
      .replace (ext_vol::varchar, '')
      .replace ('(ml)', '')
      .replace(ext_vintage,'')
      .trim () as decomp_comm
from
    bp_raw_loading);

-- extract varieties
create or replace temp table variety_ext as (
with RECURSIVE t(product_id, _comment, remaining, variety, iteration) using key (product_id) AS (
SELECT 
        product_id,
        _comment.lower() as _comment, 
        _comment.lower() as remaining,
        []::varchar[] as variety,
        1 as iteration
    from
        (select product_id, _comment from bp_raw_loading)
    union 

    select
        e.product_id,
        _comment,
        regexp_replace(remaining, v.variety_cleaned, '', 'g') as remaining,
    array_append(e.variety, v.variety_cleaned) as variety,
        iteration + 1 as iteration
    from
        t e
    join
        varieties v
    on
        POSITION (v.variety_cleaned in e.remaining) > 0
)
    select * from t);

alter table bp_extracted add column if not exists variety varchar[];
update bp_extracted as a set variety = b.variety from variety_ext as b where a.product_id = b.product_id;


-- remove values in varieties from comm_deg.
 create or replace temp table comm_deg_ex_var as (
 select
      product_id,
      variety,
     list_transform(variety, lambda v:  '(' || v || ')') as bracketed,
     array_to_string(bracketed, '|') as pattern,
     regexp_replace(decomp_comm,pattern,'','g')
        .replace(' . ','') 
        .replace('-.','')
        .replace(' .','')
        .replace('  ','')
        .regexp_replace('/\s?$','')
        .regexp_replace('(\.)\s?$','')
        .trim()
  as comm_deg_no_variety,


 from
     bp_extracted);

update bp_extracted as a
    set decomp_comm = b.comm_deg_no_variety
from
    comm_deg_ex_var as b
where
    a.product_id = b.product_id;

create or replace table bp_items (
  product_id int primary key,
  cuvee_producer varchar,
  vol int,
  vintage varchar,
  variety varchar[]
);
insert into bp_items 
select 
    product_id,
    decomp_comm as cuvee_producer,
    ext_vol as vol, 
    ext_vintage as vintage,
    variety as variety,
from
    bp_extracted;


select * from bp_items limit 10;
