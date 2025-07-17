-- select * from bp_extracted limit 5;
-- select * from wine_list limit 5;
/*
* join bp and wine_list through string similarity/distance.
* TODO: remove price from wine_list join field.
* to perform the match:
* - given 2 fields, l and r
* - for each element of l iterate over all r (not already matched)
* - calculate distance for each r
* - store current r and distance (first iteration)
* - for each iteration of r, if distance is lower, replace r and distance if distance < than last
* - complete for each l.
*/


-- select cuvee_producer, vol, vintage, variety from bp_items limit 5;
--
-- describe wine_list;
-- describe bp_items;

create or replace table wine_list_inp as 
select
    pk,
    array_to_string([
    vintage,
    merged_text_ext.lower(),
    base_year::varchar,
    cuvee_name,
    disgorg_year::varchar
  ],
  ' ') as wine_list_inp
from
    wine_list
;

create or replace table bp_inp as
select
    vintage,
    cuvee_producer,
    variety,
    product_id,
    array_to_string([
    vintage,
    cuvee_producer,
    array_to_string(variety,' ')],' ') as bp_inp
from
    bp_items
;

/*
* so..
* join on vintage and distance < 20.
* need to figure out a method of joining first on fields that we have that ARE distianct such as vintage and variety
* then use the distance clause as a final requirement.
*/
-- show tables;

