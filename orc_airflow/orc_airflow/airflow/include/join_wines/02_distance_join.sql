
-- converting to a list is an aggregation and thus needs to be done before the join
create or replace table match_by_distance as (
with bp_inp_list as (
select 
    list(bp_inp) as bp_inp,
    -- list_transform(comm_corr, lambda x: )

from
    bp_inp),

wine_list_bp_list_joined as (
select
    a.wine_list_inp as bp_text_left,
    b.bp_inp as wine_list_list_left
from
    wine_list_inp as a
cross join
    bp_inp_list as b
),

distance_measured as (
  select
      bp_text_left,
      wine_list_list_left,
      list_transform(wine_list_list_left, lambda x: levenshtein(bp_text_left, x)) as l_distance
  from
      wine_list_bp_list_joined
),

distance_argmax as (
  select
        bp_text_left,
        l_distance[0:2],
        list_aggregate(l_distance, 'min') as dist_max,
        list_position(l_distance, dist_max) as dist_idx,
        wine_list_list_left[dist_idx] as wine_list_match_right


  from
        distance_measured
)
  select * from distance_argmax
);

select * from match_by_distance limit 5;

