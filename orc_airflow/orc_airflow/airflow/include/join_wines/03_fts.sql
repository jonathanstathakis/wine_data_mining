

-- 1. Create FTS index on bp_inp
PRAGMA create_fts_index('bp_inp', 'product_id', 'bp_inp', overwrite=1);

-- 2. For each row in wine_list, find best matching row in bp_inp using FTS

/*
1. find the match for each row input.
2. pack top 5 into a json
3. return
*/

/* recursive cte
with recursive_cte (column_list)
AS
(
initial query (anchor member)

UNION ALL

recursive query (recursive member)
)

select * from recursive_cte

1. execute anchor member to get result of iteration 0
2. execute recursive member with input from iteration 0
3 return a result set to the next iteration of recursive member
4. combine all results to return.

Source: <https://www.sqlservertutorial.net/sql-server-basics/sql-server-recursive-cte/>
---

in our case, we want to iterate over bp_inp and wine_list_inp.. the search
function needs the product_id column, i.e. bp_inp as input. Thus to search 
with a string on every iteration each iteration needs to be cross-joined
with the input string.

but we can build a recursive cte with that cross join where we iterate over
line_num_tot, increasing by 1 for every iteration. does that require a lateral?

yes, that works.
*/

-- base
with iterated as (
select
    b.product_id,
    b._comment,
    0 as it
from
    bp_raw_loading as b
),

cj as (
select
    b.it,
    b.product_id,
    b._comment,
    w.pk,
    w.wine_list_inp,
from
    iterated as b
cross join lateral (

  select
      pk,
      wine_list_inp
  from
      wine_list_inp
  where
      pk = (b.it + 1)
  ) as w
),

-- recursive
matched as (
select
    it,
    pk,
    _comment,
    product_id,
    wine_list_inp,
    fts_main_bp_raw_loading.match_bm25(product_id, wine_list_inp) as score
from cj
where
    score is not null
order by score desc)

select * from matched
;
