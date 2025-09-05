/*
* get the distinct page number from page text csv and insert into
* page index table.
* */

insert into page (
  document_id,
  page_number
  )
with page_num as (
    select distinct page_number as page_number
    from
        page_csv
    order by
        page_number
),

document_id as (
    select a.id as document_id
    from
        document a
    join
        currrun b
        on
            a.run_id = b.run_id
)
select
    a.document_id,
    b.page_number
from
    page_num b
cross join
    document_id a;
select * from page;
