/*
* get the distinct page number from page text csv and insert into
* page index table.
* */

insert into page (
    page_number
)
select distinct page_number as page_number
from
    page_csv
order by
    page_number
;
