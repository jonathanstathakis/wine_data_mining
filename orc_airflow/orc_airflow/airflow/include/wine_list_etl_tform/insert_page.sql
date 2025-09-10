/*
* get the distinct page number from page text csv and insert into
* page index table.
* */
create temp table PAGELOADING as (

    select distinct
        P.PAGE_NUMBER as PAGE_NUMBER,
        D.ID as DOC_ID
    from
        PAGE_CSV P
    cross join
        DOC D
    order by
        PAGE_NUMBER
);


insert into PAGE (
    PAGE_NUMBER,
    DOC_ID
)
select
    PAGE_NUMBER,
    DOC_ID
from
    PAGELOADING;

select * from PAGE;
