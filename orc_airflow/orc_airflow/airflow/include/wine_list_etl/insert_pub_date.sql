/*
* Get the publication date string from page 2 and process
* it into a datetime type, storing it in a table pub_date
* as pub_date.pub_date with primary key id.
* also store the current date in another table
* for the run, deleting at the end.
* */
create sequence if not exists PUB_DATE_ID;
create table if not exists PUB_DATE (
    ID int primary key default nextval('pub_date_id'),
    PUB_DATE datetime unique
);

create table if not exists CURRENTRUNDATE (
    ID int primary key default 1,
    RAW_DATE varchar,
    CLEANED_DATE varchar,
    PUB_DATE datetime
);

create temp table RAWPUBDATE as (
    select * from PAGESRAW
    where
        PAGE_NUM = 2
        and
        TOP > 900
        and
        TOP < 950
);

insert into CURRENTRUNDATE (ID, RAW_DATE, CLEANED_DATE, PUB_DATE)
select
    1 as ID,
    string_agg(TEXT, ' ') as RAW_DATE,
    regexp_replace(RAW_DATE, 'st|th', '') as CLEANED_DATE,
    strptime(CLEANED_DATE, '%B %-d, %Y') as PUB_DATE
from
    RAWPUBDATE
group by
    TOP
on conflict do update
    set
        RAW_DATE = EXCLUDED.RAW_DATE,
        CLEANED_DATE = EXCLUDED.CLEANED_DATE,
        PUB_DATE = EXCLUDED.PUB_DATE;

insert into PUB_DATE (PUB_DATE)
select PUB_DATE
from
    CURRENTRUNDATE
on conflict (PUB_DATE) do update
    set
        PUB_DATE = EXCLUDED.PUB_DATE;

select * from PUB_DATE;
select * from CURRENTRUNDATE;
