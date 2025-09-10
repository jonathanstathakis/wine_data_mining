/*
* Get the publication date string from page 2 and process
* it into a datetime type, storing it in a table pub_date
* as pub_date.pub_date with primary key id.
* also store the current date in another table
* for the run, deleting at the end.
* */

create or replace temp table PUBDATELOADING as (
    with RAWPUBDATE as (
        select T.LINE_TEXT as RAW_DATE
        from
            PAGELINE L
        left join
            LINETEXT T
            on
                L.ID = T.LINE_ID
        left join
            PAGE P
            on
                P.ID = L.PAGE_ID

        where
            P.PAGE_NUMBER = 2
            and
            round(L.ANCHOR_TOP) = 935
    ),

    CURRENTRUNDATE as (
        select
            regexp_replace(RAW_DATE, 'st|th', '') as CLEANED_DATE,
            strptime(CLEANED_DATE, '%B %-d, %Y') as PUB_DATE
        from
            RAWPUBDATE
    )

    select PUB_DATE
    from
        CURRENTRUNDATE
);

insert into PUB_DATE (PUB_DATE, DOC_ID)
select
    P.PUB_DATE,
    D.ID as DOC_ID
from
    PUBDATELOADING P
cross join
    DOC D
;

select case when count(*) > 0 then 'ok' else error('PUB_DATE is empty') end
from PUB_DATE
;
