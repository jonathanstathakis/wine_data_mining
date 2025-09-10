/*
* insert the aggregated text for a line as a
* string without delimiters such as commas. Useful for
* development but lack of delimiters means cant use
* directly, delimitation is implicit rather than
* explicit.
*
*/

create or replace table LINETEXTLOADING (
    LINE_ID int references PAGELINE (ID),
    LINE_TEXT varchar not null
);

insert into LINETEXTLOADING (
    LINE_ID,
    LINE_TEXT
)
select
    first(P.ID) as LINE_ID,
    string_agg(T.TEXT, ' ') as LINE_TEXT
from
    PAGELINE P
left join
    RAWTEXTLOADING T
    on
        P.ID = T.LINE_ID
group by
    P.ID
order by
    first(P.LINE_NUM_TOT);


select case when count(*) > 0 then 'ok' else error('lineText is empty') end
from LINETEXTLOADING;

insert into LINETEXT (
    LINE_ID,
    LINE_TEXT
)
select
    LINE_ID,
    LINE_TEXT
from
    LINETEXTLOADING;

drop table LINETEXTLOADING;
