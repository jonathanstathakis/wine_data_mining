/*
* link pub_date and pagesRaw via pub_date.id
*
*/
begin;
create temp table pagesrawstaging as select * from pagesraw;
drop table pagesraw;
create table pagesraw (
    pub_date_id int references pub_date (id) not null,
    page_num int,
    text varchar,
    x0 double,
    x1 double,
    top double,
    doctop double,
    bottom double,
    upright varchar,
    height double,
    width double,
    direction varchar,
    fontname varchar,
    size double
);
insert into pagesraw (
    pub_date_id,
    page_num,
    text,
    x0,
    x1,
    top,
    doctop,
    bottom,
    upright,
    height,
    width,
    direction,
    fontname,
    size
)
select
    c.id,
    a.*
from
    pagesrawstaging as a
cross join
    currentrundate as b
inner join
    pub_date as c
    on
        b.pub_date = c.pub_date;
-- get id for current run from pub_date then join into the insert above.
select id
from
    currentrundate as a
inner join
    pub_date as b
    on
        a.pub_date = b.pub_date;


rollback;
