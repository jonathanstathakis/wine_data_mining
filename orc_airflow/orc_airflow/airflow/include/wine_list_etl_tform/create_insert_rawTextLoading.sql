create or replace sequence rawtextloading_id;
create or replace table RAWTEXTLOADING (
    id int primary key default nextval('rawtextloading_id'),
    PAGE_ID int references PAGE (ID),
    line_id int,
    TEXT varchar not null,
    X0 double not null,
    X1 double not null,
    TOP double not null,
    DOCTOP double not null,
    BOTTOM double not null,
    UPRIGHT bool not null,
    HEIGHT double not null,
    WIDTH double not null,
    DIRECTION varchar not null,
    FONTNAME varchar not null,
    SIZE double not null
);


insert into RAWTEXTLOADING (
    PAGE_ID,
    TEXT,
    X0,
    X1,
    TOP,
    DOCTOP,
    BOTTOM,
    UPRIGHT,
    HEIGHT,
    WIDTH,
    DIRECTION,
    FONTNAME,
    SIZE
)
select
    B.ID as PAGE_ID,
    A.TEXT,
    A.X0,
    A.X1,
    A.TOP,
    A.DOCTOP,
    A.BOTTOM,
    A.UPRIGHT,
    A.HEIGHT,
    A.WIDTH,
    A.DIRECTION,
    A.FONTNAME,
    A.SIZE
from
    PAGE_CSV as A
left join
    PAGE as B
    on
        A.PAGE_NUMBER = B.PAGE_NUMBER;
