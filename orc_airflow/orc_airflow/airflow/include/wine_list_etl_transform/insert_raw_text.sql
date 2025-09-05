insert into RAWTEXTSTAGING (
    RUN_ID,
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
    (select RUN_ID from CURRRUN) as RUN_ID,
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
