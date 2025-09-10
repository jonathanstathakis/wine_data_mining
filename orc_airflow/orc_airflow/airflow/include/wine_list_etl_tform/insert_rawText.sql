/* populate rawText */

insert into RAWTEXT (
    PAGE_ID,
    LINE_ID,
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
    PAGE_ID,
    LINE_ID,
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
from
    RAWTEXTLOADING;

select case when count(*) > 0 then 'ok' else error('rawText is empty') end
from RAWTEXT
;
select * from RAWTEXT;
