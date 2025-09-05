/*
* load the rectangle table from rect_csv linking
* to page.
*/
insert into rectangle (
    page_id,
    run_id,
    x0,
    y0,
    x1,
    y1,
    bottom,
    top,
    width,
    height,
    pts,
    linewidth
)
select
    b.id as page_id,
    (select run_id from currrun) as run_id,
    a.x0,
    a.y0,
    a.x1,
    a.y1,
    a.bottom,
    a.top,
    a.width,
    a.height,
    a.pts,
    a.linewidth
from
    rect_csv as a
left join
    page as b
    on
        a.page_number = b.page_number;
