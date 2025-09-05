/*
* insert parsed rectangle data into db from csv
*/

create temp table load_rect_csv as select * from read_csv('{{params.rect_csv_path }}');

insert into rect_csv(
x0,
y0,
x1,
y1,
bottom,
top,
width,
height,
pts,
linewidth,
page_number
)
select
x0,
y0,
x1,
y1,
bottom,
top,
width,
height,
pts,
linewidth,
page_number
from
    load_rect_csv;
