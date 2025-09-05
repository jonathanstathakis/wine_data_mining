/*
* insert parsed rectangle data into db from csv
*/

create temp table load_rect_csv as select * from read_csv('{{params.rect_csv_path }}');

create sequence if not exists rect_csv_seq;

create table if not exists rect_csv (
id int primary key default nextval('rect_csv_seq'),
x0 double,
y0 double,
x1 double,
y1 double,
bottom double,
top double,
width double,
height double,
pts varchar,
linewidth double,
page_number double
);

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
