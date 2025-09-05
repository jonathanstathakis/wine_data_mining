/*
* load the page text data from csv into db.
* */
-- set  variable pages_csv_path = (select path || '/' || 'pages_df.csv' from templatesearchpath);

create temp table load_page_csv as select * from read_csv('{{ params.pages_csv_path }}');

-- get curent run and insert.
create table if not exists page_csv (
id int primary key,
text varchar,
x0 varchar,
x1 double,
top double,
doctop double,
bottom double,
upright bool,
height double,
width double,
direction varchar,
fontname varchar,
size double,
page_number int
);

insert into page_csv (
id,
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
size,
page_number
)
select 
row_number() over () as id,
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
size,
page_number
  from load_page_csv
on conflict (id) do update 
  set
      text = excluded.text,
      x0 = excluded.x0,
      x1 = excluded.x1,
      top = excluded.top,
      doctop = excluded.doctop,
      bottom = excluded.bottom,
      upright = excluded.upright,
      height = excluded.height,
      width = excluded.width,
      direction = excluded.direction,
      fontname = excluded.fontname,
      size = excluded.size,
      page_number = excluded.page_number
;
