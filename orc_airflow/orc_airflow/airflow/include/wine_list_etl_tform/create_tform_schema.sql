/*
run -> document -> rawText
document -> rect
document -> lines
lines -> rawText
 */

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

create sequence if not exists page_id_seq;

create table if not exists page (
id int primary key default nextval('page_id_seq'),
page_number int not null unique,
doc_id int references doc(id)
);


create sequence if not exists pageline_seq;

create table if not exists pageLine (
id int primary key default nextval('pageline_seq'),
page_id int references page(id),
anchor_top double not null,
page_line_num int not null, -- page num relative to line num
line_num_tot int, -- total line num, or abs  .
);

create sequence if not exists pagelineType_seq;
create table if not exists pageLineType (
id int primary key default nextval('pagelinetype_seq'),
line_id int references pageLine(id),
line_type varchar not null
);


create sequence if not exists rawtext_id;

create table if not exists rawText (
id int primary key default nextval('rawtext_id'),
page_id int references page(id),
line_id int references pageline(id), 
text varchar not null,
x0 double not null,
x1 double not null,
top double not null,
doctop double not null,
bottom double not null,
upright bool not null,
height double not null,
width double not null,
direction varchar not null,
fontname varchar not null,
size double not null,
);

create sequence if not exists rect_csv_seq;

create table if not exists rect_csv (
id int primary key default nextval('rect_csv_seq'),
x0 double,
y0 double, x1 double, y1 double, bottom double,
top double,
width double,
height double,
pts varchar,
linewidth double,
page_number double
);

create sequence if not exists rect_id;

create table if not exists rectangle (
id int primary key default nextval('rect_id'),
page_id int references page(id),
x0 double,
y0 double,
x1 double,
y1 double,
bottom double,
top double,
width double,
height double,
pts varchar,
linewidth double
);

create sequence if not exists linetext_seq;

create table if not exists lineText (
id int primary key default nextval('linetext_seq'),
line_id int references pageline(id),
line_text varchar not null
);


create sequence if not exists PUB_DATE_ID;

create table if not exists PUB_DATE (
    ID int primary key default nextval('pub_date_id'),
    PUB_DATE datetime unique,
    doc_id int references Doc(id)
);

