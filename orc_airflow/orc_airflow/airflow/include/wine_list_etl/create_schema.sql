/*
run -> document -> rawText
document -> rect
document -> lines
lines -> rawText
 */

create sequence if not exists runid_seq;

create table if not exists run (
    id int primary key default nextval('runid_seq'),
    start_time datetime default now(),
);

create table if not exists currRun (
id int primary key default 1,
run_id int references run(id)
);

create sequence if not exists document_id_seq;

create table if not exists document (
id int primary key default nextval('document_id_seq'),
run_id int references run(id),
doc_path varchar
);

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
page_number int,
document_id int references document(id)
);

create sequence if not exists sectionPath_seq;

create table if not exists sectionPath (
id int primary key default nextval('sectionPath_seq'),
run_id int references run(id),
path varchar 
);

create sequence if not exists pageline_seq;

create table if not exists pageLine (
id int primary key default nextval('pageline_seq'),
run_id int references run(id),
page_id int references page(id),
anchor_top double,
page_line_num int, -- page num erlative to line num
line_num_tot int, -- total line num, or abs  .
sectionpath_id int references sectionPath(id),
full_line_text varchar -- denormalised rawText, useful for dev.
);

create sequence if not exists rawtextstaging_id;

create table if not exists rawTextStaging (
id int primary key default nextval('rawtextstaging_id'),
run_id int,
page_id int,
line_id int,
text varchar,
x0 double,
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
);

create sequence if not exists rawtext_id;

create table if not exists rawText (
id int primary key default nextval('rawtext_id'),
run_id int references run(id),
page_id int references page(id),
line_id int references pageLine(id),
text varchar,
x0 double,
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
);

create sequence if not exists rect_id;

create table if not exists rectangle (
id int primary key default nextval('rect_id'),
run_id int references run(id),
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

-- create sequence if not exists sectionlabel_seq;
--
-- create table if not exists sectionlabel (
-- id int primary key default nextval('sectionlabel_seq'),
-- run_id int references run(id),
-- line_id int references pageline(id),
-- section_type varchar default '',
-- text varchar default ''
-- );

 -- create or replace table aggregated (
 --     line_num int,
 --     page_num int,
 --     line_num_tot int primary key,
 --     merged_text varchar,
 --     word_json json
 -- );
--
-- create or replace table wineListLines (
--     LINE_NUM_TOT int primary key,
--     PAGE_NUM int,
--     LINE_NUM int,
--     SECTION varchar,
--     SUBSECTION varchar,
--     SUBSUBSECTION varchar,
--     MERGED_TEXT varchar,
--     VINTAGE varchar,
--     BASE_YEAR varchar,
--     CUVEE_NAME varchar,
--     DISGORG_YEAR varchar,
--     PRICE varchar,
--     MERGED_TEXT_EXT varchar
-- );
--
