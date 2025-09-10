
create or replace sequence SECTIONPATH_SEQ;

drop table if exists sectionPathtoPageLine;
drop table if exists sectionpath cascade;

create or replace table SECTIONPATH (
    ID int primary key default nextval('SECTIONPATH_SEQ'),
    PATH varchar not null unique,
    doc_id int references doc(id)
);

create or replace sequence sectionPathtoPageLine;

create or replace table sectionPathtoPageLine (
id int primary key default nextval('sectionPathtoPageLine'),
line_id int references pageLine(id),
sectionpath_id int references sectionPath(id)
);
