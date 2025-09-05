/* merge line text and add as a child of line */
begin;
create sequence if not exists mergedlinetext_seq;
create table if not exists mergedLineText (
id int primary key default nextval('mergedlinetext_seq'),
line_id int references pageLine(id),
merged_text varchar,
);

select
  *
from
  rawtext;
rollback;
