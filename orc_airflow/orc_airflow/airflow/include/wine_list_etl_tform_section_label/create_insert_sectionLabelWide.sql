create or replace table sectionLabelWide (
line_id int not null, 
line_num_tot int not null,
section varchar, 
subsection varchar,
subsubsection varchar);

insert into sectionLabelWide (
  line_id,
  line_num_tot,
  section,
  subsection,
  subsubsection
  )
select
    line_id,
    line_num_tot,
    section,
    subsection,
    subsubsection
from
(
  PIVOT sectionLabel s
  ON section_type 
  using
    first(text)
  order by line_id
)
  ;


