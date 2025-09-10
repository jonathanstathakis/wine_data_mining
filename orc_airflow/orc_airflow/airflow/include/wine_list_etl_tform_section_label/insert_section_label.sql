/*
creates a tabel sectionLabel with the section headers from pages 6 and onwards,
i.e. by the bottle.
*/

create or replace table sectionLabel (
line_id int references pageline(id),
section_type varchar not null,
);
-- filter to lines directly above rectangles to form a map of line number to rect text line
create or replace table sectionLabelLoading as (
with subsection as (
    select
        w.rawtext_id as word0_id,
        w.line_id as line_id,
        'subsection' as section_type
    from
        word0 w
    inner join
        rawText t
    on
        w.rawtext_id = t.id
    inner join 
        rectangle r
    on 
        r.bottom + 1 > t.bottom
    and
        t.bottom > (r.bottom - t.height)
    and
        r.page_id = t.page_id
    ),

-- -- section map
-- -- find all text greater than 200 points right and less than 100 points down.
section as (
    select
        w.line_id as line_id,
        w.rawtext_id as word0_id,
        'section' as section_type
    from
        word0 as w
    left join
        rawText t
    on
        w.rawtext_id = t.id
    where
        t.x0 > 200 
    and
        t.top < 100
    ),

 subsubsection as (
    select
        w.rawtext_id as word0_id,
        w.line_id as line_id,
        'subsubsection' as section_type, from word0 as w
    join 
    rawText t
    on
    w.rawtext_id = t.id
    where
        'Italic' in t.fontname
    and w.line_id not in (
        select 
          line_id
        from
          subsection
        )
    ),

 -- exclude line_ids matching the paragraph on skin contact
 -- dirty fix.
 skin_contact_lines as (
      select
        first(s.word0_id) as word0_id,
        first(s.line_id) as line_id,
        first(s.section_type) as section_type,
        first(t.x0) as first_x0,
        last(t.x0) as last_x0,
        first(t.top) as first_top,
        last(t.top) as last_top,
        string_agg(t.text, ' ') as text
      from
        subsubsection s
      left join
        rawText t
      on
        s.line_id = t.line_id
      group by
        s.line_id
      having
        text like '%Skin contact aka, orange wine, is made from white grapes vinified in a similar manner to red wine. Skin%'
        or text like '%contact can turn up the volume of the varietal. Adding texture, tannin and unique varietal aromatics.%'
      order by
        section_type,
        line_id,
        first_x0,
        first_top),

subsubsection_without_skin_contact as (
    select
        a.word0_id,
        a.line_id,
        a.section_type
    from
        subsubsection a
    left join
        skin_contact_lines b
    on
        a.line_id = b.line_id
    where b.line_id IS NULL
    ),

sectionLabel_ as (
    select
        word0_id,
        line_id,
        section_type
    from
        section
    union
        select
          word0_id,
          line_id,
          section_type
        from
          subsection
    union
        select
          word0_id,
          line_id,
          section_type
        from
          subsubsection_without_skin_contact
  ),

result as (
select
  first(s.line_id) as line_id,
  first(s.section_type) as section_type,
  string_agg(t.text, ' ' ORDER BY t.x0) as text
from
  sectionLabel_ s
left join
  rawText t
on
  s.line_id = t.line_id
left join
  pageLine l
on
  s.line_id = l.id
group by
  s.section_type,
  s.line_id 
having
  text not ilike '%continued%' -- page-based repeat headers.
order by
  first(l.line_num_tot),
  first(t.x0) asc
)
select 
    line_id,
    section_type,
    text
  from result 
);


create or replace table sectionLabel as
select 
    line_id,
    section_type,
from
  sectionLabelLoading;

select * from sectionLabel;
