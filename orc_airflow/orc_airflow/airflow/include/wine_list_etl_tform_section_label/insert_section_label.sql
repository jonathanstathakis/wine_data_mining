/*
1. create word0
2. create subsection
*/
create temp table word0 as 
select
    first(t.id) as rawtext_id,
    first(t.line_id) as line_id
from
    rawText as t
inner join
    pageline as l
    on
        t.line_id = l.id
group by
l.line_num_tot
order by
    l.line_num_tot;

-- filter to lines directly above rectangles to form a map of line number to rect text line
create temp table subsection as (
select
    w.rawtext_id as word0_id,
    w.line_id as line_id,
    'subsection' as section_type
from
word0 w
inner join
rawText t
on w.rawtext_id = t.id
inner join rectangle r
on 
  r.bottom + 1 > t.bottom
and
  t.bottom > (r.bottom - t.height)
and
  r.page_id = t.page_id
);

-- -- section map
-- -- find all text greater than 200 points right and less than 100 points down.
create temp table section as (
select
    w.line_id as line_id,
    w.rawtext_id as word0_id,
    'section' as section_type,
from
    word0 as w
join
    rawText t
on
    w.rawtext_id = t.id
where
    t.x0 > 200 
and
    t.top < 100
);


create temp table subsubsection as (
select
    w.rawtext_id as word0_id,
    w.line_id as line_id,
    'subsubsection' as section_type,
from
    word0 as w
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
);

 -- exclude line_ids matching the paragraph on skin contact
 -- dirty fix.
 create temp table skin_contact_lines as (
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
 join
   rawtext t
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
   first_top);

 delete from 
     subsubsection a 
 using
     skin_contact_lines b
 where
     a.line_id = b.line_id;


create table sectionLabel as 
with sectionLabel_ as (
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
          subsubsection
  )

select
  first(l.line_num_tot) as line_num_tot,
  first(s.line_id) as line_id,
  first(s.section_type) as section_type,
  string_agg(t.text, ' ') as text
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
  first(l.line_num_tot) 
;
