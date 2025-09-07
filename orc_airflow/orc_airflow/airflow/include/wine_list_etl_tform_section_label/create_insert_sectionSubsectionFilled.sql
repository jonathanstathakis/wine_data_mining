/*
* creates an interm table with section and subsection filled for 
* each line.
*/
create or replace table sectionSubsectionFilled (
line_id int not null unique,
line_num_tot int not null unique,
section varchar,
subsection varchar
);

insert into sectionSubsectionFilled
    with section as (
      select line_id,
        section,
        sum(
          case
            when section is not null then 1
            else 0
          end
        ) over (
          order by line_num_tot
        ) as section_self_join_key,
        from allLinesWithSections_Null
      where section is not null
    ),

    subsection as (
      select line_id,
        subsection,
        sum(
          case
            when subsection is not null then 1
            else 0
          end
        ) over (
          order by line_num_tot
        ) as subsection_self_join_key,
        from allLinesWithSections_Null
      where subsection is not null
    ),

    with_join_key as (
      select line_id,
        line_num_tot,
        -- as self_join_key,
        section,
        subsection,
        subsubsection,
        sum(
          case
            when section is not null then 1
            else 0
          end
        ) over (
          order by line_num_tot
        ) as section_self_join_key,
        sum(
          case
            when subsection is not null then 1
            else 0
          end
        ) over (
          order by line_num_tot
        ) as subsection_self_join_key,
        from allLinesWithSections_Null
    ),

    sectionAndSubsectionJoined as (
      select a.line_id,
        line_num_tot,
        b.section,
        c.subsection
      from with_join_key a
        left join section b on a.section_self_join_key = b.section_self_join_key
        left join subsection c on a.subsection_self_join_key = c.subsection_self_join_key
    )

    select *
    from sectionAndSubsectionJoined
;
