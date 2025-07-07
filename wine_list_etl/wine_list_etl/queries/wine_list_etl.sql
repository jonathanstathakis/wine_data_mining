begin transaction;
-- load wine list pagesRaw from source file
-- load rectangle data from source file
-- add line_num label over page number
-- aggregate text in line and store remaining word data as json

create or replace table aggregated as (
    select
        line_num,
        page_num,
        ROW_NUMBER()
            over (
                order by page_num asc, line_num asc
            )
            as line_num_tot,
        STRING_AGG(text, ' ') as merged_text,
        ARRAY_AGG(TO_JSON(line_numbered_pages)) as word_json
    from
        line_numbered_pages
    group by
        line_num,
        page_num
    order by
        line_num_tot
);


create table if not exists word_0 as (
    select
        page_num,
        line_num_tot,
        line_num,
        (word_json -> '$[0].x0')::float as x0,
        (word_json -> '$[0].fontname')::varchar as fontname,
        word_json -> '$[0].bottom' as bottom,
        word_json -> '$[0].text' as text,
        word_json -> '$[0].height' as height
    from aggregated
);

create or replace table wine_list as (
    with subsubsection as (
        select
            line_num_tot,
            page_num,
            line_num,
            'subsubsection' as line_type
        from
            word_0
        where
            fontname like '%Italic%'
        order by
            line_num_tot
    ),

    section as (
        select
            line_num_tot,
            page_num,
            line_num,
            'section' as section_type
        from
            word_0
        where
            x0 > 200
        order by
            line_num_tot
    ),

    rect_joined as (

        select
            r.page_num,
            w.page_num as w_page_num,
            w.line_num,
            r.rect_num,
            r.top as rect_top,
            r.bottom as rect_bottom,
            w.bottom as word_bottom,
            w.text,
            w.height as w_height,
            r.bottom - w.height::float as bottom_sub_height
        from
            rect as r
        left join
            word_0 as w
            on
            -- some section headers are incorreectly formatted,
            -- superimposing on the underline
            -- use a buffer of 1 to accomodate for this

                r.bottom + 1 > w.bottom
                and w.bottom > (r.bottom - w.height::float)
                and r.page_num = w.page_num
        order by
            w_page_num,
            w.line_num,
            r.rect_num
    ),

    subsection as (
        select
            a.line_num_tot,
            a.page_num,
            a.line_num,
            r.rect_num,
            a.merged_text
        from
            aggregated as a
        inner join
            rect_joined as r
            on
                a.page_num = r.page_num
                and
                a.line_num = r.line_num
        order by
            a.page_num,
            a.line_num
    ),

    line_type_labelled as (
        select
            *,
            case
                when
                    line_num_tot in (select line_num_tot from section)
                    then
                        case
                            when
                                JSON_EXTRACT_STRING(
                                    word_json, '$[0].text'
                                )::varchar
                                = '-'
                                then
                                    'page_number'
                            else
                                'section'
                        end
                when
                    line_num_tot in (select line_num_tot from subsection)
                    then
                        'subsection'
                when
                    line_num_tot in (select line_num_tot from subsubsection)
                    then
                        'subsubsection'
                else
                    'text'
            end as line_type
        from
            aggregated
        where
            line_type != 'page_number'
    ),

    /* now begins cleaning up of the text via regex.
    * we should first make 'line type' into individual columns.
    * pivot? probably very slow.
    */
    -- removal of lines otherwise irredemable or superfluous.
    text_filtered as (
        select
            line_num_tot,
            page_num,
            line_num,
            line_type,
            merged_text,
            word_json
        from
            line_type_labelled
        where
            merged_text not ilike '%continued%'
            and
            -- TODO: find a better solution for these lines.
            merged_text not like '%Skin contact aka, orange wine, is made from white grapes vinified in a similar manner to red wine.%'
            and
            merged_text not like '%contact can turn up the volume of the varietal. Adding texture, tannin and unique varietal aromatics.%'
            and
            merged_text not like '%PTO 😊%'
        order by
            line_num_tot

    )

    select * from text_filtered
);

-- assign a section, subsection and subsubsection label to each wine/line.
create temp table pivoted as (
    select
        line_num_tot,
        page_num,
        line_num,
        MAX(merged_text) filter (where line_type = 'section') as section,
        MAX(merged_text) filter (where line_type = 'subsection') as subsection,
        MAX(merged_text) filter (where line_type = 'subsubsection')
            as subsubsection,
        MAX(merged_text) filter (where line_type = 'text') as merged_text
    from
        wine_list
    group by
        line_num_tot, page_num, line_num
    order by
        line_num_tot
);

create table if not exists section_labels as (
    with section_labelled as (
        select
            line_num_tot,
            section,
            -- label each non-null row with a 1
            case when section is not null then 1 end as case_x,
            -- cumsum the non-nulls, repeating the last value on null rows
            SUM(case_x) over (
                order by line_num_tot
            ) as self_join_key
        from
            pivoted

    ),

    section_not_null as (
        select
            line_num_tot,
            section,
            case_x,
            self_join_key
        from
            section_labelled
        where
            section is not null
    ),

    section_filled as (

        select
            a.line_num_tot,
            b.section
        from
            section_labelled as a
        inner join
            section_not_null as b
            on
                a.self_join_key = b.self_join_key

        order by a.line_num_tot
    )

    select * from section_filled
);

create temp table subsection_labels as (
    with subsection_labelled as (
        select
            line_num_tot,
            subsection,
            -- label each non-null row with a 1
            case when subsection is not null then 1 end as case_x,
            -- cumsum the non-nulls, repeating the last value on null rows
            SUM(case_x) over (
                order by line_num_tot
            ) as self_join_key
        from
            pivoted

    ),

    subsection_not_null as (
        select
            line_num_tot,
            subsection,
            case_x,
            self_join_key
        from
            subsection_labelled
        where
            subsection is not null
    ),

    subsection_filled as (

        select
            a.line_num_tot,
            b.subsection
        from
            subsection_labelled as a
        inner join
            subsection_not_null as b
            on
                a.self_join_key = b.self_join_key

        order by a.line_num_tot
    )

    select * from subsection_filled
);

create table if not exists subsubsection_labels as (

    with subsubsection_labelled as (
        select
            line_num_tot,
            subsubsection,
            -- label each non-null row with a 1
            case when subsubsection is not null then 1 end as case_x,
            -- cumsum the non-nulls, repeating the last value on null rows
            SUM(case_x) over (
                order by line_num_tot
            ) as self_join_key
        from
            pivoted

    ),

    subsubsection_not_null as (
        select
            line_num_tot,
            subsubsection,
            case_x,
            self_join_key
        from
            subsubsection_labelled
        where
            subsubsection is not null
    ),

    subsubsection_filled as (

        select
            a.line_num_tot,
            b.subsubsection
        from
            subsubsection_labelled as a
        inner join
            subsubsection_not_null as b
            on
                a.self_join_key = b.self_join_key

        order by a.line_num_tot
    )

    select * from subsubsection_filled
);
-- join back to pivoted
create or replace table wine_list as (
    select
        p.line_num_tot,
        p.page_num,
        p.line_num,
        sl.section,
        ssl.subsection,
        sssl.subsubsection,
        p.merged_text,
    from
        pivoted as p
    inner join
        section_labels as sl
        on
            p.line_num_tot = sl.line_num_tot
    inner join
        subsection_labels as ssl
        on
            p.line_num_tot = ssl.line_num_tot
    inner join
        subsubsection_labels as sssl
        on
            p.line_num_tot = sssl.line_num_tot
    where
    -- drop section header text rows.
        merged_text is not null
    order by
        p.line_num_tot
);

-- cleanup.
-- TODO: break off vintages and prices at least.


drop table aggregated;
drop table line_numbered_pages;
drop table pagesraw;
drop table pivoted;
drop table rect;
drop table section_labels;
drop table subsection_labels;
drop table subsubsection_labels;
drop table word_0;

create or replace table wine_list as (
  select
        * exclude merged_text,
        --- parse and cleanup prices.
        regexp_extract(merged_text, '([\d,\,]+)$',1) as price_ext,
        -- parse and cleanup vintages
        REGEXP_EXTRACT(merged_text, '^(NV|\d{4})\b', 1) as vintage_ext,
        REGEXP_EXTRACT(merged_text, '(\(\d{2}\))', 1) as base_year_ext,
        REGEXP_EXTRACT(merged_text, '\s(‘.+’)\s', 1) as cuvee_name_ext,
        REGEXP_EXTRACT(merged_text, '(\(disg \d{4}\))', 1) as disgorg_year_ext,
        base_year_ext.replace('(','').replace(')','').trim() as base_year,
        cuvee_name_ext.trim().regexp_replace('^‘','').regexp_replace('’$','').trim() as cuvee_name,
        disgorg_year_ext.trim().replace('(disg ','').replace(')','').trim() as disgorg_year,
        price_ext.replace(',','').trim()::int as price,
        merged_text
          .replace(price_ext,'')
          .replace(vintage_ext,'')
          .replace(base_year_ext,'')
          .replace(cuvee_name_ext,'')
          .replace(disgorg_year_ext, '')
          .replace('  ',' ')
          .trim() as merged_text_ext,
    from wine_list
)
;
show tables;

commit;
--
-- /* decomposition of text field
-- * its going to have be done in sections i.e. champagne follows different rules to
-- * other seections.
-- TODO: continue this line of approach, or scrap. Works fine for Champagnes, not for still
-- due to lack of distinct delimiters.
-- */
--
-- -- with champagne as (
-- --   select
-- --     line_num_tot,
-- --     merged_text,
-- --     REGEXP_EXTRACT(merged_text, '^(NV|\d{4})\b', 1) as vintage,
-- --     REGEXP_EXTRACT(merged_text, '\((\d{2})\)', 1) as base_year,
-- --     REGEXP_EXTRACT(merged_text, '\s‘(.+)’\s', 1) as cuvee_name,
-- --     REGEXP_EXTRACT(merged_text, '\(disg (\d{4})\)', 1) as disgorg_year,
-- --     REGEXP_EXTRACT(merged_text, '([\d,\,]+)$', 1) as price,
-- --     REGEXP_EXTRACT(merged_text, '’\s(.+)\s\(', 1) as sweetness,
-- --     REGEXP_EXTRACT(merged_text, '\d{4}\)\s(.+?)\s[\d,\,]+$',1) as region,
-- --     merged_text
-- --         .replace(vintage, '')
-- --         .replace(base_year, '')
-- --         .replace(cuvee_name, '')
-- --         .replace(disgorg_year, '')
-- --         .replace(price, '')
-- --         .replace(sweetness, '')
-- --         .replace(region, '')
-- --         .replace('()','')
-- --         .replace('‘’','')
-- --         .replace('(disg )','')
-- --         .trim() as producer 
-- -- from pivot_with_section_labels
-- -- where section = 'Sparkling Wine'
-- -- )
-- -- , still_wines as (
-- -- select
-- --     merged_text,
-- --     REGEXP_EXTRACT(merged_text, '\d{4}') as vintage,
-- --     REGEXP_EXTRACT(merged_text, '\s([\d,\,]+)$', 1) as price,
-- --     REGEXP_EXTRACT(merged_text, '‘(.+)’', 1) as cuvee_name
-- -- from
-- --   pivot_with_section_labels
-- -- where section != 'Sparkling Wine'
-- -- )
-- -- select * from still_wines;
--
-- /*
-- TODO:
-- - [ ] start breaking down merged_text field.
-- - [ ] organise etl line
-- - [ ] write orchestrating python function (i.e. airflow.)
--
-- */
