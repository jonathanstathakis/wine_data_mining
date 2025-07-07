create temp table if not exists line_numbered_pages as (
    with recursive unique_tops as (
        select distinct
            page_num,
            top
        from pagesraw
        order by
            page_num,
            top
    ),

    -- Step 2: Recursively group tops within 4 units into bins (anchor_top)
    grouped_tops as (
    -- First top in a page becomes a new anchor
        select
            page_num,
            top as anchor_top,
            top as current_top
        from unique_tops as ut
        where
            not exists (
                select 1
                from unique_tops as ut2
                where
                    ut2.page_num = ut.page_num
                    and ut2.top < ut.top
                    and ut.top - ut2.top <= 4
            )
        union all
        -- Link next top within 4 units of previous
        select
            gt.page_num,
            gt.anchor_top,
            ut.top as current_top
        from grouped_tops as gt
        inner join unique_tops as ut on
            gt.page_num = ut.page_num
            and gt.current_top < ut.top
            and ut.top - gt.current_top <= 4
    ),

    -- Step 3: Create a mapping of original tops to anchor tops
    top_to_anchor as (
        select distinct
            page_num,
            current_top as top,
            anchor_top
        from grouped_tops
    ),

    -- Step 4: Assign anchor_top and dense line numbers to each word
    words_with_lines as (
        select
            w.*,
            a.anchor_top,
            DENSE_RANK() over (
                partition by w.page_num
                order by a.anchor_top
            ) as line_num
        from pagesraw as w
        inner join top_to_anchor as a
            on
                w.page_num = a.page_num
                and ABS(w.top - a.top) < 0.01
        order by
            w.page_num,
            line_num,
            x0
    )

    select * from words_with_lines
);
