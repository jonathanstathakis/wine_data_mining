/*
* label lines by iterating over top values
* to partition words by matching tops.
* then insert into pageLine, a child of page
* and make rawtext a child of pageLine through line_id.
*/
create temp table PAGELINESTAGING as (
    with recursive UNIQUE_TOPS as (
        select distinct
            PAGE_ID,
            TOP
        from RAWTEXT
        order by
            PAGE_ID,
            TOP
    ),

    -- Step 2: Recursively group tops within 4 units into bins (anchor_top)
    GROUPED_TOPS as (
    -- First top in a page becomes a new anchor
        select
            PAGE_ID,
            TOP as ANCHOR_TOP,
            TOP as CURRENT_TOP
        from UNIQUE_TOPS as UT
        where
            not exists (
                select 1
                from UNIQUE_TOPS as UT2
                where
                    UT2.PAGE_ID = UT.PAGE_ID
                    and UT2.TOP < UT.TOP
                    and UT.TOP - UT2.TOP <= 4
            )
        union all
        -- Link next top within 4 units of previous
        select
            GT.PAGE_ID,
            GT.ANCHOR_TOP,
            UT.TOP as CURRENT_TOP
        from GROUPED_TOPS as GT
        inner join UNIQUE_TOPS as UT on
            GT.PAGE_ID = UT.PAGE_ID
            and GT.CURRENT_TOP < UT.TOP
            and UT.TOP - GT.CURRENT_TOP <= 4
    ),

    -- Step 3: Create a mapping of original tops to anchor tops
    TOP_TO_ANCHOR as (
        select distinct
            PAGE_ID,
            CURRENT_TOP as TOP,
            ANCHOR_TOP
        from GROUPED_TOPS
    ),

    -- Step 4: Assign anchor_top and dense line numbers to each word
    WORDS_WITH_LINES as (
        select
            A.PAGE_ID,
            A.ID as RAWTEXT_ID,
            B.ANCHOR_TOP,
            DENSE_RANK() over (
                partition by A.PAGE_ID
                order by B.ANCHOR_TOP
            ) as LINE_NUM
        from RAWTEXT as A
        inner join TOP_TO_ANCHOR as B
            on
                A.PAGE_ID = B.PAGE_ID
                and ABS(A.TOP - B.TOP) < 0.01
        order by
            A.PAGE_ID,
            LINE_NUM,
            X0
    )

    select
        PAGE_ID,
        RAWTEXT_ID,
        ANCHOR_TOP,
        LINE_NUM
    from
        WORDS_WITH_LINES
);


insert into PAGELINE (
    PAGE_ID,
    PAGE_LINE_NUM,
    ANCHOR_TOP
)
select
    PAGE_ID,
    LINE_NUM as PAGE_LINE_NUM,
    FIRST(ANCHOR_TOP)
from
    PAGELINESTAGING
group by
    PAGE_ID, PAGE_LINE_NUM
order by
    PAGE_ID,
    PAGE_LINE_NUM;

-- update pageline line_num_tot (or abs)
update PAGELINE set
    LINE_NUM_TOT = X.LINE_NUM_TOT
from (
    select
        L.ID,
        PAGE_NUMBER,
        PAGE_LINE_NUM,
        ROW_NUMBER() over (
            order by PAGE_NUMBER, PAGE_LINE_NUM
        ) as LINE_NUM_TOT
    from PAGELINE as L
    inner join
        PAGE as P
        on
            L.PAGE_ID = P.ID
    order by
        PAGE_NUMBER,
        PAGE_LINE_NUM
) as X
where
    PAGELINE.ID = X.ID;

-- need to add line_id to text as well.

update RAWTEXT set
    LINE_ID = X.LINE_ID
from
    (
        select
            S.PAGE_ID,
            T.ID as RAWTEXT_ID,
            L.ID as LINE_ID,
            S.LINE_NUM as PAGE_LINE_NUM,
            T.TEXT
        from
            RAWTEXT as T
        inner join
            PAGELINESTAGING as S
            on
                T.ID = S.RAWTEXT_ID
        inner join
            PAGELINE as L
            on
                S.PAGE_ID = L.PAGE_ID
                and
                S.LINE_NUM = L.PAGE_LINE_NUM
        order by
            L.LINE_NUM_TOT
    ) as X
where
    X.RAWTEXT_ID = RAWTEXT.ID;
