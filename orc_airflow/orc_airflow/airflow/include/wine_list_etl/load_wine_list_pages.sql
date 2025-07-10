create table pagesraw as (
    select
        page_number as page_num,
        * exclude (page_number)
    from page_df
);

create table rect as (
    select
        page_number as page_num,
        row_number() over () as rect_num_tot,
        row_number() over (partition by page_number) as rect_num,
        x0,
        y0,
        x1,
        y1,
        width,
        height,
        pts,
        bottom,
        top,
        linewidth
    from
        rect_df
);
