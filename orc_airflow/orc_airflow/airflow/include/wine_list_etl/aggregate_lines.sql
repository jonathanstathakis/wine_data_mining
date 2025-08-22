create or replace table aggregated (
    line_num int,
    page_num int,
    line_num_tot int primary key,
    merged_text varchar,
    word_json json
);

insert into aggregated (
    line_num,
    page_num,
    line_num_tot,
    merged_text,
    word_json
)
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
;
