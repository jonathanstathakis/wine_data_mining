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
    ARRAY_AGG(TO_JSON(pagelines)) as word_json
from
    pagelines
group by
    line_num,
    page_num
order by
    line_num_tot;
