/*
* insert the aggregated text for a line as a
* string without delimiters such as commas. Useful for
* development but lack of delimiters means cant use
* directly, delimitation is implicit rather than
* explicit.
*/
-- begin;
create temp table full_line_text as (
    select
        first(p.id) as line_id,
        first(p.line_num_tot) as line_num_tot,
        string_agg(t.text, ' ') as line_text
    from
        pageline p
    left join
        rawtext t
        on
            p.id = t.line_id
    group by
        p.id
    order by
        first(p.line_num_tot)

);
update pageline l
set
    full_line_text = a.line_text
from full_line_text a
where
    a.line_id = l.id;
-- select * from pageline limit 10;
-- rollback;
