create or replace view PAGELINE_BTB as (
    select L.*
    from
        PAGELINE L
    left join
        PAGE P
        on
            P.ID = L.PAGE_ID
    where
        P.PAGE_NUMBER > 5
);
