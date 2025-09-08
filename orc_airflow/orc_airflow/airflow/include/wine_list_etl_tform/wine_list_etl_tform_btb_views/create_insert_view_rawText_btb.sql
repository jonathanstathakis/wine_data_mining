create or replace view RAWTEXT_BTB as (
    select T.*
    from
        RAWTEXT T
    left join
        PAGELINE L
        on
            L.ID = T.LINE_ID
    left join
        PAGE P
        on
            P.ID = L.PAGE_ID
    where
        P.PAGE_NUMBER > 5
);
