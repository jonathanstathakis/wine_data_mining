/*
* add line_id to rawTextLoading
*/
update RAWTEXTLOADING set
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
            RAWTEXTLOADING as T
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
    X.RAWTEXT_ID = RAWTEXTLOADING.ID;
--
--
select * from RAWTEXTLOADING;
