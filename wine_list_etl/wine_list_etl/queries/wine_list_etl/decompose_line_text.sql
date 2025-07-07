
create or replace table wine_list as (
  select
        * exclude merged_text,
        --- parse and cleanup prices.
        regexp_extract(merged_text, '([\d,\,]+)$', 1) as price_ext,
        -- parse and cleanup vintages
        REGEXP_EXTRACT(merged_text, '^(NV|\d{4})\b', 1) as vintage_ext,
        REGEXP_EXTRACT(merged_text, '(\(\d{2}\))', 1) as base_year_ext,
        REGEXP_EXTRACT(merged_text, '\s(‘.+’)\s', 1) as cuvee_name_ext,
        REGEXP_EXTRACT(merged_text, '(\(disg \d{4}\))', 1) as disgorg_year_ext,
        base_year_ext.replace('(','').replace(')','').trim() as base_year,
        cuvee_name_ext.trim().regexp_replace('^‘','').regexp_replace('’$','').trim() as cuvee_name,
        disgorg_year_ext.trim().replace('(disg ','').replace(')','').trim() as disgorg_year,
        price_ext.replace(',','').trim() as price,
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
