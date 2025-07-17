# Devlog

## Wine List ETL - A Stall in Parsing the Wine List

2025-07-06 10:26:00

Have managed to parse the champagne page successfully, however the variations in patterns and lack of delimiters in the still wines has made the task too difficult, too individual. There are two options now - revisit the pdf parsing routine to be column-aware (this can be based on the difference in x0, would require a window function, similar to how we've worked on observation time smoothing in the past), or binning, etc. The creation of a column_num column labelling the columns, followed by a seperation on inter-column delimiters. The second option is to create a number of tables of wine terms - regions, GI, PGI, PDO, etc. This is a potentially more fruitful task long-term as it would provide me with a useful set of information against which to validate parsed text, search terms, etc. A source of truth. However this is a very different approach, requiring research and understsanding of the systems used country by country. Considering there appears to be no central database providing a useful data format, we may have to proceed country by country, i.e. EU, USA, Australia, New Zealand, South Africa, etc. Problematic. Another option could be to parse the GuildSomm website, as they have lists of relevant regions by country. This is a trustworthy datasource as the community verifies and validates the data. 3 options. Frankly I think option 1 would be the most rewarding atm, and will attempt to do so.

## Wine List ETL - Detecting Columns

2025-07-06 10:35:56

Each wine page in the wine list contains a number of columns. For example The champagne page (page 6) contains 5 columns - vintage, (Producer, Cuvee name), Sweetness, village, Price. Still wines contain vintage, (Producer, Cuvee Name), (Region, Country/State), Volume, Price. Unfortunately this is not ALWAYS the case, as for example, Austrian and german riesling has an additional column for their respective quality wine heirarchies - OTW for Austria, VDP for Germany. Furthermore the text in these columns often overflows into the volume column. That is not a problem if we base column designation on x0, however. Thankfully, it appears that the presence of the quality column is respected within the other sections, if un-used/overflowed into. To label these columns, we would identify islands of x0. i.e., a column seperation is represented by a _larger than 1 space_ number of pixels, a columns words represented by a run of close-by x0. Start with Aussie riesling and generalise. See how we go. Get a result in 15 mins.

## Wine List ETL - Compromising Parsing in Favor of Progress

2025-07-06 23:43:14

There's a non-zero chance that we'll be able to join the bepoz table and wine list table without 100% decomposition of the wine line. Thus in favor of haste, moving on to the fuzzy join stage of the pipeline is prudent. Once the most available fields are extracted (vintage, base vintage, cuvee name, price, disgorgement date) we can move on to the join. This will require wrapping both the bepoz pipeline and wine list pipeline in python functions and executing the pipeline in a 3rd project.

## Ingesting Bepoz/Wine List Join

2025-07-10 13:09

The flow is as follows:

wine_list, bepoz -> joined table (unverified) -> user verification -> integration into displayed wine list

so steps 1 and 2 are easy, simply create a wine_list table with a verified field.

User verification requires a UI.

Have a page that provides a table of unverified wines, displaying the bepoz and wine list text. user can manually verify the joins. This would be a POST request kind of interaction.

Integrating airflow with pythonanywhere appears to be problematic, so short term we could simply execute a dag through a django view and save scheduling for a later date.

## Wine List ETL - Done

2025-07-12 11:33

As done as anything can be. Table looks good, just need to add constraints and a primary key. Im thinking a hash on the "merged_text" field. After that we need to do the same for the bepoz ETL.

## Fuzzy Search Join

2025-07-16 09:57

The fuzzy joining of the bepoz and wine list tables has not gone as well as expected. Currently approximately 160 of the 600 or so entries have matched correctly. The strategy currently is to assemble the same field, such as vintage, country etc in the same order and leaving less easily manipulated information - such as cuvee and producer to the mercy of the distance algorithm. This has not worked. The fundamental problem is that the bepoz fields are very messy. The only true solution will be an unordered string match. There are a number of options to explore:

- full string search: some sort of complex searching algorithm tool that is standard to most RDBMS
- Python fuzzy string search packages
- a bespoke solution

Obviously option 3 is to be avoided at all costs, and the integration of Python-based logic makes the development workflow more complicated. So we'll start with option 1. See [full text search](https://duckdb.org/docs/stable/guides/sql_features/full_text_search.html).

## Fuzzy Search Join - Promising Success

2025-07-16 10:26

Ok looking good for singular cases. 2 tests have returned the correct item each time. Now how to expand to full column searches? its looking like a recursive CTE job. Need to return the top result of each search for each row of wine-list.

## Fuzzy Search Join - Adapting in Python

2025-07-17 00:43

To iterate over the tables we need to either perform a recursive CTE over one or more of the tables (nesting recursive CTEs, if its possible), Nested cross-join with recursive CTE, or embed the full-text search in a python script doing the same. As the python approach will be infinitely easier to debug, this is the approach we will take.

To do this we need:

- [x] parametrized SQL file
- [x] get wine list fields as an array / frame
- [x] function to return results from sql query
- [x] function to add results to database.
- [x] integration into dag

## Fuzzy Search Join - Failure

2025-07-17 02:05

The strings are just too different. Furthermore, the integrity of the bepoz dataset is questionable. Until the two datasets can be reconciled I will move on with uploading the wine list dataset to the webapp. Bepoz linkage can wait.

For now we'll simply run a dag which scans the current pdf and loads 'new' wines into a holding table until a user verifies their entry. A webpage can provide a view which shows the new wines.
