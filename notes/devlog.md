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

## Uploading Wines - Planning

2025-07-17 11:01

To upload the wines extracted by wine list we can get them as a csv and upload that to python anywhere then run a migration. Im expecting to be able to adapt this pretty simply. Question is how to use the ORM. We can use the wine_wiki_site test project to experiment. Going to need a date-added field to enable removal during testing but also useful. 'date-added-to-website' to be clear.

TODO:

- [ ] add wine.csv to test project
- [ ] experiment with shell
- [ ] write data migration script
- [ ] test

Im expecting this to require a little translation and configuration on both ends, especially fields that are missing from either.

## Adding Columnwise Partitioning

2025-08-22 13:50

The original approach ignored the presence of columns in the wine list, extracting each horizontal line as continuous. The expectation was that the data would be structured enough to match by similarity to other sources. This proved to be a false assumption, and as such are now looking to preserve the structure and information. To that end I have created a branch and will look to adapt or refactor the ETL pipeline to partition the lines by the presence of columns. It should be quite simple, working from the right to the left we will identify where the column starts (as the strings are left-aligned) and label thusly. The first step is to see where we can action this.

## Adding Columnwise Partitioning - Figuring out an Insertion Point

2025-08-22 14:46

A lesson I learned long ago is to never, ever EVER modify old code, especially stale old code. Work with what you have, final. Optimisations are never worth it. Unless they are..

Anyway, on that topic, the result of the ETL as it stands is a denormalised, duplicated mess with a ton of manually created \*Ref tables used in the fine grained extraction, a wine list staging table, text decomposition tables, and more. That's fine though. The problem is that somewhere along the way we deleted the pdf data such as x and y coordinates. The simplest solution will be to modify that deletion, instead storing in a table. I believe the reason for this was difficulty in identifying an appropriate primary key considering that the section lines are turned into fields. Should have just kept the natural primary key from the line ordering and be damned with monotonicity.

## Refactoring Dags - Complete

2025-09-06 09:18

I don't know where my last entries are, it may be the ones prior in this document, but I don't recognise them. Anyway. After foolishly breaking the wine_list_etl dag I have painstakingly reconstructed everything and re-interfaced it with the columnwise decomposition dag. lessons learnt:

- You CANNOT use a relational model in an ETL with evolving schemas. It is gross and pointless. At least you can, but give up on DRY.
- You CANNOT avoid polluting a database with intermediate tables that should by all rights be temporary as the alternative is OBF (One Big File) which is gross.
- Atomised DAGs and Tasks is key. One SQL statement per task. One complete action per DAG.
- Don't use DuckDB if you can avoid it (?)
- Python is probably better for ETLs. State, debugging, object inspection. All infinitely better in Python.

Anyway, its done now.

TODO:

- [ ] extract information from headers - country, region, variety, etc.

## Next Steps

2025-09-07 10:12

I have planned out the next phase - induction into wine app database. After completing the load phase of the ETL pipeline, which simply adds document publication date, filepath and runtime to the table of wines, we will proceed with the construction of a form to accept and verify the result of the ETL. Specifically, the data will be input into a wine_list Model with fields reflecting those of the wine list. Another model will track which publication date/run is current. The induction process will automatically match existing lines to the previous list, leaving us with a selection of wines that are not matched. These not matched items will be used to ask the user to either match manually or create a new wine item from the input fields. We will begin by completing the load phase, manually outputting a csv that is loaded into the wine app, build the wine_list model and currentList tracker, build the linkage tool and finally display the current wine list. Further functionality can be built out from this but this is the core structure.

The problem currently is that I am designing a process for the n_0+1 iteration of ingestion, but I have no method of linking n_0 wine list. So we will need to deal with that first. What is the method? To avoid manually linking every wine we need to construct a method of searching. Back to fuzzy joins.

A method of fuzzy join is to use levenstein distance for each join field and join where above threshold. You can give more weighting to a field by increasing the required distance score. Ok. So we need to make sure that the wine database wines have the same fields as the wine_list entri That may require some finangling.

Finally we want to implement a REST API on the webapp to enable automatic upload of ETL results to staging table.

TODO:

- [ ] complete Load phase:
  - [x] load results of column decomp DAG to a table 'wine_list_wine'
  - [x] add section_path_id to wine_list_wine
  - [x] add publication date to wine_list_wine
  - [x] add run_datetime to wine_list_wine
  - [x] identify if any other FK need to be added to wine_list_wine
  - [x] create output csv query that joins wine_list_wine with all FK tables and outputs values to a csv file name is input filename, run date
  - [x] add output to csv Task.
  - [ ] re-establish relationships where possible.
- [ ] n_0 wine_list to wine join:
  - [ ] download prod database
  - [ ] create a interface table from wine that mimics the fields of wine_list
  - [ ] define a fuzzy join from the join table to the interface table
  - [ ] perfect.
- [ ] n +1 ingestion process:
  - [ ] load wine_list from csv file
  - [ ] create wine_list_staging model instances from csv file data
  - [ ] join wine_list_staging to n wine_list updating wine_id to link already linked rows to wine.
  - [ ] build form to display unlinked wines for user linkage:
    - [ ] create view
    - [ ] create url
    - [ ] create form:
      - [ ] table output
      - [ ] add wine field columns
      - [ ] manual search column: add column to enable search
      - [ ] add new wine from input wine list button
      - [ ] manual entry button to create new wine
- [ ] test
- [ ] REST API:
  - [ ] create API
  - [ ] add upload task to ETL.

## Adding Publication Date

2025-09-08 11:46

Adding publication date has not been easy. In fact, its revealed a glaring hole in the ETL design methodology, namely dependency management. Each of the DAGs has both a dependency on upstream DAGs, that is natural. However they do need some form of encapsulation. The problem really lies in how unreadable SQL actually is. Without the ability to encapsulate, environmental observation is time dependent. What has already been run, etc. You cannot drop in a breakpoint and observe what a line is doing, and what is available to it.

So we're going to need to fix this. The main solution I can see is to not refer to the store tables such as rawText, pageLine, page etc. throughout an ETL but instead create a temporary table for the process that acts as a bridge. That way you can modify the bridge to propagate filters through to the downstream statements. This is a form of encapsulation.

We will start with section_label. Separate dependency on the core tables.

Section label is done, have wrapped section_path back into section_label for management simplicity. Next is col decomp and load. Same deal, ensure that queries are on the \_btb views not the core store and convert to using some sort of bridging table for ease of later modification.

coldecomp is done, it all goes through extractedWords.

Got page numbers and headers in wine_list_wine.

## Task Groups

2025-09-10 09:59

Turns out my multi-DAG approach was a bad practice, and that utilising several task groups within a DAG is the expected approach. Will need to refactor at a later date.
