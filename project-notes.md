# dbt

- `qualify` a good tool to apply filters on window function at the same level

- `materialized='incremental'` uses JINJA to avoid boilerplate codes. saves lines.
```sql
-- 1. Create a work table that combines history + new batch
-- This is where the "heavy lifting" happens
CREATE TABLE analytics.maid_device_counts_NEW AS
SELECT 
    maid, 
    device_model, 
    SUM(day_count) as total_days
FROM (
    SELECT maid, device_model, day_count FROM analytics.maid_device_counts_PROD
    UNION ALL
    SELECT maid, device_model, 1 as day_count FROM staging.weekly_telemetry
)
GROUP BY 1, 2;

-- 2. Audit/Validation (Manual check or Python script check)
-- SELECT count(*) FROM analytics.maid_device_counts_NEW...

-- 3. The "Atomic" Swap (The scary part)
BEGIN;
  DROP TABLE analytics.maid_device_counts_OLD;
  ALTER TABLE analytics.maid_device_counts_PROD RENAME TO analytics.maid_device_counts_OLD;
  ALTER TABLE analytics.maid_device_counts_NEW RENAME TO analytics.maid_device_counts_PROD;
COMMIT;
```
-->
```sql
{{ config(
    materialized='incremental',
    unique_key=['maid', 'device_model'],
    incremental_strategy='delete+insert' -- Or 'merge' depending on the warehouse
) }}

WITH new_data AS (
    SELECT maid, device_model, count(distinct activity_date) as day_count
    FROM {{ ref('stg_telemetry') }}
    GROUP BY 1, 2
)

SELECT 
    n.maid, 
    n.device_model,
    -- If we are running incrementally, add new days to existing history
    {% if is_incremental() %}
        COALESCE(n.day_count, 0) + COALESCE(h.total_days, 0) as total_days
    {% else %}
        n.day_count as total_days
    {% endif %}
FROM new_data n
{% if is_incremental() %}
LEFT JOIN {{ this }} h -- {{ this }} refers to the table as it exists NOW in prod
    ON n.maid = h.maid AND n.device_model = h.device_model
{% endif %}
```

- `dbt run --select XXX` can execute `dbt run` specifically based on what have been selected. very handy.

- yml is very powerful. directing configuration and add comments
When there are multiple entries in a block. distinguish each by `- `.
There should be a handshake between end users and DE on columns name/description in models/marts/schema.sql

## dbt project structure
### analysis
- for SQL files that u dont want to expose
- data quality reports
### dbt_project.yml
- the most vital file in dbt project
- tells dbt some default setup
	project name
	profile nae
- prerequisites of dbt cli commands
### macros
- behave like func
### readme.md
project desc
installation instruction
### seeds
- a place to hold csv and flat fiels
- data in folder seeds is data that are small, static lookup tables. Such as zip mapping, country code list.
	`dbt seed`
- quick and dirty approach to extract data from external sources
### snapshots
- take a picture when u run snapshot
- useful to track the history of a column that overwrites itself
### tests
- put assertions in SQL format
- singular tests
### models
- dbt suggests 3 subfolders
#### staging
- sources (raw table from db)
- staging files are 1-1 copy of data with min cleaning
	- data types
	- renaming columns
#### intermediate
- between staging marts
- any data shouldn't be exposed to consumers
#### marts
- data ready for consumption
- the only folder u should access for dash, DS

## often used dbt commands

`dbt debug` checks whether dbt_project.yml setup.

`dbt init` builds a project folder

`dbt test` test dbt integrity

`dbt run`  whenever there r updates related to DDL changes. new table/definition/views, run this to reflect changes

`dbt docs generate` a json file in /targe
`dbt docs serve` if using dbt-core to start a local hosted db docs for the project. very verbose.

`dbt run --select TABLE_NAME --full-refresh` to DROP then CRATE TABLE_NAME.


# duckdb

duckdb v1.5.1 only supports `partition by` multiple columns iff they are of the same data type.

# general