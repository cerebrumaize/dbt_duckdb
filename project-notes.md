# dbt

- [dbt labs best practices](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview?version=1.12) is a good place to start.

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

- dbt cli argument `--select XXX` can execute `dbt run` specifically based on what have been selected. very handy.

- yml is very powerful. directing configuration and add comments
When there are multiple entries in a block. distinguish each by `- `.
There should be a handshake between end users and DE on columns name/description in models/marts/schema.sql
    - generic tests are defined in sources.yml
    - table name/desc/meta and columns are defined in yml
    - table DDL config can be defined in yml
    - almost everything is in yml :O

- dbt hub provides solidly tested codes.
    `dbt deps` installs packages listed in packages.yml


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

`dbt ls` lists resources in current projects

`dbt debug` checks whether dbt_project.yml setup.

`dbt init` builds a project folder

`dbt test` test dbt integrity

`dbt run`  whenever there r updates related to DDL changes. new table/definition/views, run this to reflect changes
`dbt run --select TABLE_NAME --full-refresh` to DROP then CRATE TABLE_NAME.

`dbt docs generate` a json file in /targe
`dbt docs serve` if using dbt-core to start a local hosted db docs for the project. very verbose.

`dbt test` runs scripts under tests/ and gives a report of resutls

`dbt source freshness` checks pre-written meta data or designated columns in sources.yml `loaded_at_field` with customised freshness checks

`dbt clean` clean up project structure

`dbt compile` translate JINJA, spots JINJA error quicker than `dbt build`

`dbt build` = dbt run+test+seed+snapshot+UDF
`dbt retry`

---

# duckdb

- duckdb v1.5.1 only supports `partition by` multiple columns iff they are of the same data type.

But duckdb supports columns(*), thus we can `CAST(COLUMNS(*) AS VARCHAR)` for a transpose using `PIVOT`
the below query returns len(columns)*3 btw.
```sql
WITH random_3 AS (
  SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ny_taxi.prod.green_tripdata LIMIT 3
)
UNPIVOT random_3
ON COLUMNS(*)
INTO
    NAME column_name
    VALUE column_value;
```

- `DESCRIBE SELECT * FROM {{ source('raw_data', 'green_tripdata') }};`
sees what duckdb thinks the column type. Duckdb "sniffing" and guess the datatype instead default to VARCHAR.

## local_duckdb/dbt config issues
### 1. Syntax: Literals vs. Identifiers
**Problem:** Errors when using functions like `strptime` or `ref`.
**Key Insight:**
* **Single Quotes (`'`)**: Used strictly for **string literals** (e.g., date formats, status strings).
* **Double Quotes (`"`)**: Used for **identifiers** (database, schema, or table names). 
* **dbt Context**: `{{ ref("TABLE_NAME") }}` compiles to double quotes. When using SQL functions inside a model, ensure your literals are in single quotes so DuckDB doesn't mistake them for column names.

### 2. Ingestion: External Data Mapping
**Problem:** `Catalog Error: Table does not exist` when running staging models.
**Key Insight:**
* **Option A (External Mapping):** In `models/staging/sources.yml`, assign an `external_location: 'path/to/files/*.parquet'` config. This tells dbt to read the files directly from your local disk as if they were tables.
* **Option B (Manual Build):** If not using the config above, you must pre-build the `.duckdb` file using an external script (e.g., `ingestion.py`) before running dbt.

### 3. Configuration: The "Catalog Does Not Exist" Error
**Problem:** `Binder Error: Catalog "taxi_rides_ny" does not exist`.
**Key Insight:**
* In a local DuckDB setup, DuckDB often fails to recognize a three-part name (`database.schema.table`) unless the database is explicitly attached with an alias.
* **Solution:** Remove the `database:` key from your `sources.yml`. This forces dbt to use two-part naming (`schema.table`), which DuckDB resolves correctly within the currently open file.

### 4. Architecture: Environment Separation
**Problem:** Risk of dev work overwriting production data.
**Key Insight:**
* Use separate `.duckdb` files for different environments in `profiles.yml`.
    * **Dev:** `path: taxi_rides_ny_dev.duckdb`
    * **Prod:** `path: taxi_rides_ny.duckdb`
* Switch between them using the `--target` flag: `dbt run --target dev`.

### 5. Persistence
**Problem:** Tables missing after a dbt run finishes.
**Key Insight:**
* Ensure the `path` in `profiles.yml` is a filename (e.g., `taxi_rides_ny.duckdb`) and not `:memory:`. 
* Filesystem persistence allows you to inspect the results using the DuckDB CLI independently of dbt.


---

# general