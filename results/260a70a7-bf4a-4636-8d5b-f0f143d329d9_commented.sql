```sql
-- dbt Model: staging/stg_employee_data.sql
--
-- This model loads data from the raw text file (demo.txt.txt) into a staging table.
-- It performs initial cleaning by trimming whitespace and handling null values.

{{ config(materialized='table') }}

{% set source_file = 'demo.txt.txt' %}
{% set delimiter = '|' %}

with source as (

    -- Select all columns from the raw employee data source.
    -- The source is defined in the sources.yml file.
    select * from {{ source('mysource', 'employee_raw') }}

),

renamed as (

    -- Split the single-column data into individual columns based on the pipe delimiter.
    -- Uses the dbt_utils.split_part macro to extract data based on the delimiter and position.
    -- Adjust the part_number for each column to match the data order in the source file.
    select
        {{ dbt_utils.split_part(string='_airbyte_data', delimiter=delimiter, part_number=1) }} as id,
        {{ dbt_utils.split_part(string='_airbyte_data', delimiter=delimiter, part_number=2) }} as name,
        {{ dbt_utils.split_part(string='_airbyte_data', delimiter=delimiter, part_number=3) }} as dept,
        {{ dbt_utils.split_part(string='_airbyte_data', delimiter=delimiter, part_number=4) }} as sal,
        {{ dbt_utils.split_part(string='_airbyte_data', delimiter=delimiter, part_number=5) }} as age

    from source

),

trimmed as (

    -- Trim leading and trailing whitespaces from all text fields.
    -- Removes unnecessary spaces from the data to ensure data consistency.
    -- Filter out records where ID is NULL in order to clean
    select
        trim(id) as id,
        trim(name) as name,
        trim(dept) as dept,
        trim(sal) as sal,
        trim(age) as age

    from renamed
    where id is not null

)

-- Select all columns from the cleaned and trimmed data.
select * from trimmed
```

```sql
-- dbt Model: intermediate/int_employee_unique.sql
--
-- This model removes duplicate rows from the staging data based on the 'NAME' column.
-- It uses the ROW_NUMBER() window function to identify and filter out duplicate records.

{{ config(materialized='table') }}

with stg_employee_data as (

    -- Select all columns from the staging employee data model.
    -- References the 'stg_employee_data' model created in the previous step.
    select * from {{ ref('stg_employee_data') }}

),

unique_rows as (

    -- Assign a unique row number to each row within each partition defined by the 'NAME' column.
    -- The row number is ordered by the 'id' column.
    -- This step identifies duplicate rows based on the 'NAME' column.
    select *,
           row_number() over (partition by name order by id) as row_num
    from stg_employee_data

),

final as (

    -- Select the distinct rows from the 'unique_rows' CTE where the row_num is 1.
    -- This effectively filters out the duplicate rows, keeping only the first occurrence of each name.
    select
        id,
        name,
        dept,
        sal,
        age
    from unique_rows
    where row_num = 1

)

-- Select all columns from the deduplicated data.
select * from final
```

```sql
-- dbt Model: marts/fact_employee.sql
--
-- This model transforms the unique employee data and applies data quality checks.
-- It calculates a salary range based on the 'SAL' value and adds a timestamp column.
-- Also applies not null and data-type checks before inserting into final mart table.

{{ config(materialized='table') }}

with int_employee_unique as (

    -- Select all columns from the unique employee data model.
    -- References the 'int_employee_unique' model created in the previous step.
    select * from {{ ref('int_employee_unique') }}

),

transformed as (

    -- Apply transformations to the data, including calculating the salary range and adding a timestamp.
    -- The salary range is determined based on the 'SAL' value.
    -- A date_timestamp column is added with the current timestamp.
    select
        id,
        name,
        dept,
        sal,
        age,
        CASE
            WHEN sal <= 5500 THEN 'Low'
            WHEN sal > 6500 THEN 'High'
            ELSE 'Medium'
        END as sal_range,
        CAST(CURRENT_TIMESTAMP() AS STRING) as date_timestamp -- Adjust data type for your specific database

    from int_employee_unique

),

final as (
    -- Apply data quality checks.
    select * from transformed
    where
        name is not null and
        dept is not null and
        sal is not null and
        age is not null and
        sal ~ '^[-+]?[0-9]+$' -- check SAL is numeric. Replace with the appropriate regex for your database
)

-- Select all columns from the transformed data.
select *
from final
```