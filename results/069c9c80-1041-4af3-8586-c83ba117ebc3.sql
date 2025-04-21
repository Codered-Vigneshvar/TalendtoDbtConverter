-- dbt model #1
{{ config(materialized='table') }}

{% set source_file = 'demo.txt.txt' %}
{% set delimiter = '|' %}

with source as (

    select * from {{ source('mysource', 'employee_raw') }}

),

renamed as (

    select
        {{ dbt_utils.split_part(string='_airbyte_data', delimiter=delimiter, part_number=1) }} as id,
        {{ dbt_utils.split_part(string='_airbyte_data', delimiter=delimiter, part_number=2) }} as name,
        {{ dbt_utils.split_part(string='_airbyte_data', delimiter=delimiter, part_number=3) }} as dept,
        {{ dbt_utils.split_part(string='_airbyte_data', delimiter=delimiter, part_number=4) }} as sal,
        {{ dbt_utils.split_part(string='_airbyte_data', delimiter=delimiter, part_number=5) }} as age

    from source

),

trimmed as (

    select
        trim(id) as id,
        trim(name) as name,
        trim(dept) as dept,
        trim(sal) as sal,
        trim(age) as age

    from renamed
    where id is not null

)

select * from trimmed

-- dbt model #2
*   `dbt_utils.split_part`: This macro is used to split the data from a single column, after it has been loaded from the source file. It splits by the pipe delimiter into several fields.
*   `trimmed`: Applies the `TRIM` function to all the text fields to remove leading and trailing whitespaces as defined in the Talend job.

**2. `intermediate/int_employee_unique.sql`**

-- dbt model #3
**Explanation:**

*   This model references the `stg_employee_data` model.
*   It uses the `row_number()` function to assign a unique rank to each row partitioned by `name`, effectively identifying duplicates based on the `NAME` field.
*   Finally, it selects only the first occurrence of each `NAME` to produce a table with only unique values.

**3. `marts/fact_employee.sql`**

