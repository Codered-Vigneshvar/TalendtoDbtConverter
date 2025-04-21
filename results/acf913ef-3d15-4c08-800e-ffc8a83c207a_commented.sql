```sql
-- dbt models generated from Talend job analysis

--------------------------------------------------------------------------------
-- Model: stg_employees
-- Description: Staging model for reading raw employee data and performing initial cleaning (trimming).
-- Mirrors the tFileInputDelimited_1 component in the Talend job.
--------------------------------------------------------------------------------

{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'employee_data') }}

),

renamed as (

    select
        trim(ID) as id,
        trim(NAME) as name,
        trim(DEPT) as dept,
        SAL as sal,
        trim(AGE) as age

    from source

)

select * from renamed


--------------------------------------------------------------------------------
-- Model: int_employees_unique
-- Description: Intermediate model for removing duplicate rows based on the NAME column.
-- Mirrors the tUniqRow_1 component in the Talend job.
--------------------------------------------------------------------------------

{{ config(materialized='view') }}

with stg_employees as (

    select * from {{ ref('stg_employees') }}

),

unique_employees as (

    select
        *,
        row_number() over (partition by name order by id) as row_num
    from stg_employees

)

select * from unique_employees
where row_num = 1


--------------------------------------------------------------------------------
-- Model: employees
-- Description: Final model for performing data transformations, creating SAL_RANGE and DATE_TIMESTAMP.
-- Mirrors the tMap_1 component in the Talend job. Also includes data validation.
--------------------------------------------------------------------------------

{{ config(materialized='table') }}

with int_employees_unique as (

    select * from {{ ref('int_employees_unique') }}

),

transformed as (

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
        {{ current_timestamp() }} as date_timestamp -- Use dbt's macro for cross-DB compatibility

    from int_employees_unique

)

select * from transformed
```