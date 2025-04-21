-- dbt model #1
{{ config(materialized='view') }}

with source_data as (
    select
        ID as employee_id,
        NAME as employee_name,
        DEPT as department,
        SAL as salary,
        AGE as age
    from {{ source('raw', 'employee_data') }}
    /*
    Assumptions:
    - I'm assuming you've configured a `source` named 'raw' and a table named 'employee_data'
      in your `dbt_project.yml` file to point to the raw file.

    Example `dbt_project.yml` configuration:

    sources:
      - name: raw
        database: your_database_name  # Replace with your database name
        schema: your_staging_schema # Replace with the schema where the file is loaded
        tables:
          - name: employee_data #Replace with the table name where the file is loaded.
            identifier: 'demo.txt'  # Replace with the actual file name
            loaded_at_field: _etl_loaded_at #Use whatever column represents when the file was loaded
    */
)

select *
from source_data

-- dbt model #2
{{ config(materialized='view') }}

with stg_employee_data as (
    select * from {{ ref('stg_employee_data') }}
),

unique_employees as (
    select
        *,
        row_number() over (partition by employee_name order by employee_id) as row_num
    from stg_employee_data
)

select *
from unique_employees
where row_num = 1

-- dbt model #3
{{ config(materialized='view') }}

with unique_employees as (
    select * from {{ ref('int_unique_employee_data') }}
)

select
    *,
    CASE
        WHEN salary <= 5500 THEN 'Low'
        WHEN salary > 6500 THEN 'High'
        ELSE 'Medium'
    END as salary_range,
    CAST(current_timestamp() as STRING) as date_timestamp -- Using current_timestamp function
from unique_employees

-- dbt model #4
{{ config(materialized='view') }}

with transformed_data as (
    select * from {{ ref('int_transformed_employee_data') }}
),

validated_data as (
    select
        employee_id,
        employee_name,
        department,
        salary,
        age,
        salary_range,
        date_timestamp
    from transformed_data
    where employee_name is not null
      and department is not null
      and salary is not null
      and age is not null
      and salary_range is not null
      and date_timestamp is not null

      --Data Type Validation - Optional
      and try_cast(employee_id as STRING) is not null
      and try_cast(employee_name as STRING) is not null
      and try_cast(department as STRING) is not null
      and try_cast(salary as INT) is not null
      and try_cast(age as STRING) is not null
      and try_cast(salary_range as STRING) is not null
      and try_cast(date_timestamp as STRING) is not null

)

select *
from validated_data

-- dbt model #5
{{ config(materialized='table') }}

select *
from {{ ref('int_schema_validated_employee_data') }}

-- dbt model #6
-- intermediate/int_duplicate_employee_data.sql
{{ config(materialized='view') }}

with stg_employee_data as (
    select * from {{ ref('stg_employee_data') }}
),

unique_employees as (
    select
        *,
        row_number() over (partition by employee_name order by employee_id) as row_num
    from stg_employee_data
),

duplicate_employees as (
  select *
  from unique_employees
  where row_num > 1
)

select *
from duplicate_employees

-- dbt model #7
-- marts/fact_duplicate_employees.sql
{{ config(materialized='table') }}

select *
from {{ ref('int_duplicate_employee_data') }}

