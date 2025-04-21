-- dbt model #1
-- staging/stg_employee_data.sql
{{ config(materialized='table') }}

{% set source_file = 'C:/Users/TV562EV/OneDrive - EY/Desktop/demo.txt.txt' %}
{% set field_separator = '|' %}

WITH source AS (
    {%- if execute %}
        {%- set results = run_query("SELECT * FROM read_csv('" ~ source_file ~ "', sep='" ~ field_separator ~ "', header=True)") -%}
        SELECT
            {% for col in results.columns %}
            '{{ col.name }}' AS column_name,
            {% endfor %}
        FROM {{ results }}
    {%- else %}
        SELECT
            CAST(NULL AS VARCHAR) AS ID,
            CAST(NULL AS VARCHAR) AS NAME,
            CAST(NULL AS VARCHAR) AS DEPT,
            CAST(NULL AS INTEGER) AS SAL,
            CAST(NULL AS VARCHAR) AS AGE
        LIMIT 0
    {%- endif %}
)

SELECT
    ID,
    NAME,
    DEPT,
    SAL,
    AGE
FROM source

-- dbt model #2
-- intermediate/int_unique_employee.sql
{{ config(materialized='table') }}

WITH source AS (

    SELECT * FROM {{ ref('stg_employee_data') }}

),

unique_rows AS (

    SELECT 
        ID,
        NAME,
        DEPT,
        SAL,
        AGE,
        ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY ID) as row_num
    FROM source
)

SELECT 
    ID,
    NAME,
    DEPT,
    SAL,
    AGE
FROM unique_rows
WHERE row_num = 1

-- dbt model #3
-- models/employee_salaries.sql
{{ config(materialized='table') }}

WITH source AS (

    SELECT * FROM {{ ref('int_unique_employee') }}

)

SELECT
    ID,
    NAME,
    DEPT,
    SAL,
    AGE,
    CASE
        WHEN SAL <= 5500 THEN 'Low'
        WHEN SAL > 6500 THEN 'High'
        ELSE 'Medium'
    END AS SAL_RANGE,
    CAST(CURRENT_TIMESTAMP() AS VARCHAR) AS DATE_TIMESTAMP -- Generic way to get current timestamp
FROM source

