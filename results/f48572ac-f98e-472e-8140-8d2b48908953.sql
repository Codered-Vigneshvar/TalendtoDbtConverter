-- dbt model #1
{{ config(materialized='table') }}

{% set source_file = 'C:/Users/TV562EV/OneDrive - EY/Desktop/demo.txt.txt' %}

WITH source AS (
    {% if execute %}
        {% set results = load_file(source_file) %}
        {% set rows = results.split('\n') %}
        {% set header = rows[0].split('|') %}
        {% set data = rows[1:] %}

        SELECT
            {% for col in header %}
            {% set col = col.strip() %}
            {% if loop.first %} '{{ col }}' as {{ col }}
            {% else %} '{{ col }}' as {{ col }}
            {% endif %}
            {% if not loop.last %},{% endif %}
            {% endfor %}
        {% for row in data %}
        {% set values = row.split('|') %}
            UNION ALL
            SELECT
            {% for value in values %}
                {% set value = value.strip() %}
            {% if loop.first %} '{{ value }}'
            {% else %} '{{ value }}'
            {% endif %}
            {% if not loop.last %},{% endif %}
            {% endfor %}
        {% endfor %}
    {% else %}
        SELECT 1 as ID, 'Dummy' as NAME, 'Dummy' as DEPT, 1 as SAL, 'Dummy' as AGE
        where 1=0
    {% endif %}
),

renamed AS (
    SELECT
        ID,
        NAME,
        DEPT,
        CAST(SAL AS INT) AS SAL,
        AGE
    FROM source
)

SELECT *
FROM renamed

-- dbt model #2
{{ config(materialized='table') }}

WITH staged_employees AS (
    SELECT *
    FROM {{ ref('stg_employees') }}
),

unique_employees AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY ID) AS row_num
    FROM staged_employees
),

transformed_employees AS (
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
        CAST(CURRENT_TIMESTAMP() AS STRING) AS DATE_TIMESTAMP -- Adjust for your specific database
    FROM unique_employees
    where row_num = 1
)

SELECT *
FROM transformed_employees

-- dbt model #3
{{ config(materialized='table') }}

WITH int_employees AS (
    SELECT *
    FROM {{ ref('int_employee_salaries') }}
)

SELECT
    ID,
    NAME,
    DEPT,
    SAL,
    AGE,
    SAL_RANGE,
    DATE_TIMESTAMP
FROM int_employees

-- dbt model #4
{{ config(materialized='table') }}

WITH staged_employees AS (
    SELECT *
    FROM {{ ref('stg_employees') }}
),

unique_employees AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY ID) AS row_num
    FROM staged_employees
),

duplicate_employees AS (
    SELECT
        ID,
        NAME,
        DEPT,
        SAL,
        AGE
    FROM unique_employees
    where row_num > 1
)

SELECT *
FROM duplicate_employees

