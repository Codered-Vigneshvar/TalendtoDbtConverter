-- dbt model #1
{{ config(materialized='table') }}

SELECT
    ID,
    NAME,
    DEPT,
    SAL,
    AGE
FROM {{ source('your_source_name', 'raw_employee_data') }}

-- dbt model #2
*   Selects all the columns

**2. Intermediate Model (int_unique_employees.sql):**

This model removes the duplicates.

-- dbt model #3
**Explanation:**

*   `{{ ref('stg_employee_data') }}`:  References the staging model we created in the previous step.  dbt will handle the dependency.
*   `QUALIFY ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY NAME) = 1`:  This is crucial for removing duplicates based on the `NAME` column. The `ROW_NUMBER()` window function assigns a unique number to each row within a partition defined by the `NAME` column. The `QUALIFY` clause filters the results, keeping only the first row within each partition.  The `ORDER BY NAME` is included for completeness; while not strictly necessary for removing duplicates, it ensures consistent selection of the "first" record.

**3. Intermediate Model (int_employee_salaries.sql):**

This model calculates the salary range and add the current timestamp.

-- dbt model #4
**Explanation:**

*   `CASE` statement:  Implements the salary range logic from the Talend `tMap` component.
*   `CURRENT_TIMESTAMP()`:  Gets the current timestamp. The `CAST` is added to match the target string column.

**4. Final Model (fact_employee_data.sql):**

This model represents the final processed data. You can configure it to be an incremental model.

-- dbt model #5
**Schema Compliance Check (Handling in dbt)**

The `tSchemaComplianceCheck` component in Talend performs data quality checks. In dbt, we can achieve similar results using:

*   **dbt tests:**  Use dbt's built-in testing framework to validate data types, nullability, uniqueness, and other constraints.
*   **Custom data quality models:**  Create separate models that specifically focus on data quality checks.

Here are some example dbt tests you could add to your models:

-- dbt model #6
Create these custom test macros in your `macros/` directory. Examples:

-- dbt model #7


-- dbt model #8
* Update the where clause as per your target timestamp format.

To run the tests, use the command `dbt test`.

**5. Write to MySQL (Update Operation):**

This part assumes you are using `fact_employee_data` model to update table on MySQL.
Add post_hook in the `dbt_project.yml` file.

-- dbt model #9
Create macro `sync_mysql`

