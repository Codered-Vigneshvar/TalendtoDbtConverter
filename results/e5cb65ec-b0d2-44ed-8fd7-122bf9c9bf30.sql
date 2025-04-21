-- dbt model #1
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

-- dbt model #2
**2. `intermediate/int_employees_unique.sql`**

-- dbt model #3
**Explanation:**

*   `{{ ref('stg_employees') }}`:  This uses dbt's `ref` function to reference the `stg_employees` model.  dbt handles dependency management.
*   `row_number() over (partition by name order by id)`: Creates a row number partitioned by `name`. This assigns a unique number to each row with the same `name`, ordered by `id`.  The `where row_num = 1` clause keeps only the first occurrence of each `name`. If the NAME is not unique, then it arbitrarily picks one of the NAME entries ordered by ID.

**3. `models/employees.sql`**

-- dbt model #4
**Explanation:**

*   `CASE WHEN ... THEN ... ELSE ... END`:  This implements the conditional logic for calculating `sal_range`, mirroring the `tMap_1` component.
*   `{{ current_timestamp() }}`: This uses a dbt macro to get the current timestamp in a database-agnostic way.  This is better than `TalendDate.getDate("YYYY-MM-DD hh:mm:ss")` which is specific to Talend's expression language.

**Data Validation (Schema Compliance)**

The `tSchemaComplianceCheck_1` component is a bit more complex. dbt doesn't have a direct equivalent *component*. Instead, data validation is typically handled using:

*   **Schema Tests:**  Define data types and constraints in your `schema.yml` files within your dbt project. dbt will automatically run these tests.
*   **Data Tests:** Write SQL-based tests to validate data quality and business rules.

**Example `schema.yml` (in the same directory as your `employees.sql` model):**

-- dbt model #5
**Explanation of `schema.yml`:**

*   `not_null`:  Ensures the column does not contain null values. This enforces the nullable configurations specified in the Talend job.
*   `unique`: Enforces the `unique` key on the `id` column.
*   `accepted_values`: Checks that the column only contains values from a predefined list. This is useful for `sal_range`.
*   `dbt_utils.expression_is_true`: A more general test that allows you to write any SQL expression that must be true.  You can use this to enforce constraints like `sal >= 0`.

**Additional Data Tests (SQL-based):**

If you need more complex validation, you can create SQL-based tests.  For example, create a file like `tests/assert_valid_dept.sql`:

