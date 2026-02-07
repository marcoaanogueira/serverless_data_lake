{#
  iceberg_table materialization for dbt-duckdb

  Full overwrite of an Iceberg table in an attached catalog.
  dbt-duckdb manages the transaction context automatically.

  Usage:
    {{ config(materialized='iceberg_table') }}
    SELECT col1, col2 FROM source_table

  The table is referenced as: <catalog>.<schema>.<table>
  via {{ this.database }}.{{ this.schema }}.{{ this.identifier }}

  Behavior:
    - First run: CREATE TABLE AS SELECT
    - Subsequent runs: DELETE FROM + INSERT INTO
#}

{% materialization iceberg_table, adapter="duckdb" %}

  {%- set target_catalog = this.database -%}
  {%- set target_schema = this.schema -%}
  {%- set target_table = this.identifier -%}
  {%- set full_target = target_catalog ~ '.' ~ target_schema ~ '.' ~ target_table -%}

  {% do log("iceberg_table: target = " ~ full_target, info=True) %}

  {#-- Check if the table already exists by attempting a metadata query --#}
  {%- set check_sql = "SELECT 1 FROM information_schema.tables WHERE table_catalog = '" ~ target_catalog ~ "' AND table_schema = '" ~ target_schema ~ "' AND table_name = '" ~ target_table ~ "'" -%}

  {%- set table_exists = false -%}
  {%- set results = run_query(check_sql) -%}
  {%- if results and results.rows | length > 0 -%}
    {%- set table_exists = true -%}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% if not table_exists %}
    {#-- First run: create the table with CTAS --#}
    {% do log("iceberg_table: creating new table " ~ full_target, info=True) %}

    {%- set create_sql -%}
      CREATE TABLE {{ full_target }} AS (
        {{ compiled_code }}
      );
    {%- endset -%}

    {% call statement('main') %}
      {{ create_sql }}
    {% endcall %}

  {% else %}
    {#-- Subsequent runs: overwrite via DELETE + INSERT in transaction --#}
    {% do log("iceberg_table: overwriting " ~ full_target, info=True) %}

    {%- set overwrite_sql -%}
      DELETE FROM {{ full_target }};
      INSERT INTO {{ full_target }} (
        {{ compiled_code }}
      );
    {%- endset -%}

    {% call statement('main') %}
      {{ overwrite_sql }}
    {% endcall %}

  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {#-- Return a relation so dbt tracks it --#}
  {%- set target_relation = api.Relation.create(
      database=target_catalog,
      schema=target_schema,
      identifier=target_table,
      type='table'
  ) -%}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
