{#
  iceberg_incremental materialization for dbt-duckdb

  Incremental writes to an Iceberg table in an attached catalog.
  Supports two strategies:

  1. append (default):
      {{ config(materialized='iceberg_incremental', incremental_strategy='append') }}
      Simple INSERT INTO — no deletes, no updates.

  2. upsert:
      {{ config(
        materialized='iceberg_incremental',
        incremental_strategy='upsert',
        unique_key='id'
      ) }}
      DELETE matching rows by unique_key, then INSERT new rows.
      Implemented as DELETE + INSERT (not MERGE) inside a transaction.

  All writes are wrapped in BEGIN/COMMIT for Iceberg ACID compliance.

  First run always creates the table via CTAS.
#}

{% materialization iceberg_incremental, adapter="duckdb" %}

  {%- set target_catalog = this.database -%}
  {%- set target_schema = this.schema -%}
  {%- set target_table = this.identifier -%}
  {%- set full_target = target_catalog ~ '.' ~ target_schema ~ '.' ~ target_table -%}

  {%- set incremental_strategy = config.get('incremental_strategy', 'append') -%}
  {%- set unique_key = config.get('unique_key') -%}

  {% do log("iceberg_incremental: target = " ~ full_target ~ ", strategy = " ~ incremental_strategy, info=True) %}

  {#-- Validate config --#}
  {% if incremental_strategy == 'upsert' and not unique_key %}
    {% do exceptions.raise_compiler_error("iceberg_incremental with strategy='upsert' requires a 'unique_key' config") %}
  {% endif %}

  {% if incremental_strategy not in ['append', 'upsert'] %}
    {% do exceptions.raise_compiler_error("iceberg_incremental: unsupported strategy '" ~ incremental_strategy ~ "'. Use 'append' or 'upsert'.") %}
  {% endif %}

  {#-- Check if the table already exists --#}
  {%- set check_sql = "SELECT 1 FROM information_schema.tables WHERE table_catalog = '" ~ target_catalog ~ "' AND table_schema = '" ~ target_schema ~ "' AND table_name = '" ~ target_table ~ "'" -%}

  {%- set table_exists = false -%}
  {%- set results = run_query(check_sql) -%}
  {%- if results and results.rows | length > 0 -%}
    {%- set table_exists = true -%}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% if not table_exists %}
    {#-- First run: create the table with CTAS --#}
    {% do log("iceberg_incremental: creating new table " ~ full_target, info=True) %}

    {%- set create_sql -%}
      BEGIN TRANSACTION;
      CREATE TABLE {{ full_target }} AS (
        {{ compiled_code }}
      );
      COMMIT;
    {%- endset -%}

    {% call statement('main') %}
      {{ create_sql }}
    {% endcall %}

  {% elif incremental_strategy == 'append' %}
    {#-- Append: simple INSERT INTO --#}
    {% do log("iceberg_incremental: appending to " ~ full_target, info=True) %}

    {%- set append_sql -%}
      BEGIN TRANSACTION;
      INSERT INTO {{ full_target }} (
        {{ compiled_code }}
      );
      COMMIT;
    {%- endset -%}

    {% call statement('main') %}
      {{ append_sql }}
    {% endcall %}

  {% elif incremental_strategy == 'upsert' %}
    {#-- Upsert: DELETE matching rows by unique_key, then INSERT --#}
    {% do log("iceberg_incremental: upserting into " ~ full_target ~ " (key=" ~ unique_key ~ ")", info=True) %}

    {#-- Handle composite keys (list) vs single key (string) --#}
    {%- if unique_key is string -%}
      {%- set key_columns = [unique_key] -%}
    {%- else -%}
      {%- set key_columns = unique_key -%}
    {%- endif -%}

    {%- set key_csv = key_columns | join(', ') -%}

    {%- set upsert_sql -%}
      BEGIN TRANSACTION;
      DELETE FROM {{ full_target }}
      WHERE ({{ key_csv }}) IN (
        SELECT {{ key_csv }} FROM (
          {{ compiled_code }}
        ) AS __dbt_source
      );
      INSERT INTO {{ full_target }} (
        {{ compiled_code }}
      );
      COMMIT;
    {%- endset -%}

    {% call statement('main') %}
      {{ upsert_sql }}
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
