{% materialization batched_table, adapter="duckdb" %}

  {# -- Configuration -- #}
  {%- set batch_column = config.get('batch_column', 'site_id') -%}
  {%- set batch_size = config.get('batch_size', 50) -%}

  {# -- Relation setup (mirrors built-in table materialization) -- #}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {%- set grant_config = config.get('grants') -%}

  {# -- Temp view name (unique per model to avoid collisions) -- #}
  {%- set tmp_view = '__dbt_batched_' ~ model['unique_id'] | replace('.', '_') -%}

  {# -- Drop stale temp relations -- #}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {# -- Step 1: Create temp view from model SQL (lazy, zero memory) -- #}
  {% call statement('create_view') %}
    create or replace temporary view {{ tmp_view }} as (
      {{ sql }}
    )
  {% endcall %}

  {# -- Step 2: Get distinct batch keys -- #}
  {% call statement('get_batch_keys', fetch_result=True) %}
    select distinct {{ batch_column }} from {{ tmp_view }} order by {{ batch_column }}
  {% endcall %}

  {%- set batch_keys_table = load_result('get_batch_keys') -%}
  {%- set batch_keys = batch_keys_table['data'] | map(attribute=0) | list -%}
  {%- set total_keys = batch_keys | length -%}

  {{ log("BATCHED_TABLE: " ~ model.unique_id ~ " — " ~ total_keys ~ " distinct " ~ batch_column ~ " values, batch_size=" ~ batch_size, info=True) }}

  {# -- Step 3: Process in batches -- #}
  {%- set ns = namespace(batch_num=0) -%}

  {%- for i in range(0, total_keys, batch_size) -%}
    {%- set batch = batch_keys[i : i + batch_size] -%}
    {%- set ns.batch_num = ns.batch_num + 1 -%}
    {%- set batch_end = [i + batch_size, total_keys] | min -%}

    {# -- Build IN clause with proper quoting -- #}
    {%- set quoted_keys = [] -%}
    {%- for key in batch -%}
      {%- do quoted_keys.append("'" ~ key | replace("'", "''") ~ "'") -%}
    {%- endfor -%}
    {%- set in_clause = quoted_keys | join(', ') -%}

    {{ log("BATCHED_TABLE: batch " ~ ns.batch_num ~ " — " ~ batch_column ~ " values " ~ (i + 1) ~ "-" ~ batch_end ~ " of " ~ total_keys, info=True) }}

    {%- if ns.batch_num == 1 -%}
      {# -- First batch: CREATE TABLE (uses statement 'main' for dbt logging) -- #}
      {% call statement('main') %}
        create table {{ intermediate_relation }} as (
          select * from {{ tmp_view }}
          where {{ batch_column }} in ({{ in_clause }})
        )
      {% endcall %}
    {%- else -%}
      {# -- Subsequent batches: INSERT INTO -- #}
      {% call statement('batch_' ~ ns.batch_num) %}
        insert into {{ intermediate_relation }}
        select * from {{ tmp_view }}
        where {{ batch_column }} in ({{ in_clause }})
      {% endcall %}
    {%- endif -%}
  {%- endfor -%}

  {# -- Handle empty result set -- #}
  {%- if total_keys == 0 -%}
    {{ log("BATCHED_TABLE: no data found, creating empty table", info=True) }}
    {% call statement('main') %}
      create table {{ intermediate_relation }} as (
        select * from {{ tmp_view }} where 1=0
      )
    {% endcall %}
  {%- endif -%}

  {# -- Step 4: Drop the temp view -- #}
  {% call statement('drop_view') %}
    drop view if exists {{ tmp_view }}
  {% endcall %}

  {# -- Step 5: Swap relations (same pattern as built-in table materialization) -- #}
  {% if existing_relation is not none %}
    {% do drop_indexes_on_relation(existing_relation) %}
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {% endif %}

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
