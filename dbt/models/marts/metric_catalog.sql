{#- One row per mart column tagged with `metric` meta in this package's yml.
    Built from dbt's graph at execution time so the catalog never drifts from
    the column definitions. Portable SQL (BigQuery + DuckDB). -#}
{%- set rows = [] -%}
{%- if execute -%}
  {%- for node in graph.nodes.values() | sort(attribute='name') -%}
    {%- if node.resource_type == 'model' and node.package_name == project_name -%}
      {%- for col_name, col in node.columns.items() -%}
        {%- set m = (col.config.meta or {}) if col.config is defined and col.config else {} -%}
        {%- set metric = m.get('metric') or (col.meta or {}).get('metric') -%}
        {%- if metric -%}
          {%- do rows.append({
              'metric': col_name,
              'mart': node.name,
              'column_name': col_name,
              'label': metric.get('label'),
              'unit': metric.get('unit'),
              'description': col.description
          }) -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endfor -%}
{%- endif -%}

{% if rows | length > 0 %}
{% for r in rows | sort(attribute='metric') %}
select
    cast({{ dbt.string_literal(dbt.escape_single_quotes(r.metric or '')) }} as {{ dbt.type_string() }}) as metric,
    cast({{ dbt.string_literal(dbt.escape_single_quotes(r.mart or '')) }} as {{ dbt.type_string() }}) as mart,
    cast({{ dbt.string_literal(dbt.escape_single_quotes(r.column_name or '')) }} as {{ dbt.type_string() }}) as column_name,
    cast({{ dbt.string_literal(dbt.escape_single_quotes(r.label or '')) }} as {{ dbt.type_string() }}) as label,
    cast({{ dbt.string_literal(dbt.escape_single_quotes(r.unit or '')) }} as {{ dbt.type_string() }}) as unit,
    cast({{ dbt.string_literal(dbt.escape_single_quotes(r.description or '')) }} as {{ dbt.type_string() }}) as description
{% if not loop.last %}union all{% endif %}
{% endfor %}
{% else %}
select
    cast(null as {{ dbt.type_string() }}) as metric,
    cast(null as {{ dbt.type_string() }}) as mart,
    cast(null as {{ dbt.type_string() }}) as column_name,
    cast(null as {{ dbt.type_string() }}) as label,
    cast(null as {{ dbt.type_string() }}) as unit,
    cast(null as {{ dbt.type_string() }}) as description
where false
{% endif %}
