{% test table_freshness(model, stale_after_seconds) %}
  {{ return(adapter.dispatch('table_freshness', 'datahub')(model, stale_after_seconds)) }}
{% endtest %}

-- Generic macro, which throws an error.
{% macro default__table_freshness(model, stale_after_seconds) %}
	{{ exceptions.raise_not_implemented(
		"table_freshness is not supported with adapter"+adapter.type()) }}
{% endmacro %}

-- Snowflake macro.
{% macro snowflake__table_freshness(model, stale_after_seconds) %}

with recency as (
    SELECT last_altered
    FROM {{ model.database }}.information_schema.tables
    WHERE UPPER(table_schema) = UPPER('{{ model.schema }}') 
	AND   UPPER(table_name) = UPPER('{{ model.table }}')
)

select last_altered
from recency
where last_altered < {{ dbt.dateadd(datepart="seconds", interval=-1 * stale_after_seconds,
									from_date_or_timestamp=dbt.current_timestamp()) }}

{% endmacro %}
