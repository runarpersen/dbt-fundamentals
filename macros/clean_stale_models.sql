{% macro clean_stale_models(database=target.database, schema=target.schema,days=7, dry_run=True) %}
    {% set get_drop_commands_query %}
      select case when table_name = 'VIEW' then table_type else 'TABLE' end as drop_type,
        'DROP ' || upper(drop_type) || ' ANALYTICS.ANALYZE.' || upper(table_name) as drp_cmd
        from analytics.information_schema.tables
        where table_schema = 'ANALYZE'
        and last_altered <= current_date() - 7
    {% endset %}

    {{ log('\nGenerating cleanup queries...\n', info=true) }}
    {% set drop_queries = run_query(get_drop_commands_query).columns[1].values() %}

    {% for query in drop_queries %}
        {% if dry_run %}
            {{ log(query, info=True) }}   
        {% else %}
            {{ log('Dropping object with command: ' ~ query, info=True) }}
            {% do run_query(query)  %}
        {% endif %}      
    {% endfor %}
{% endmacro %}