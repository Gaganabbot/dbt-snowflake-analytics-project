{%- macro get_custom_database(custom_database_name, node) -%}
    {%- if target.name == 'prod' -%}
        {{ env_var('SNOWFLAKE_PROD_DATABASE', 'ANALYTICS_PROD') }}
    {%- elif target.name == 'ci' -%}
        {{ env_var('SNOWFLAKE_CI_DATABASE', 'ANALYTICS_CI') }}
    {%- else -%}
        {{ env_var('SNOWFLAKE_DEV_DATABASE', 'ANALYTICS_DEV') }}
    {%- endif -%}
{%- endmacro -%}
