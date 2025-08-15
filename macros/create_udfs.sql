{%- macro create_udfs() -%}
    {%- if target.name == 'prod' -%}
        -- Create any custom UDFs here for production
        create or replace function if not exists {{ target.database }}.{{ target.schema }}.calculate_ltv(
            order_values array,
            order_dates array
        )
        returns float
        language python
        runtime_version = '3.8'
        handler = 'calculate_ltv'
        as
        $$
        import numpy as np
        from datetime import datetime

        def calculate_ltv(order_values, order_dates):
            if not order_values or len(order_values) == 0:
                return 0.0

            # Simple LTV calculation - sum of all order values
            return float(sum(order_values))
        $$;
    {%- endif -%}
{%- endmacro -%}
