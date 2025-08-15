-- Customer Lifetime Value Analysis
-- This analysis calculates CLV using different methods

with customer_metrics as (
    select 
        customer_id,
        customer_name,
        total_orders,
        lifetime_value,
        first_order_date,
        last_order_date,
        customer_lifecycle_stage,
        customer_value_tier,
        datediff('day', first_order_date, last_order_date) as customer_lifespan_days,
        case 
            when total_orders > 1 then 
                datediff('day', first_order_date, last_order_date) / (total_orders - 1)
            else 0 
        end as avg_days_between_orders

    from {{ ref('dim_customers') }}
),

clv_calculations as (
    select 
        *,
        lifetime_value / nullif(total_orders, 0) as average_order_value,

        -- Simple CLV: Historical LTV
        lifetime_value as historical_clv,

        -- Predictive CLV (simple version)
        case 
            when avg_days_between_orders > 0 and customer_lifecycle_stage in ('Active Customer', 'New Customer') then
                lifetime_value * (365.0 / avg_days_between_orders) * 2  -- Assume 2 year future value
            else lifetime_value 
        end as predicted_clv,

        -- CLV score (1-10 scale)
        ntile(10) over (order by lifetime_value) as clv_decile

    from customer_metrics
)

select * from clv_calculations
order by predicted_clv desc
