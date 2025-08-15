{{ config(
    materialized='table',
    tags=['mart', 'core', 'dimension', 'daily']
) }}

with customer_orders as (
    select * from {{ ref('int_customer_order_history') }}
),

-- Aggregate customer metrics
customer_summary as (
    select 
        customer_id,

        -- Customer profile
        max(customer_name) as customer_name,
        max(customer_email) as customer_email,
        max(customer_created_at) as customer_created_at,
        max(email_provider) as email_provider,

        -- Data quality flags
        max(is_missing_name_flag) as has_missing_name,
        max(is_invalid_email_flag) as has_invalid_email,

        -- Order statistics
        count(distinct order_id) as total_orders,
        count(distinct case when is_completed_order then order_id end) as completed_orders,
        count(distinct case when is_cancelled_order then order_id end) as cancelled_orders,
        count(distinct case when is_pending_order then order_id end) as pending_orders,

        -- Financial metrics
        sum(coalesce(total_order_amount, 0)) as lifetime_value,
        sum(case when is_completed_order then coalesce(total_order_amount, 0) else 0 end) as completed_order_value,
        avg(coalesce(total_order_amount, 0)) as average_order_value,
        min(coalesce(total_order_amount, 0)) as min_order_value,
        max(coalesce(total_order_amount, 0)) as max_order_value,

        -- Behavioral metrics
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        avg(days_since_previous_order) as avg_days_between_orders,

        -- Payment behavior
        avg(coalesce(payment_count, 1)) as avg_payments_per_order,
        sum(case when coalesce(payment_count, 1) > 1 then 1 else 0 end) as split_payment_orders,

        -- Seasonal behavior
        count(distinct order_quarter) as quarters_active,
        count(distinct order_year) as years_active

    from customer_orders
    group by customer_id
),

-- Add calculated fields and segmentation
final as (
    select 
        *,

        -- Recency, Frequency, Monetary components
        datediff('day', last_order_date, current_date()) as days_since_last_order,
        total_orders as order_frequency,
        lifetime_value as monetary_value,

        -- Customer tenure
        datediff('day', customer_created_at, current_date()) as customer_tenure_days,

        -- Order completion rates
        case 
            when total_orders > 0 then 
                completed_orders::float / total_orders::float 
            else 0 
        end as order_completion_rate,

        -- RFM Score (1-5 scale for each component)
        ntile(5) over (order by datediff('day', last_order_date, current_date()) desc) as recency_score,
        ntile(5) over (order by total_orders) as frequency_score,
        ntile(5) over (order by lifetime_value) as monetary_score,

        -- Customer lifecycle stage
        case 
            when total_orders = 1 and datediff('day', last_order_date, current_date()) <= 30 then 'New Customer'
            when datediff('day', last_order_date, current_date()) <= 30 then 'Active Customer'
            when datediff('day', last_order_date, current_date()) between 31 and 90 then 'At Risk'
            when datediff('day', last_order_date, current_date()) between 91 and 365 then 'Dormant'
            when datediff('day', last_order_date, current_date()) > 365 then 'Lost Customer'
            else 'Unclassified'
        end as customer_lifecycle_stage,

        -- Value tier based on thresholds from variables
        case 
            when lifetime_value >= {{ var('high_value_threshold') }} then 'High Value'
            when lifetime_value >= {{ var('medium_value_threshold') }} then 'Medium Value'
            when lifetime_value >= {{ var('low_value_threshold') }} then 'Low Value'
            else 'Minimal Value'
        end as customer_value_tier,

        -- Order frequency tier
        case 
            when total_orders >= 10 then 'Frequent Buyer'
            when total_orders >= 5 then 'Regular Buyer'
            when total_orders >= 2 then 'Repeat Buyer'
            else 'One-time Buyer'
        end as order_frequency_tier

    from customer_summary
)

select * from final
