{{ config(
    materialized='table',
    tags=['mart', 'core', 'fact', 'daily']
) }}

with order_history as (
    select * from {{ ref('int_customer_order_history') }}
),

payment_aggregations as (
    select * 
    from {{ ref('int_payment_aggregations') }}
    where aggregation_level = 'order'
),

-- Join order data with payment aggregations
orders_enhanced as (
    select 
        -- Primary keys
        oh.order_id,
        oh.customer_id,

        -- Order details
        oh.order_date,
        oh.order_status,
        oh.is_completed_order,
        oh.is_cancelled_order,
        oh.is_pending_order,
        oh.order_type_category,
        oh.customer_order_sequence,
        oh.days_since_previous_order,
        oh.days_as_customer_at_order,

        -- Customer context at time of order
        oh.customer_name,
        oh.customer_email,
        oh.customer_created_at,
        oh.email_provider,

        -- Order timing and context
        oh.order_year,
        oh.order_month,
        oh.order_quarter,
        oh.order_day_type,
        oh.order_time_of_day,

        -- Financial metrics from order history
        oh.total_order_amount as order_amount_from_history,
        oh.payment_count as payment_count_from_history,
        oh.distinct_payment_methods,
        oh.payment_methods_used,
        oh.card_payment_amount,
        oh.gift_card_amount,
        oh.bank_transfer_amount,
        oh.coupon_amount,

        -- Payment timing from order history
        oh.first_payment_at,
        oh.last_payment_at,
        oh.payment_processing_time_minutes,

        -- Enhanced payment metrics from aggregations
        pa.total_payment_amount,
        pa.avg_payment_amount,
        pa.min_payment_amount,
        pa.max_payment_amount,
        pa.distinct_payment_categories,
        pa.used_card_payment,
        pa.used_gift_card,
        pa.used_coupon,
        pa.first_payment_timestamp,
        pa.last_payment_timestamp,
        pa.payment_window_minutes,
        pa.invalid_payment_count,
        pa.is_split_payment_order,
        pa.is_fully_paid_order,

        -- Running customer metrics at time of order
        oh.customer_running_total,
        oh.customer_running_order_count,
        oh.customer_3_order_avg_amount

    from order_history oh
    left join payment_aggregations pa 
        on oh.order_id = pa.order_id
),

-- Add derived business metrics
final as (
    select 
        *,

        -- Use the most complete payment amount available
        coalesce(total_payment_amount, order_amount_from_history, 0) as final_order_amount,

        -- Order value tier
        case 
            when coalesce(total_payment_amount, order_amount_from_history, 0) >= 500 then 'High Value Order'
            when coalesce(total_payment_amount, order_amount_from_history, 0) >= 100 then 'Medium Value Order'
            when coalesce(total_payment_amount, order_amount_from_history, 0) >= 25 then 'Low Value Order'
            else 'Minimal Value Order'
        end as order_value_tier,

        -- Customer behavior at order time
        case 
            when customer_order_sequence = 1 then 'First Purchase'
            when customer_order_sequence <= 3 then 'Early Customer'
            when customer_order_sequence <= 10 then 'Established Customer'
            else 'Loyal Customer'
        end as customer_maturity_at_order,

        -- Order success indicator
        case 
            when is_completed_order and is_fully_paid_order and coalesce(invalid_payment_count, 0) = 0 
            then true 
            else false 
        end as is_successful_order,

        -- Date dimensions for easy filtering
        date_trunc('month', order_date) as order_month_date,
        date_trunc('quarter', order_date) as order_quarter_date,
        extract(dayofweek from order_date) as order_day_of_week_number,

        -- Revenue recognition
        case 
            when is_completed_order then coalesce(total_payment_amount, order_amount_from_history, 0)
            else 0 
        end as recognized_revenue

    from orders_enhanced
)

select * from final
