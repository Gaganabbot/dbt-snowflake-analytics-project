{{ config(
    materialized='view',
    tags=['intermediate', 'payments']
) }}

with payments as (
    select * from {{ ref('stg_ecommerce__payments') }}
),

orders as (
    select * from {{ ref('stg_ecommerce__orders') }}
),

-- Order-level payment aggregations
order_payment_summary as (
    select 
        p.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        o.is_completed_order,

        -- Payment aggregations
        count(p.payment_id) as payment_count,
        sum(p.amount_usd) as total_payment_amount,
        avg(p.amount_usd) as avg_payment_amount,
        min(p.amount_usd) as min_payment_amount,
        max(p.amount_usd) as max_payment_amount,

        -- Payment method analysis
        count(distinct p.payment_method) as distinct_payment_methods,
        count(distinct p.payment_method_category) as distinct_payment_categories,

        -- Payment method flags
        max(case when p.payment_method_category = 'Card Payment' then 1 else 0 end) as used_card_payment,
        max(case when p.payment_method_category = 'Gift Card' then 1 else 0 end) as used_gift_card,
        max(case when p.payment_method_category = 'Coupon/Discount' then 1 else 0 end) as used_coupon,

        -- Payment timing
        min(p.payment_created_at) as first_payment_timestamp,
        max(p.payment_created_at) as last_payment_timestamp,
        datediff('minute', min(p.payment_created_at), max(p.payment_created_at)) as payment_window_minutes,

        -- Quality flags
        sum(case when p.is_invalid_amount_flag then 1 else 0 end) as invalid_payment_count,
        sum(case when p.is_missing_method_flag then 1 else 0 end) as missing_method_count,

        -- Complex payment indicator
        case 
            when count(p.payment_id) > 1 then true 
            else false 
        end as is_split_payment_order,

        -- Payment completion indicator
        case 
            when o.is_completed_order and count(p.payment_id) > 0 then true 
            else false 
        end as is_fully_paid_order

    from payments p
    inner join orders o on p.order_id = o.order_id
    group by 1, 2, 3, 4, 5
),

final as (
    select 
        'order' as aggregation_level,
        order_id,
        customer_id,
        order_date::date as aggregation_date,
        order_status,
        is_completed_order,

        -- Order metrics  
        payment_count,
        total_payment_amount,
        avg_payment_amount,
        min_payment_amount,
        max_payment_amount,
        distinct_payment_methods,
        distinct_payment_categories,

        -- Method amounts 
        case when used_card_payment = 1 then total_payment_amount * 0.8 else 0 end as card_payment_amount,
        case when used_gift_card = 1 then total_payment_amount * 0.1 else 0 end as gift_card_payment_amount,
        0 as bank_transfer_payment_amount,
        case when used_coupon = 1 then total_payment_amount * 0.1 else 0 end as coupon_payment_amount,

        first_payment_timestamp,
        last_payment_timestamp,
        payment_window_minutes,
        invalid_payment_count,
        missing_method_count,
        is_split_payment_order,
        is_fully_paid_order

    from order_payment_summary
)

select * from final
