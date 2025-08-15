{{ config(
    materialized='view',
    tags=['intermediate', 'customer']
) }}

with customers as (
    select * from {{ ref('stg_ecommerce__customers') }}
),

orders as (
    select * from {{ ref('stg_ecommerce__orders') }}
),

payments as (
    select * from {{ ref('stg_ecommerce__payments') }}
),

-- Aggregate payments by order
order_payments as (
    select 
        order_id,
        sum(amount_usd) as total_order_amount,
        count(*) as payment_count,
        count(distinct payment_method) as distinct_payment_methods,
        {{ dbt_utils.listagg('distinct payment_method', "', '") }} as payment_methods_used,

        -- Payment method breakdown
        sum(case when payment_method_category = 'Card Payment' then amount_usd else 0 end) as card_payment_amount,
        sum(case when payment_method_category = 'Gift Card' then amount_usd else 0 end) as gift_card_amount,
        sum(case when payment_method_category = 'Bank Transfer' then amount_usd else 0 end) as bank_transfer_amount,
        sum(case when payment_method_category = 'Coupon/Discount' then amount_usd else 0 end) as coupon_amount,

        -- Payment timing
        min(payment_created_at) as first_payment_at,
        max(payment_created_at) as last_payment_at

    from payments
    where not is_invalid_amount_flag
    group by order_id
),

-- Join everything together
customer_orders as (
    select 
        c.customer_id,
        c.full_name as customer_name,
        c.email as customer_email,
        c.customer_created_at,
        c.is_missing_name_flag,
        c.is_invalid_email_flag,
        c.email_provider,

        o.order_id,
        o.order_date,
        o.order_status,
        o.is_completed_order,
        o.is_cancelled_order,
        o.is_pending_order,
        o.order_year,
        o.order_month,
        o.order_quarter,
        o.order_day_type,
        o.order_time_of_day,

        op.total_order_amount,
        op.payment_count,
        op.distinct_payment_methods,
        op.payment_methods_used,
        op.card_payment_amount,
        op.gift_card_amount,
        op.bank_transfer_amount,
        op.coupon_amount,
        op.first_payment_at,
        op.last_payment_at,

        -- Calculate order sequence for each customer
        row_number() over (
            partition by c.customer_id 
            order by o.order_date, o.order_id
        ) as customer_order_sequence,

        -- Calculate days since previous order
        datediff('day', 
            lag(o.order_date) over (
                partition by c.customer_id 
                order by o.order_date, o.order_id
            ),
            o.order_date
        ) as days_since_previous_order,

        -- Calculate days as customer when order was placed
        datediff('day', c.customer_created_at, o.order_date) as days_as_customer_at_order,

        -- Order type categorization
        case 
            when row_number() over (partition by c.customer_id order by o.order_date, o.order_id) = 1 
            then 'First Order'
            when row_number() over (partition by c.customer_id order by o.order_date, o.order_id) = 2 
            then 'Second Order'
            else 'Repeat Order'
        end as order_type_category

    from customers c
    inner join orders o 
        on c.customer_id = o.customer_id
    left join order_payments op 
        on o.order_id = op.order_id
),

-- Add running totals and moving averages
final as (
    select 
        *,

        -- Running totals for customer
        sum(coalesce(total_order_amount, 0)) over (
            partition by customer_id 
            order by order_date, order_id 
            rows unbounded preceding
        ) as customer_running_total,

        count(*) over (
            partition by customer_id 
            order by order_date, order_id 
            rows unbounded preceding
        ) as customer_running_order_count,

        -- Moving averages (last 3 orders)
        avg(coalesce(total_order_amount, 0)) over (
            partition by customer_id 
            order by order_date, order_id 
            rows between 2 preceding and current row
        ) as customer_3_order_avg_amount,

        -- Time between payments within order
        case 
            when first_payment_at is not null and last_payment_at is not null
            then datediff('minute', first_payment_at, last_payment_at)
            else 0 
        end as payment_processing_time_minutes

    from customer_orders
)

select * from final
