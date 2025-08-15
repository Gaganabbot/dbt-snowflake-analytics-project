{{ config(
    materialized='view',
    tags=['staging', 'ecommerce', 'financial']
) }}

with source as (
    select * from {{ source('ecommerce', 'payments') }}
),

renamed as (
    select
        -- Primary keys
        id as payment_id,
        order_id,

        -- Payment details
        payment_method,
        amount as amount_cents,
        {{ cents_to_dollars('amount') }} as amount_usd,

        -- Timestamps
        created_at as payment_created_at,

        -- Payment method categorization
        case 
            when payment_method in ('credit_card', 'debit_card') then 'Card Payment'
            when payment_method = 'gift_card' then 'Gift Card'
            when payment_method = 'bank_transfer' then 'Bank Transfer'
            when payment_method = 'coupon' then 'Coupon/Discount'
            when payment_method = 'cash' then 'Cash'
            else 'Other'
        end as payment_method_category,

        -- Payment validation flags
        case 
            when amount <= 0 then true 
            else false 
        end as is_invalid_amount_flag,

        case 
            when payment_method is null or payment_method = '' then true 
            else false 
        end as is_missing_method_flag,

        -- Amount categorization
        case 
            when amount < 2500 then 'Small'        -- < $25
            when amount < 10000 then 'Medium'      -- $25-$100
            when amount < 50000 then 'Large'       -- $100-$500
            else 'Extra Large'                     -- $500+
        end as payment_amount_tier,

        -- Date dimensions for payment timing
        extract(year from created_at) as payment_year,
        extract(month from created_at) as payment_month,
        extract(quarter from created_at) as payment_quarter,
        extract(day from created_at) as payment_day,
        extract(hour from created_at) as payment_hour

    from source
),

final as (
    select * from renamed

    -- Data quality filters  
    where payment_id is not null
      and order_id is not null
      and not is_invalid_amount_flag
      and payment_created_at is not null
)

select * from final
