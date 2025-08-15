{{ config(
    materialized='view',
    tags=['staging', 'ecommerce']
) }}

with source as (
    select * from {{ source('ecommerce', 'orders') }}
),

renamed as (
    select
        -- Primary keys
        id as order_id,
        customer_id,

        -- Order details
        order_date,
        status as order_status,

        -- Timestamps
        created_at as order_created_at,
        updated_at as order_updated_at,

        -- Business logic flags
        case 
            when status in ('delivered') then true 
            else false 
        end as is_completed_order,

        case 
            when status in ('cancelled', 'returned') then true 
            else false 
        end as is_cancelled_order,

        case 
            when status in ('pending', 'processing') then true 
            else false 
        end as is_pending_order,

        -- Date dimensions
        extract(year from order_date) as order_year,
        extract(month from order_date) as order_month,
        extract(quarter from order_date) as order_quarter,
        extract(day from order_date) as order_day,
        extract(dayofweek from order_date) as order_day_of_week,

        -- Date formatting
        date_trunc('month', order_date) as order_month_date,
        date_trunc('quarter', order_date) as order_quarter_date,
        date_trunc('week', order_date) as order_week_date,

        -- Business categorization
        case 
            when extract(dayofweek from order_date) in (1, 7) then 'Weekend'
            else 'Weekday'
        end as order_day_type,

        case 
            when extract(hour from order_created_at) between 6 and 11 then 'Morning'
            when extract(hour from order_created_at) between 12 and 17 then 'Afternoon'  
            when extract(hour from order_created_at) between 18 and 22 then 'Evening'
            else 'Night'
        end as order_time_of_day

    from source
),

final as (
    select * from renamed

    -- Data quality filters
    where order_id is not null
      and customer_id is not null
      and order_date is not null
      and order_date >= '{{ var("start_date") }}'
      and order_date <= current_date()
)

select * from final
