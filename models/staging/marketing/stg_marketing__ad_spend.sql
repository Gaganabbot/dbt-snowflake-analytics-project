{{ config(
    materialized='view',
    tags=['staging', 'marketing', 'daily']
) }}

with source as (
    select * from {{ source('marketing', 'ad_spend') }}
),

renamed as (
    select
        -- Primary keys
        campaign_id,
        date as spend_date,

        -- Core metrics
        spend as daily_spend,
        impressions as daily_impressions,
        clicks as daily_clicks,
        conversions as daily_conversions,

        -- Calculated performance metrics
        case 
            when impressions > 0 then 
                round(clicks::float / impressions::float, 4)
            else 0 
        end as click_through_rate,

        case 
            when clicks > 0 then 
                round(conversions::float / clicks::float, 4)
            else 0 
        end as conversion_rate,

        case 
            when clicks > 0 then 
                round(spend::float / clicks::float, 2)
            else 0 
        end as cost_per_click,

        case 
            when conversions > 0 then 
                round(spend::float / conversions::float, 2)
            else 0 
        end as cost_per_conversion,

        case 
            when impressions > 0 then 
                round(spend::float / impressions::float * 1000, 2)
            else 0 
        end as cost_per_thousand_impressions,

        -- Performance flags
        case 
            when impressions > 0 and (clicks::float / impressions::float) > {{ var('high_ctr_threshold') }} 
            then true 
            else false 
        end as is_high_ctr_day,

        case 
            when clicks > 0 and (conversions::float / clicks::float) > {{ var('high_conversion_threshold') }} 
            then true 
            else false 
        end as is_high_conversion_day,

        case 
            when spend > 100 then true 
            else false 
        end as is_high_spend_day,

        -- Date dimensions
        extract(year from date) as spend_year,
        extract(month from date) as spend_month,
        extract(quarter from date) as spend_quarter,
        extract(day from date) as spend_day,
        extract(dayofweek from date) as spend_day_of_week,

        -- Date formatting
        date_trunc('month', date) as spend_month_date,
        date_trunc('quarter', date) as spend_quarter_date,
        date_trunc('week', date) as spend_week_date,

        -- Day type categorization
        case 
            when extract(dayofweek from date) in (1, 7) then 'Weekend'
            else 'Weekday'
        end as spend_day_type

    from source
),

final as (
    select * from renamed

    -- Data quality filters
    where campaign_id is not null
      and spend_date is not null
      and spend_date >= '{{ var("start_date") }}'
      and spend_date <= current_date()
      and daily_spend >= 0
      and daily_impressions >= 0
      and daily_clicks >= 0
      and daily_conversions >= 0
      -- Ensure logical consistency
      and daily_clicks <= daily_impressions
      and daily_conversions <= daily_clicks
)

select * from final
