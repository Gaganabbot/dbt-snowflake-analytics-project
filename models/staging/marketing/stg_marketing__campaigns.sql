{{ config(
    materialized='view',
    tags=['staging', 'marketing']
) }}

with source as (
    select * from {{ source('marketing', 'campaigns') }}
),

renamed as (
    select
        -- Primary key
        campaign_id,

        -- Campaign details
        campaign_name,
        channel as marketing_channel,
        start_date as campaign_start_date,
        end_date as campaign_end_date,
        budget as campaign_budget,

        -- Timestamps
        created_at as campaign_created_at,
        updated_at as campaign_updated_at,

        -- Campaign status logic
        case 
            when end_date is null then 'Active'
            when end_date < current_date() then 'Ended'
            when start_date > current_date() then 'Scheduled'
            else 'Active'
        end as campaign_status,

        -- Campaign duration
        case 
            when end_date is not null then 
                datediff('day', start_date, end_date)
            else 
                datediff('day', start_date, current_date())
        end as campaign_duration_days,

        -- Channel categorization for reporting
        case 
            when channel in ('search', 'display') then 'Paid Advertising'
            when channel = 'social' then 'Social Media'
            when channel = 'email' then 'Email Marketing'
            when channel = 'direct' then 'Direct Traffic'
            when channel = 'referral' then 'Referral Traffic'
            else 'Other'
        end as channel_category,

        -- Budget tiers
        case 
            when budget < 1000 then 'Small Budget'
            when budget < 10000 then 'Medium Budget'
            when budget < 100000 then 'Large Budget'
            else 'Enterprise Budget'
        end as budget_tier,

        -- Date dimensions
        extract(year from start_date) as campaign_start_year,
        extract(month from start_date) as campaign_start_month,
        extract(quarter from start_date) as campaign_start_quarter

    from source
),

final as (
    select * from renamed

    -- Data quality filters
    where campaign_id is not null
      and campaign_name is not null
      and marketing_channel is not null
      and campaign_start_date is not null
      and campaign_start_date >= '{{ var("start_date") }}'
)

select * from final
