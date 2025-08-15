{{ config(
    materialized='table',
    tags=['mart', 'marketing', 'fact', 'daily']
) }}

with ad_spend as (
    select * from {{ ref('stg_marketing__ad_spend') }}
),

campaigns as (
    select * from {{ ref('stg_marketing__campaigns') }}
),

final as (
    select 
        -- Primary keys
        {{ dbt_utils.generate_surrogate_key(['ad.campaign_id', 'ad.spend_date']) }} as performance_id,
        ad.campaign_id,
        ad.spend_date,

        -- Campaign context
        c.campaign_name,
        c.marketing_channel,
        c.channel_category,
        c.campaign_status,

        -- Daily metrics
        ad.daily_spend,
        ad.daily_impressions,
        ad.daily_clicks,
        ad.daily_conversions,

        -- Performance ratios
        ad.click_through_rate,
        ad.conversion_rate,
        ad.cost_per_click,
        ad.cost_per_conversion,
        ad.cost_per_thousand_impressions,

        -- Performance flags
        ad.is_high_ctr_day,
        ad.is_high_conversion_day,
        ad.is_high_spend_day,

        -- Date dimensions
        ad.spend_year,
        ad.spend_month,
        ad.spend_quarter,
        ad.spend_month_date,
        ad.spend_day_of_week,

        -- Running totals
        sum(ad.daily_spend) over (
            partition by ad.campaign_id 
            order by ad.spend_date 
            rows unbounded preceding
        ) as cumulative_spend,

        sum(ad.daily_conversions) over (
            partition by ad.campaign_id 
            order by ad.spend_date 
            rows unbounded preceding
        ) as cumulative_conversions,

        -- Moving averages (7-day)
        avg(ad.daily_spend) over (
            partition by ad.campaign_id 
            order by ad.spend_date 
            rows between 6 preceding and current row
        ) as spend_7day_moving_avg,

        avg(ad.click_through_rate) over (
            partition by ad.campaign_id 
            order by ad.spend_date 
            rows between 6 preceding and current row
        ) as ctr_7day_moving_avg

    from ad_spend ad
    join campaigns c on ad.campaign_id = c.campaign_id
)

select * from final
