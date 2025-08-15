{{ config(
    materialized='table',
    tags=['mart', 'marketing', 'dimension', 'daily']
) }}

with campaigns as (
    select * from {{ ref('stg_marketing__campaigns') }}
),

campaign_spend as (
    select 
        campaign_id,
        sum(daily_spend) as total_campaign_spend,
        sum(daily_impressions) as total_impressions,
        sum(daily_clicks) as total_clicks,
        sum(daily_conversions) as total_conversions,
        count(distinct spend_date) as active_days,
        min(spend_date) as first_spend_date,
        max(spend_date) as last_spend_date,
        avg(daily_spend) as avg_daily_spend,
        avg(click_through_rate) as avg_click_through_rate,
        avg(conversion_rate) as avg_conversion_rate,
        avg(cost_per_click) as avg_cost_per_click,
        avg(cost_per_conversion) as avg_cost_per_conversion
    from {{ ref('stg_marketing__ad_spend') }}
    group by campaign_id
),

final as (
    select 
        c.campaign_id,
        c.campaign_name,
        c.marketing_channel,
        c.channel_category,
        c.campaign_start_date,
        c.campaign_end_date,
        c.campaign_duration_days,
        c.campaign_status,
        c.campaign_budget,
        c.budget_tier,

        -- Spend metrics
        coalesce(cs.total_campaign_spend, 0) as total_actual_spend,
        coalesce(cs.total_impressions, 0) as total_impressions,
        coalesce(cs.total_clicks, 0) as total_clicks,
        coalesce(cs.total_conversions, 0) as total_conversions,
        coalesce(cs.active_days, 0) as active_spend_days,

        -- Performance metrics
        coalesce(cs.avg_daily_spend, 0) as avg_daily_spend,
        coalesce(cs.avg_click_through_rate, 0) as avg_click_through_rate,
        coalesce(cs.avg_conversion_rate, 0) as avg_conversion_rate,
        coalesce(cs.avg_cost_per_click, 0) as avg_cost_per_click,
        coalesce(cs.avg_cost_per_conversion, 0) as avg_cost_per_conversion,

        -- Budget utilization
        case 
            when c.campaign_budget > 0 then cs.total_campaign_spend / c.campaign_budget 
            else 0 
        end as budget_utilization_rate,

        -- Performance tiers
        case 
            when cs.avg_click_through_rate >= 0.05 then 'High CTR'
            when cs.avg_click_through_rate >= 0.02 then 'Medium CTR'
            when cs.avg_click_through_rate > 0 then 'Low CTR'
            else 'No CTR Data'
        end as ctr_performance_tier,

        case 
            when cs.avg_conversion_rate >= 0.10 then 'High Conversion'
            when cs.avg_conversion_rate >= 0.05 then 'Medium Conversion'
            when cs.avg_conversion_rate > 0 then 'Low Conversion'
            else 'No Conversion Data'
        end as conversion_performance_tier

    from campaigns c
    left join campaign_spend cs on c.campaign_id = cs.campaign_id
)

select * from final
