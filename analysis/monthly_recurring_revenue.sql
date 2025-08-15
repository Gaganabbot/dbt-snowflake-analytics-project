-- Monthly Recurring Revenue Analysis
-- Tracks MRR growth and customer cohorts

with monthly_orders as (
    select 
        date_trunc('month', order_date) as order_month,
        customer_id,
        sum(final_order_amount) as customer_monthly_revenue
    from {{ ref('fct_orders') }}
    where is_completed_order = true
    group by 1, 2
),

monthly_revenue as (
    select 
        order_month,
        count(distinct customer_id) as active_customers,
        sum(customer_monthly_revenue) as total_monthly_revenue,
        avg(customer_monthly_revenue) as avg_revenue_per_customer
    from monthly_orders
    group by order_month
),

mrr_growth as (
    select 
        *,
        lag(total_monthly_revenue) over (order by order_month) as prev_month_revenue,
        total_monthly_revenue - lag(total_monthly_revenue) over (order by order_month) as revenue_growth,
        (total_monthly_revenue - lag(total_monthly_revenue) over (order by order_month)) / 
            nullif(lag(total_monthly_revenue) over (order by order_month), 0) * 100 as revenue_growth_rate
    from monthly_revenue
)

select * from mrr_growth
order by order_month
