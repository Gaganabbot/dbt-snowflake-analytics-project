-- Test that all completed orders have positive payment amounts

select 
    order_id,
    final_order_amount
from {{ ref('fct_orders') }}
where is_completed_order = true
  and (final_order_amount is null or final_order_amount <= 0)
