{{ dbt_utils.surrogate_key('order_id', 'customer_id') }} as new_key

{{
    config(
        materialized='incremental',
        unique_key= 'new_key'
        
    )
}}
with a as(
    select * from {{ ref('fct_orders') }}
),
final as(
    select * from a
)
select * from final

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where order_id >= (select max(order_id) from {{ this }})

{% endif %}