{{
    config(
        materialized='incremental',
        unique_key='order_id'
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