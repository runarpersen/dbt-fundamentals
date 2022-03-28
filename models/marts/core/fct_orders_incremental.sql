{{
    config(
        materialized='incremental',
        unique_key= 'composite_key'
        
    )
}}
with a as(
    select *, md5 ( concat ( coalesce(to_char(order_id), ''), coalesce(to_char(customer_id), '') ) ) as composite_key,
    from {{ ref('fct_orders') }}
),
final as(
    select * from a
)
select * from final

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where order_id >= (select max(order_id) from {{ this }})

{% endif %}