{{
    config(
        materialized='incremental',
        unique_key='year_month'
    )
}}

with a as(
    select * from {{ ref('fct_orders') }}
),
final as(
    select * from a
)
select to_char(order_date,'yyyymm') year_month, sum(amount) amount from final
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date_day >= (select max(year_month) from {{ this }})

{% endif %}
group by 1