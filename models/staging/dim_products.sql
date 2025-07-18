{{
    config(
        materialized='incremental',
        unique_key='product_id'
    )
}}

with base as (
  select
    StockCode,
    Description,
    length(Description) as desc_len
  from {{ source('orders', 'orders') }}
  --where Description is not null
),

ranked as (
  select *,
         row_number() over (partition by StockCode order by desc_len desc) as rn,
         count(distinct Description) over (partition by StockCode) as desc_variation_count
  from base
)

select
  StockCode as product_id,
  Description as product_name,
  desc_variation_count,
  case
    when desc_variation_count > 1 then 'conflict'
    else 'ok'
  end as desc_quality_flag
from ranked
where rn = 1
