{{ config(materialized='view') }}

select
    *,
    conversions * 100 as revenue,
    case when spend > 0 then (conversions * 100.0) / spend else null end as roas
from {{ ref('base_ads') }}
