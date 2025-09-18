{{ config(materialized='view') }}

select
    *,
    case when conversions > 0 then spend / conversions else null end as cac
from {{ ref('base_ads') }}
