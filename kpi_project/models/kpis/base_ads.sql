{{ config(materialized='view') }}

select
    date,
    platform,
    account,
    campaign,
    country,
    device,
    spend,
    clicks,
    impressions,
    conversions,
    load_date,
    source_file_name,
    record_id
from ads_data
