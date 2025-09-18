{{ config(materialized='view') }}

with metrics as (
    select
        cast(c.date as date) as date,
        c.cac,
        r.roas
    from {{ ref('cac') }} c
    join {{ ref('roas') }} r
        using (record_id, date)
),

date_bounds as (
    select max(date) as max_date from metrics
),

last_30 as (
    select
        avg(cac) as cac,
        avg(roas) as roas
    from metrics, date_bounds
    where date between (cast(max_date as date) - INTERVAL '29 days') and cast(max_date as date)
),

prev_30 as (
    select
        avg(cac) as cac,
        avg(roas) as roas
    from metrics, date_bounds
    where date between (cast(max_date as date) - INTERVAL '59 days') and (cast(max_date as date) - INTERVAL '30 days')
),

comparison as (
    select
        'CAC' as metric,
        l.cac as last_30_value,
        p.cac as prev_30_value,
        (l.cac - p.cac) as delta_abs,
        case when p.cac > 0 then (l.cac - p.cac) / p.cac * 100 else null end as delta_pct
    from last_30 l, prev_30 p

    union all

    select
        'ROAS' as metric,
        l.roas as last_30_value,
        p.roas as prev_30_value,
        (l.roas - p.roas) as delta_abs,
        case when p.roas > 0 then (l.roas - p.roas) / p.roas * 100 else null end as delta_pct
    from last_30 l, prev_30 p
)

select * from comparison
