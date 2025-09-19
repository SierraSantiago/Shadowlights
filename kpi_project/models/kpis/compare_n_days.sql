{{ config(materialized='view') }}

{% set start_date = var('start_date', '2025-01-01') %}
{% set end_date   = var('end_date', '2025-01-15') %}

{% set period_days = (modules.datetime.datetime.strptime(end_date, "%Y-%m-%d") - modules.datetime.datetime.strptime(start_date, "%Y-%m-%d")).days + 1 %}

with metrics as (
    select
        cast(c.date as date) as date,
        c.cac,
        r.roas
    from {{ ref('cac') }} c
    join {{ ref('roas') }} r
        using (record_id, date)
),

last_period as (
    select
        avg(cac) as cac,
        avg(roas) as roas
    from metrics
    where date between '{{ start_date }}' and '{{ end_date }}'
),

prev_period as (
    select
        avg(cac) as cac,
        avg(roas) as roas
    from metrics
    where date between date('{{ start_date }}') - INTERVAL {{ period_days }} DAY
                   and date('{{ start_date }}') - INTERVAL 1 DAY
),

comparison as (
    select
        'CAC' as metric,
        l.cac as last_value,
        p.cac as prev_value,
        (l.cac - p.cac) as delta_abs,
        case when p.cac > 0 then (l.cac - p.cac) / p.cac * 100 else null end as delta_pct
    from last_period l, prev_period p

    union all

    select
        'ROAS' as metric,
        l.roas as last_value,
        p.roas as prev_value,
        (l.roas - p.roas) as delta_abs,
        case when p.roas > 0 then (l.roas - p.roas) / p.roas * 100 else null end as delta_pct
    from last_period l, prev_period p
)

select * from comparison
