with dim_date as (
    SELECT * FROM {{ref('dim_date')}}
),
dim_company as (
    SELECT * FROM {{ref('dim_company')}}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key( ["f.funding_round_id"] ) }} as funding_round_id,
    f.funding_round_id as nk_funding_round,
    c1.company_id as object_id,
    dd.date_id as funded_at,
    f.funding_round_type as funding_round_type,
    f.funding_round_code as funding_round_code,
    f.raised_amount_usd as raised_amount_usd,
    f.pre_money_valuation_usd as pre_money_valuation_usd,
    f.post_money_valuation_usd as post_money_valuation_usd,
    f.participants as participants,
    f.is_first_round as is_first_round,
    f.is_last_round as is_last_round,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
FROM {{ source('staging', 'funding_rounds') }} as f
join dim_company as c1 on f.object_id = c1.object_id
join dim_date as dd on f.funded_at = dd.date_actual