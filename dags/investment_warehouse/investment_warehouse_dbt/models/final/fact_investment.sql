with dim_company as (
    SELECT * FROM {{ref('dim_company')}}
),
dim_funding_round as (
    SELECT * FROM {{ref('dim_funding_round')}}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key( ["i.investment_id"] ) }} as investment_id,
    i.investment_id as nk_investment,
    f.funding_round_id as funding_round_id,
    c1.company_id as funded_company_id,
    c2.company_id as investor_company_id,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
FROM {{ source('staging', 'investments') }} as i
join dim_company as c1 on i.funded_object_id = c1.object_id
join dim_company as c2 on i.investor_object_id = c2.object_id
join dim_funding_round as f on i.funding_round_id = f.nk_funding_round
