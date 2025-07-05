with dim_date as (
    SELECT * FROM {{ref('dim_date')}}
),
dim_company as (
    SELECT * FROM {{ref('dim_company')}}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key( ["f.fund_id"] ) }} as fund_id,
    f.fund_id as nk_fund,
    c1.company_id as object_id,
    f.name as name,
    dd.date_id as funded_at,
    f.raised_amount as raised_amount,
    f.raised_currency_code as raised_currency_code,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
FROM {{ source('staging', 'funds') }} as f
join dim_company as c1 on f.object_id = c1.object_id
join dim_date as dd on f.funded_at = dd.date_actual