with dim_date as (
    SELECT * FROM {{ref('dim_date')}}
),
dim_company as (
    SELECT * FROM {{ref('dim_company')}}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key( ["i.ipo_id"] ) }} as ipo_id,
    i.ipo_id as nk_ipo,
    c1.company_id as object_id,
    i.valuation_amount as valuation_amount,
    i.valuation_currency_code as valuation_currency_code,
    i.raised_amount as raised_amount,
    i.raised_currency_code as raised_currency_code,
    dd.date_id as public_at,
    i.stock_symbol as stock_symbol,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
FROM {{ source('staging', 'ipos') }} as i
join dim_company as c1 on i.object_id = c1.object_id
join dim_date as dd on i.public_at = dd.date_actual