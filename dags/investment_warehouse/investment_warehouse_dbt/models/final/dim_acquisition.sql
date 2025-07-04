with dim_date as (
    SELECT * FROM {{ref('dim_date')}}
),
dim_company as (
    SELECT * FROM {{ref('dim_company')}}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key( ["a.acquisition_id"] ) }} as acquisition_id,
    a.acquisition_id as nk_acquisition,
    c1.company_id as acquiring_object_id,
    c2.company_id as acquired_object_id,
    a.term_code as term_code,
    a.price_amount as price_amount,
    a.price_currency_code as price_currency_code,
    dd.date_id as acquired_at,
    a.source_url as source_url,
    a.source_description as source_description,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
FROM {{ source('staging', 'acquisition') }} as a
join dim_company as c1 on a.acquiring_object_id = c1.object_id
join dim_company as c2 on a.acquired_object_id = c2.object_id
join dim_date as dd on a.acquired_at = dd.date_actual