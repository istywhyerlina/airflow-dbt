with dim_date as (
    SELECT * FROM {{ref('dim_date')}}
),
dim_company as (
    SELECT * FROM {{ref('dim_company')}}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key( ["m.milestone_id"] ) }} as milestone_id,
    m.milestone_id as nk_milestone,
    c1.company_id as object_id,
    m.description as description,
    m.milestone_code as milestone_code,
    dd.date_id as milestone_at,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
FROM {{ source('staging', 'milestone') }} as m
join dim_company as c1 on m.object_id = c1.object_id
join dim_date as dd on to_date(milestone_at, 'YYYY-MM-DD') = dd.date_actual