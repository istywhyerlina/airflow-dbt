with dim_date as (
    SELECT * FROM {{ref('dim_date')}}
),
dim_company as (
    SELECT * FROM {{ref('dim_company')}}
),
dim_people as (
    SELECT * FROM {{ref('dim_people')}}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key( ["r.relationship_id"] ) }} as relationship_id,
    r.relationship_id as nk_relationship,
    c1.company_id as company_id,
    p.people_id as people_id,
    dd1.date_id as start_at,
    dd2.date_id as end_at,
    r.is_past as is_past,
    r.sequence as sequence,
    r.title as title,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
FROM {{ source('staging', 'relationships') }} as r
join dim_company as c1 on r.relationship_object_id = c1.object_id
join dim_people as p on r.person_object_id = p.people_object_id
join dim_date as dd1 on to_date(r.start_at, 'YYYY-MM-DD') = dd1.date_actual
join dim_date as dd2 on to_date(r.end_at, 'YYYY-MM-DD') = dd2.date_actual