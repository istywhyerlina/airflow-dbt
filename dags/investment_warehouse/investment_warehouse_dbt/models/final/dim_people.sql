with dim_company as (
    SELECT * FROM {{ref('dim_company')}}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key( ["p.people_id"] ) }} as people_id,
    p.people_id as nk_people,
    p.object_id as people_object_id,
    p.first_name as first_name,
    p.last_name as last_name,
    p.affiliation_name as affiliation_name,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
FROM {{ source('staging', 'people') }} as p
