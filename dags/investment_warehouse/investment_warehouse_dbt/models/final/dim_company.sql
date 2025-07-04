SELECT 
    {{ dbt_utils.generate_surrogate_key( ["c.office_id"] ) }} as company_id,
    c.office_id as nk_office,
    object_id,
    description,
    region,
    address1,
    address2,
    city,
    zip_code, 
    state_code,
    country_code,
    latitude,
    longitude,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
FROM {{source('staging', 'company')}} as c