with dim_company as (
    SELECT * FROM {{ref('dim_company')}}
),
dim_fund as (
    SELECT * FROM {{ref('dim_fund')}}
),
fact_investment as (
    SELECT * FROM {{ref('fact_investment')}}
),
dim_acquisition as (
    SELECT * FROM {{ref('dim_acquisition')}}
),
total_investment as (
    SELECT 
        i.investor_company_id as company_id,
        count(i.funded_company_id) as count_of_invested_company
    FROM fact_investment as i
    GROUP BY i.investor_company_id
),
total_acquisition as (
    SELECT 
        a.acquiring_object_id as company_id,
        count(a.acquired_object_id) as count_of_acquired_company
    FROM dim_acquisition as a
    GROUP BY a.acquiring_object_id
),
total_fund as (
    SELECT 
        f.object_id as company_id,
        count(f.nk_fund) as count_of_fund_raised_by_company
    FROM dim_fund as f
    GROUP BY f.object_id
)
-- This model aggregates the total investments, acquisitions, and funds raised by each company
SELECT 
    {{ dbt_utils.generate_surrogate_key( ["c.company_id"] ) }} as performance_id,
    c.company_id as company_id,
    ti.count_of_invested_company as total_invested_company,
    ta.count_of_acquired_company as total_acquired_company,
    tf.count_of_fund_raised_by_company as total_fund_raised_by_company,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
FROM dim_company as c
left join total_investment as ti on c.company_id = ti.company_id
left join total_acquisition as ta on c.company_id = ta.company_id
left join total_fund as tf on c.company_id = tf.company_id
