
select
    
    id as account_id,
    global_account_id_c as global_account_id,
    name as account_name, 
    number_of_employees as account_number_of_employees,
    annual_revenue as account_annual_revenue,
    industry_cleaned_c as account_industry,
    zoominfo_subindustry_c as account_sub_industry,
    sales_region_c as account_sales_region,
    billing_country as account_country,
    tier_c as account_tier,
    owner_id,
    csm_c as csm_id,
    account_manager_c as account_manager_id
    
from {{ source('salesforce', 'account') }}
