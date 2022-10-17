
select 

    opp_id,
    opp_name,
    opp.account_id,
    account.account_name,
    account.account_industry,
    account.account_sales_region,
    opp.opp_partner_account,
    account_2.account_name as partner_account_name,
    opp.opp_stage_name,
    opp.opp_stage_name_ordered,
    opp.opp_is_active,
    opp.opp_revenue_type,
    opp.opp_lead_source,
    opp.opp_secondary_lead_source,
    opp.opp_is_partner_influenced,
    case when marketing_influenced.marketing_influenced_opportunity_id is not null then true else false end as opp_is_marketing_influenced,
    opp.opp_commit_status,
    opp.opp_pipeline_category,
    opp.opp_is_qualified_pipeline,
    opp.opp_is_pipeline,
    opp.opp_fiscal,
    opp.opp_loss_reason,
    opp.opp_closed_won_dte,
    opp.opp_created_dte,
    opp.opp_sales_cycle_days,
    opp.opp_start_dte,
    opp.opp_close_dte,
    opp.opp_discovery_call_dte,
    opp.opp_vf_dte,
    opp.opp_tdd_dte,
    opp.opp_eb_go_no_go_dte,
    opp.opp_poc_dte,
    opp.opp_eb_review_dte,
    opp.opp_neg_and_close_dte,
    opp.opp_vo_dte,
    opp.opp_nbm_meeting_dte,
    opp.opp_dq_dte,/*
    fy.fy_qtr_year as closed_won_fy_qtr,
    fy.qtr_end_dte as closed_won_qtr_end_dte,
    fy1.fy_qtr_year as close_fy_qtr,
    fy1.qtr_end_dte as close_qtr_end_dte,
    fy2.fy_qtr_year as start_fy_qtr,
    fy2.qtr_end_dte as start_qtr_end_dte,*/
    opp.opp_arr,
    opp.opp_net_new_arr,
    opp.prior_opp_id,
    opp.opp_services_nrr,
    opp.is_deleted,
    opp.opp_owner_id,
    opportunity_owner.full_name as opp_owner_name,
    opportunity_owner.role_name as opp_owner_role,
    opp.opp_created_by_id,
    created_by.full_name as opp_created_by_name,
    account_owner.full_name as account_owner_name,
    account_owner.role_name as account_owner_role,
    sdr.full_name as sdr_name,
    had_value_fit,
    had_tdd,
    had_eb_go_no_go,
    had_tve,
    had_eb_revisit,
    had_negotiate_and_close


    from {{ ref('stg_opportunities') }} opp
    left join {{ ref('stg_accounts') }} account on (opp.account_id = account.account_id)
    left join {{ ref('stg_accounts') }} account_2 on (opp.opp_partner_account = account_2.account_id)
    left join {{ ref('dim_users_and_queues') }} opportunity_owner on (opp.opp_owner_id = opportunity_owner.id)
    left join {{ ref('dim_users_and_queues') }} created_by on (opp.opp_created_by_id = created_by.id)
    left join {{ ref('dim_users_and_queues') }} account_owner on (account.owner_id = account_owner.id)
    left join {{ ref('dim_users_and_queues') }} sdr  on (opp.opp_sdr_id = sdr.id)
    left join {{ ref('stg_marketing_influenced_opps') }} marketing_influenced on (opp.opp_id = marketing_influenced.marketing_influenced_opportunity_id)
    left join {{ ref('stg_opp_stage_history') }} on (opp.opp_id = opportunity_id)
    where opp.is_deleted = false








