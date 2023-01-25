
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
    opp.opp_loss_reason,
    opp.opp_closed_won_dte,
    opp.opp_created_dte,
    opp.opp_sales_cycle_days,
    opp.opp_start_dte,
    opp.opp_close_dte,
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
    opp.opp_stage_1_date,
    opp.opp_stage_2_date,
    opp.opp_stage_3_date,
    opp.opp_stage_4_date,
    opp.opp_stage_5_date,
    xo_account_discovery_check,
    x0_book_nbm_check,
    x0_discovery_call_check,
    x0_pg_plan_check,
    x1_champion_check,
    x1_conduct_nbm_check,
    x1_define_next_steps_check,
    x1_demo_check,
    x1_first_meeting_deck_check,
    x1_identify_pain_check,
    x1_metrics_check,
    x1_value_pyramid_check,
    x1_vo_approved_by_rd_check,
    x2_confirm_alignment_with_champion_check,
    x2_decision_criteria_check,
    x2_develop_3_whys_check,
    x2_economic_buyer_check,
    x2_tdd_check


    from {{ ref('stg_opportunities') }} opp
    left join {{ ref('stg_accounts') }} account on (opp.account_id = account.account_id)
    left join {{ ref('stg_accounts') }} account_2 on (opp.opp_partner_account = account_2.account_id)
    left join {{ ref('dim_users_and_queues') }} opportunity_owner on (opp.opp_owner_id = opportunity_owner.id)
    left join {{ ref('dim_users_and_queues') }} created_by on (opp.opp_created_by_id = created_by.id)
    left join {{ ref('dim_users_and_queues') }} account_owner on (account.owner_id = account_owner.id)
    left join {{ ref('dim_users_and_queues') }} sdr  on (opp.opp_sdr_id = sdr.id)
    left join {{ ref('stg_marketing_influenced_opps') }} marketing_influenced on (opp.opp_id = marketing_influenced.marketing_influenced_opportunity_id)
    where opp.is_deleted = false








