
select

	distinct id as opp_id,
	name as opp_name,
	account_id as account_id,
	partner_account_c as opp_partner_account,
	stage_name as opp_stage_name,
	case
		when stage_name = 'Pipeline Generation' then '0. Pipeline Generation'
		when stage_name = 'Discovery & Qualification' then '1. Discovery & Qualification'
		when stage_name = 'Alignment' then '2. Alignment'
		when stage_name = 'EB Sponsorship' then '3. EB Sponsorship'
		when stage_name = 'Value & Validation' then '4. Value & Validation'
		when stage_name = 'EB Signoff & Contracts' then '5. EB Signoff & Contracts'
		when stage_name = 'Closed Won' then '6. Closed Won'
		when stage_name = 'Closed Lost' then '7. Closed Lost'
		else 'Other'
	end as opp_stage_name_ordered,
	active_opportunity_c as opp_is_active,
	revenue_type_c as opp_revenue_type,
	lead_source as opp_lead_source,
	secondary_lead_source_c as opp_secondary_lead_source,
	case when (lead_source in ('Partner') or partner_account_c is not null) then true else false end as opp_is_partner_influenced,
	owner_id as opp_owner_id,
	created_by_id as opp_created_by_id,
	commit_status_c as opp_commit_status,
	case 
		when commit_status_c != 'Pipeline' then 'qualified_pipeline'
		when commit_status_c = 'Pipeline' then 'pipeline'
		else 'other'
	end as opp_pipeline_category,
	case when commit_status_c != 'Pipeline' then true else false end as opp_is_qualified_pipeline,
	case when commit_status_c = 'Pipeline' then true else false end as opp_is_pipeline,
	loss_reason_c as opp_loss_reason,
	to_timestamp(closed_won_date_c) as opp_closed_won_dte,
	to_timestamp(created_date) as opp_created_dte,
	datediff('day', created_date, closed_won_date_c) as opp_sales_cycle_days,
	to_timestamp(start_date_c) as opp_start_dte,
	to_timestamp(close_date) as opp_close_dte,
	to_timestamp(closed_lost_date_c) as opp_dq_dte,
    to_timestamp(stage_1_date_c) as opp_stage_1_date,
    to_timestamp(stage_2_date_c) as opp_stage_2_date,
    to_timestamp(stage_3_date_c) as opp_stage_3_date,
    to_timestamp(stage_4_date_c) as opp_stage_4_date,
    to_timestamp(stage_5_date_c) as opp_stage_5_date,
	forecasted_arr_c as opp_arr,
	net_new_arr_forecast_c as opp_net_new_arr,
	services_nrr_c as opp_services_nrr,
	prior_opportunity_c as prior_opp_id,
	sdr_c as opp_sdr_id,
	is_deleted,

    --sale stage requirements

    x_0_account_discovery_c as xo_account_discovery_check,
    x_0_book_nbm_c as x0_book_nbm_check,
    x_0_discovery_call_s_c as x0_discovery_call_check,
    x_0_pg_plan_c as x0_pg_plan_check,
    x_1_champion_c as x1_champion_check,
    x_1_conduct_nbm_c as x1_conduct_nbm_check,
    x_1_define_next_steps_c as x1_define_next_steps_check,
    x_1_demo_c as x1_demo_check,
    x_1_first_meeting_deck_c as x1_first_meeting_deck_check,
    x_1_identify_pain_c as x1_identify_pain_check,
    x_1_metrics_c as x1_metrics_check,
    x_1_value_pyramid_c as x1_value_pyramid_check,
    x_1_vo_approved_by_rd_c as x1_vo_approved_by_rd_check,
    x_2_confirm_alignment_with_champion_c as x2_confirm_alignment_with_champion_check,
    x_2_decision_criteria_c as x2_decision_criteria_check,
    x_2_develop_3_whys_c as x2_develop_3_whys_check,
    x_2_economic_buyer_c as x2_economic_buyer_check,
    x_2_tdd_c as x2_tdd_check




from {{ source('salesforce', 'opportunity') }}
where is_deleted = false
and _fivetran_active = 'TRUE'
order by id asc

