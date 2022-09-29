
select

	distinct id as opp_id,
	name as opp_name,
	account_id as account_id,
	partner_account_c as opp_partner_account,
	stage_name as opp_stage_name,
	case
		when stage_name = 'Diccovery & Qualification' then '1. Discovery & Qualification'
		when stage_name = 'Alignment' then '2. Alignment'
		when stage_name = 'EB Sponsorship' then '3. EB Sponsorship'
		when stage_name = 'Value & Validation' then '4. Value & Validation'
		when stage_name = 'EB Singoff & Contracts' then '5. EB Signoff & Contracts'
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
	fiscal as opp_fiscal,
	loss_reason_c as opp_loss_reason,
	to_timestamp(closed_won_date_c) as opp_closed_won_dte,
	to_timestamp(created_date) as opp_created_dte,
	datediff('day', created_date, closed_won_date_c) as opp_sales_cycle_days,
	to_timestamp(start_date_c) as opp_start_dte,
	to_timestamp(close_date) as opp_close_dte,
	to_timestamp(discovery_call_date_c) as opp_discovery_call_dte,
	to_timestamp(vf_date_c) as opp_vf_dte,
	to_timestamp(tdd_date_c) as opp_tdd_dte,
	to_timestamp(eb_go_no_go_date_c) as opp_eb_go_no_go_dte,
	to_timestamp(poc_date_c) as opp_poc_dte,
	to_timestamp(eb_review_date_c) as opp_eb_review_dte,
	to_timestamp(negotiate_and_close_c) as opp_neg_and_close_dte,
	to_timestamp(vo_date_c) as opp_vo_dte,
	to_timestamp(nbm_meeting_date_c) as opp_nbm_meeting_dte,
	to_timestamp(closed_lost_date_c) as opp_dq_dte,
	forecasted_arr_c as opp_arr,
	net_new_arr_forecast_c as opp_net_new_arr,
	services_nrr_c as opp_services_nrr,
	prior_opportunity_c as prior_opp_id,
	sdr_c as opp_sdr_id,
	is_deleted

from {{ source('salesforce', 'opportunity') }}
where is_deleted = false
order by id asc

