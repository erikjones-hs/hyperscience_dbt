{{ config
(
    materialized="table"
)
}}


with analysis_table as (
select
opp.opp_id,
opp.opp_name,
opp.opp_location,
opp.contact_emails,
opp.contact_links,
opp.contact_phone_nums,
opp.opp_headline,
opp.opp_origin,
opp.opp_create_dte,
opp.opp_last_interaction,
opp.opp_last_advanced_at,
opp.opp_source,
opp.opp_tag,
opp.opp_archived_at,
opp.opp_archive_reason,
opp.opp_stage_name,
opp.opp_status,
opp.opp_owner,
opp.opp_owner_email,
opp.opp_referrer_name,
app.application_id,
app.application_type,
app.application_create_dte,
app.application_archived_dte,
app.application_archive_reason,
app.application_hiring_manager_id,
app.application_hiring_manager,
app.application_hiring_manager_email,
app.application_ref_id,
app.application_ref_name,
app.application_ref_email,
app.req_for_hire_id,
post.post_id,
post.post_state,
post.post_name,
post.post_create_dte,
post.post_team,
post.post_dept,
post.post_locations,
post.post_commit,
post.post_level,
post.post_req_code,
post.post_creator_id,
post.post_creator_name,
post.post_creator_email,
post.post_owner_id,
post.post_owner_name,
post.post_owner_email,
post.post_tags,
post.post_dist_channels
from "DEV"."HR"."LEVER_DIM_OPPORTUNITY" as opp 
left join "DEV"."HR"."LEVER_DIM_APPLICATION" as app on (app.application_opp_id = opp.opp_id)
left join "DEV"."HR"."LEVER_DIM_POSTING" as post on (app.application_posting_id = post.post_id)
)

select * from analysis_table order by opp_id