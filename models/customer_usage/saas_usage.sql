{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CUSTOMER_USAGE'
)
}} 

with raw_data as (
select distinct
REPORT_ID,
to_timestamp(to_date(PERIOD_START)) as period_start,
SUBMISSIONS_CREATED_COUNT as number_of_submissions_created,
SUBMISSIONS_COUNT as number_of_submissions_completed,
FORMS_CREATED_COUNT as number_of_documents_created,
FORMS_COUNT as number_of_documents_completed,
SUBMISSION_PAGE_CREATED_COUNT as number_of_pages_created,
SUBMISSION_PAGE_COUNT as number_of_pages_completed,
PAGES_CREATED_STRUCTURED_COUNT as number_of_pages_matched_to_form_layouts_created,
PAGES_STRUCTURED_COUNT as number_of_pages_matched_to_form_layouts_completed,
PAGES_CREATED_VARIABLE_COUNT as number_of_pages_matched_to_flex_layouts_created,
PAGES_VARIABLE_COUNT as number_of_pages_matched_to_flex_layouts_completed,
PAGES_WITH_FIELDS_CREATED_COUNT as number_of_pages_with_fields_on_them_created,
PAGES_WITH_FIELDS_COUNT as number_of_pages_with_fields_on_them_completed,
FIELDS_CREATED_COUNT as number_of_fields_created,
FIELDS_COMPLETED_COUNT as number_of_fields_completed,
TRANSCRIPTION_CHARACTERS_COUNT as number_of_characters_completed,
LOGIN_COUNT as seats,
SUBMISSIONS_FILES_CREATED_COUNT as number_of_filed_submitted,
MACHINE_TRANSCRIBED_ENTRIES_COUNT as number_of_fields_machine_transcribed,
HUMAN_TRANSCRIBED_ENTRIES_COUNT as number_of_fields_manually_transcribed,
MACHINE_IDENTIFIED_FIELDS_COUNT as number_of_fields_machine_identified,
HUMAN_IDENTIFIED_FIELDS_COUNT as number_of_fields_manually_indentified,
PAGES_COUNT_MACHINE_SUM as number_of_pages_classified_automatically,
PAGES_COUNT_MANUAL_SUM as number_of_pages_classified_manually,
TEMPLATE_COUNT_MATCH as number_of_unique_layouts_matched,
MANUAL_CHECKED_TRANSCRIPTIONS_COUNT as number_of_qa_responses_on_manual_transcription,
MANUAL_CORRECT_TRANSCRIPTIONS_COUNT as number_of_qa_correct_responses_on_manual_transcription,
SYSTEM_CHECKED_TRANSCRIPTIONS_COUNT as number_of_qa_responses_on_system_transcription,
SYSTEM_CORRECT_TRANSCRIPTIONS_COUNT as number_of_qa_correct_responses_on_system_transcription,
MACHINE_CHECKED_TRANSCRIPTIONS_COUNT as number_of_qa_responses_on_machine_transcription,
MACHINE_CORRECT_TRANSCRIPTIONS_COUNT as number_of_qa_corect_responses_on_machine_transcription,
MANUAL_CHECKED_FIELD_ID_COUNT as number_of_qa_responses_on_manual_field_identification,
MANUAL_CORRECT_FIELD_ID_COUNT as number_of_qa_correct_responses_on_manual_field_identification,
SYSTEM_CHECKED_FIELD_ID_COUNT as number_of_qa_responses_on_system_field_identification,
SYSTEM_CORRECT_FIELD_ID_COUNT as number_of_qa_correct_responses_on_system_field_identification,
MACHINE_CHECKED_FIELD_ID_COUNT as number_of_qa_responses_on_machine_field_identification ,
MACHINE_CORRECT_FIELD_ID_COUNT as number_of_qa_correct_responses_on_machine_field_identification,
FIELDS_INC_MACHINE_TRANSCRIBED_COUNT as number_of_incremental_fields_machine_transcribed,
ORGANIZE_DOCS_TASKS_COUNT as organize_documents_number_of_tasks_performed,
ORGANIZE_DOCS_PAGES_SHOWN_COUNT as organize_documents_number_of_pages_shown_in_step_1,
ORGANIZE_DOCS_PAGES_PROCESSED_COUNT as organize_documents_number_of_pages_categorized_in_step_1,
ORGANIZE_DOCS_DOCS_CREATED_COUNT as organize_documents_number_of_documents_created_in_step_1,
ORGANIZE_DOCS_DOCS_COUNT as organize_documents_number_of_documents_outputted_in_step_1,
ORGANIZE_DOCS_FOLDERS_COUNT as organize_documents_number_of_folders_created_in_step_2,
LIVE_LAYOUTS_COUNT as number_of_live_layouts,
FORMS_VERSION as software_version,
MACHINE_MATCH_NLC_PAGE_COUNT as number_of_correct_machine_predicted_non_structured_pages,
HUMAN_MATCH_NLC_PAGE_COUNT as number_of_incorrect_machine_predicted_non_structured_pages,
RELEASES_COUNT as number_of_releases,
ARCHIVED_RELEASES_COUNT as number_of_archived_releases,
LAYOUTS_COUNT as number_of_layouts,
ARCHIVED_LAYOUTS_COUNT as number_of_archived_layouts,
LAYOUT_VERSIONS_COUNT as number_of_layout_versions,
LIVE_IDP_FLOWS_COUNT as number_of_release_deploys,
CELLS_CREATED_COUNT as number_of_table_cells_created,
CELLS_COMPLETED_COUNT as number_of_table_cells_completed,
MACHINE_IDENTIFIED_CELLS_COUNT as number_of_table_cells_machine_identified,
HUMAN_IDENTIFIED_CELLS_COUNT as number_of_cells_manually_identified,
MANUAL_CHECKED_NLC_PAGE_COUNT as qa_responses_on_manual_non_structured_classification,
MANUAL_CORRECT_NLC_PAGE_COUNT as qa_correct_responses_on_manual_non_structured_classification,
SYSTEM_CHECKED_NLC_PAGE_COUNT as qa_responses_on_system_non_structured_classification,
SYSTEM_CORRECT_NLC_PAGE_COUNT as qa_correct_responses_on_system_non_structured_classification,
MACHINE_CHECKED_NLC_PAGE_COUNT as qa_responses_on_machine_non_structured_classification,
MACHINE_CORRECT_NLC_PAGE_COUNT as qa_correct_responses_on_machine_non_structured_classification,
FLEEX_TRANSCRIBED_FIELDS_COUNT as number_of_fields_extracted_in_flexible_extraction,
FLEEX_TRANSCRIBED_TABLE_CELLS_COUNT as number_of_table_cells_extracted_in_flexible_extraction,
CUSTOM_SUPERVISION_TRANSCRIBED_FIELDS_COUNT as number_of_fields_extracted_in_custom_supervision,
split_part(report_id,'_',0) as customer_int,
CASE WHEN customer_int = 'irc-prod' then 'IRC'
     WHEN customer_int = 'promomash-prod' then 'Promomash'
     WHEN customer_int in ('transflo-prod') then 'Transflo prod'
     when customer_int in ('rtstransflo-prod') then 'RTS Transflo prod'
     WHEN customer_int = 'benefitmall-prod' then 'Benefit Mall'
     WHEN customer_int = 'kovack-prod' then 'Kovack'
     WHEN customer_int = 'missionunderwriters-prod' then 'Mission Underwriters'
     WHEN customer_int = 'cifinancial-prod' then 'CI Financial'
     when customer_int = 'lossexpress-prod' then 'Loss Express'
     when customer_int = 'vault-prod' then 'Vault'
     when customer_int = 'resound-prod' then 'ReSound'
     when customer_int = 'navix-prod' then 'Navix'
     when customer_int = 'momentum' then 'Momentum SaaS'
     when customer_int = 'carmax-prod' then 'CarMax'
     when customer_int = 'outgo-prod' then 'OutGo'
     when customer_int = 'cleanharbors-prod' then 'Clean Harbors' 
     when customer_int = 'harborcompliance-prod' then 'Harbor Compliance'
     when customer_int = 'sentryfunding-prod' then 'Sentry Funding'
     when customer_int = 'gac-prod' then 'Gulf Agency Company'
     when customer_int = 'sahomeloans-prod' then 'SA Home Loans'
     when customer_int = 'stryker-prod' then 'Stryker'
     when customer_int = 'compiq-prod' then 'CompIQ'
     when customer_int = 'mems-prod' then 'MEMS'
     when customer_int = 'worldgroup-prod' then 'World Shipping'
     when customer_int = 'rts2transflo-prod' then 'RTS2 Transflo prod'
     ELSE 'non-prod' end as customer 
from "RAW"."USAGE_REPORTING"."SAAS_PROD"
where customer_int in ('irc-prod','promomash-prod','transflo-prod','benefitmall-prod','kovack-prod','missionunderwriters-prod','cifinancial-prod',
                       'lossexpress-prod','vault-prod','resound-prod','navix-prod','rtstransflo-prod','momentum','carmax-prod','outgo-prod','cleanharbors-prod',
                       'harborcompliance-prod','sentryfunding-prod','gac-prod','sahomeloans-prod','stryker-prod','compiq-prod','mems-prod','worldgroup-prod','rts2transflo-prod')
order by customer, period_start asc
),

backfill as (
select 
REPORT_ID,
to_timestamp(to_date(PERIOD_START)) as period_start,
SUBMISSIONS_CREATED_COUNT as number_of_submissions_created,
SUBMISSIONS_COUNT as number_of_submissions_completed,
FORMS_CREATED_COUNT as number_of_documents_created,
FORMS_COUNT as number_of_documents_completed,
SUBMISSION_PAGE_CREATED_COUNT as number_of_pages_created,
SUBMISSION_PAGE_COUNT as number_of_pages_completed,
PAGES_CREATED_STRUCTURED_COUNT as number_of_pages_matched_to_form_layouts_created,
PAGES_STRUCTURED_COUNT as number_of_pages_matched_to_form_layouts_completed,
PAGES_CREATED_VARIABLE_COUNT as number_of_pages_matched_to_flex_layouts_created,
PAGES_VARIABLE_COUNT as number_of_pages_matched_to_flex_layouts_completed,
PAGES_WITH_FIELDS_CREATED_COUNT as number_of_pages_with_fields_on_them_created,
PAGES_WITH_FIELDS_COUNT as number_of_pages_with_fields_on_them_completed,
FIELDS_CREATED_COUNT as number_of_fields_created,
FIELDS_COMPLETED_COUNT as number_of_fields_completed,
TRANSCRIPTION_CHARACTERS_COUNT as number_of_characters_completed,
LOGIN_COUNT as seats,
SUBMISSIONS_FILES_CREATED_COUNT as number_of_filed_submitted,
MACHINE_TRANSCRIBED_ENTRIES_COUNT as number_of_fields_machine_transcribed,
HUMAN_TRANSCRIBED_ENTRIES_COUNT as number_of_fields_manually_transcribed,
MACHINE_IDENTIFIED_FIELDS_COUNT as number_of_fields_machine_identified,
HUMAN_IDENTIFIED_FIELDS_COUNT as number_of_fields_manually_indentified,
PAGES_COUNT_MACHINE_SUM as number_of_pages_classified_automatically,
PAGES_COUNT_MANUAL_SUM as number_of_pages_classified_manually,
TEMPLATE_COUNT_MATCH as number_of_unique_layouts_matched,
MANUAL_CHECKED_TRANSCRIPTIONS_COUNT as number_of_qa_responses_on_manual_transcription,
MANUAL_CORRECT_TRANSCRIPTIONS_COUNT as number_of_qa_correct_responses_on_manual_transcription,
SYSTEM_CHECKED_TRANSCRIPTIONS_COUNT as number_of_qa_responses_on_system_transcription,
SYSTEM_CORRECT_TRANSCRIPTIONS_COUNT as number_of_qa_correct_responses_on_system_transcription,
MACHINE_CHECKED_TRANSCRIPTIONS_COUNT as number_of_qa_responses_on_machine_transcription,
MACHINE_CORRECT_TRANSCRIPTIONS_COUNT as number_of_qa_corect_responses_on_machine_transcription,
MANUAL_CHECKED_FIELD_ID_COUNT as number_of_qa_responses_on_manual_field_identification,
MANUAL_CORRECT_FIELD_ID_COUNT as number_of_qa_correct_responses_on_manual_field_identification,
SYSTEM_CHECKED_FIELD_ID_COUNT as number_of_qa_responses_on_system_field_identification,
SYSTEM_CORRECT_FIELD_ID_COUNT as number_of_qa_correct_responses_on_system_field_identification,
MACHINE_CHECKED_FIELD_ID_COUNT as number_of_qa_responses_on_machine_field_identification ,
MACHINE_CORRECT_FIELD_ID_COUNT as number_of_qa_correct_responses_on_machine_field_identification,
FIELDS_INC_MACHINE_TRANSCRIBED_COUNT as number_of_incremental_fields_machine_transcribed,
ORGANIZE_DOCS_TASKS_COUNT as organize_documents_number_of_tasks_performed,
ORGANIZE_DOCS_PAGES_SHOWN_COUNT as organize_documents_number_of_pages_shown_in_step_1,
ORGANIZE_DOCS_PAGES_PROCESSED_COUNT as organize_documents_number_of_pages_categorized_in_step_1,
ORGANIZE_DOCS_DOCS_CREATED_COUNT as organize_documents_number_of_documents_created_in_step_1,
ORGANIZE_DOCS_DOCS_COUNT as organize_documents_number_of_documents_outputted_in_step_1,
ORGANIZE_DOCS_FOLDERS_COUNT as organize_documents_number_of_folders_created_in_step_2,
LIVE_LAYOUTS_COUNT as number_of_live_layouts,
FORMS_VERSION as software_version,
MACHINE_MATCH_NLC_PAGE_COUNT as number_of_correct_machine_predicted_non_structured_pages,
HUMAN_MATCH_NLC_PAGE_COUNT as number_of_incorrect_machine_predicted_non_structured_pages,
RELEASES_COUNT as number_of_releases,
ARCHIVED_RELEASES_COUNT as number_of_archived_releases,
LAYOUTS_COUNT as number_of_layouts,
ARCHIVED_LAYOUTS_COUNT as number_of_archived_layouts,
LAYOUT_VERSIONS_COUNT as number_of_layout_versions,
LIVE_IDP_FLOWS_COUNT as number_of_release_deploys,
CELLS_CREATED_COUNT as number_of_table_cells_created,
CELLS_COMPLETED_COUNT as number_of_table_cells_completed,
MACHINE_IDENTIFIED_CELLS_COUNT as number_of_table_cells_machine_identified,
HUMAN_IDENTIFIED_CELLS_COUNT as number_of_cells_manually_identified,
MANUAL_CHECKED_NLC_PAGE_COUNT as qa_responses_on_manual_non_structured_classification,
MANUAL_CORRECT_NLC_PAGE_COUNT as qa_correct_responses_on_manual_non_structured_classification,
SYSTEM_CHECKED_NLC_PAGE_COUNT as qa_responses_on_system_non_structured_classification,
SYSTEM_CORRECT_NLC_PAGE_COUNT as qa_correct_responses_on_system_non_structured_classification,
MACHINE_CHECKED_NLC_PAGE_COUNT as qa_responses_on_machine_non_structured_classification,
MACHINE_CORRECT_NLC_PAGE_COUNT as qa_correct_responses_on_machine_non_structured_classification,
FLEEX_TRANSCRIBED_FIELDS_COUNT as number_of_fields_extracted_in_flexible_extraction,
FLEEX_TRANSCRIBED_TABLE_CELLS_COUNT as number_of_table_cells_extracted_in_flexible_extraction,
CUSTOM_SUPERVISION_TRANSCRIBED_FIELDS_COUNT as number_of_fields_extracted_in_custom_supervision,
split_part(report_id,'_',0) as customer_int,
CASE WHEN customer_int = 'irc-prod' then 'IRC'
     WHEN customer_int = 'promomash-prod' then 'Promomash'
     WHEN customer_int in ('transflo-prod') then 'Transflo prod'
     when customer_int in ('rtstransflo-prod') then 'RTS Transflo prod'
     WHEN customer_int = 'benefitmall-prod' then 'Benefit Mall'
     WHEN customer_int = 'kovack-prod' then 'Kovack'
     ELSE 'non-prod' end as customer
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."SAAS_USAGE_BACKFILL"
order by customer, period_start asc
),

transflo_backfill as (
select 
REPORT_ID,
to_timestamp(to_date(PERIOD_START)) as period_start,
SUBMISSIONS_CREATED_COUNT as number_of_submissions_created,
SUBMISSIONS_COUNT as number_of_submissions_completed,
FORMS_CREATED_COUNT as number_of_documents_created,
FORMS_COUNT as number_of_documents_completed,
SUBMISSION_PAGE_CREATED_COUNT as number_of_pages_created,
SUBMISSION_PAGE_COUNT as number_of_pages_completed,
PAGES_CREATED_STRUCTURED_COUNT as number_of_pages_matched_to_form_layouts_created,
PAGES_STRUCTURED_COUNT as number_of_pages_matched_to_form_layouts_completed,
PAGES_CREATED_VARIABLE_COUNT as number_of_pages_matched_to_flex_layouts_created,
PAGES_VARIABLE_COUNT as number_of_pages_matched_to_flex_layouts_completed,
PAGES_WITH_FIELDS_CREATED_COUNT as number_of_pages_with_fields_on_them_created,
PAGES_WITH_FIELDS_COUNT as number_of_pages_with_fields_on_them_completed,
FIELDS_CREATED_COUNT as number_of_fields_created,
FIELDS_COMPLETED_COUNT as number_of_fields_completed,
TRANSCRIPTION_CHARACTERS_COUNT as number_of_characters_completed,
LOGIN_COUNT as seats,
SUBMISSIONS_FILES_CREATED_COUNT as number_of_filed_submitted,
MACHINE_TRANSCRIBED_ENTRIES_COUNT as number_of_fields_machine_transcribed,
HUMAN_TRANSCRIBED_ENTRIES_COUNT as number_of_fields_manually_transcribed,
MACHINE_IDENTIFIED_FIELDS_COUNT as number_of_fields_machine_identified,
HUMAN_IDENTIFIED_FIELDS_COUNT as number_of_fields_manually_indentified,
PAGES_COUNT_MACHINE_SUM as number_of_pages_classified_automatically,
PAGES_COUNT_MANUAL_SUM as number_of_pages_classified_manually,
TEMPLATE_COUNT_MATCH as number_of_unique_layouts_matched,
MANUAL_CHECKED_TRANSCRIPTIONS_COUNT as number_of_qa_responses_on_manual_transcription,
MANUAL_CORRECT_TRANSCRIPTIONS_COUNT as number_of_qa_correct_responses_on_manual_transcription,
SYSTEM_CHECKED_TRANSCRIPTIONS_COUNT as number_of_qa_responses_on_system_transcription,
SYSTEM_CORRECT_TRANSCRIPTIONS_COUNT as number_of_qa_correct_responses_on_system_transcription,
MACHINE_CHECKED_TRANSCRIPTIONS_COUNT as number_of_qa_responses_on_machine_transcription,
MACHINE_CORRECT_TRANSCRIPTIONS_COUNT as number_of_qa_corect_responses_on_machine_transcription,
MANUAL_CHECKED_FIELD_ID_COUNT as number_of_qa_responses_on_manual_field_identification,
MANUAL_CORRECT_FIELD_ID_COUNT as number_of_qa_correct_responses_on_manual_field_identification,
SYSTEM_CHECKED_FIELD_ID_COUNT as number_of_qa_responses_on_system_field_identification,
SYSTEM_CORRECT_FIELD_ID_COUNT as number_of_qa_correct_responses_on_system_field_identification,
MACHINE_CHECKED_FIELD_ID_COUNT as number_of_qa_responses_on_machine_field_identification ,
MACHINE_CORRECT_FIELD_ID_COUNT as number_of_qa_correct_responses_on_machine_field_identification,
FIELDS_INC_MACHINE_TRANSCRIBED_COUNT as number_of_incremental_fields_machine_transcribed,
ORGANIZE_DOCS_TASKS_COUNT as organize_documents_number_of_tasks_performed,
ORGANIZE_DOCS_PAGES_SHOWN_COUNT as organize_documents_number_of_pages_shown_in_step_1,
ORGANIZE_DOCS_PAGES_PROCESSED_COUNT as organize_documents_number_of_pages_categorized_in_step_1,
ORGANIZE_DOCS_DOCS_CREATED_COUNT as organize_documents_number_of_documents_created_in_step_1,
ORGANIZE_DOCS_DOCS_COUNT as organize_documents_number_of_documents_outputted_in_step_1,
ORGANIZE_DOCS_FOLDERS_COUNT as organize_documents_number_of_folders_created_in_step_2,
LIVE_LAYOUTS_COUNT as number_of_live_layouts,
FORMS_VERSION as software_version,
MACHINE_MATCH_NLC_PAGE_COUNT as number_of_correct_machine_predicted_non_structured_pages,
HUMAN_MATCH_NLC_PAGE_COUNT as number_of_incorrect_machine_predicted_non_structured_pages,
RELEASES_COUNT as number_of_releases,
ARCHIVED_RELEASES_COUNT as number_of_archived_releases,
LAYOUTS_COUNT as number_of_layouts,
ARCHIVED_LAYOUTS_COUNT as number_of_archived_layouts,
LAYOUT_VERSIONS_COUNT as number_of_layout_versions,
LIVE_IDP_FLOWS_COUNT as number_of_release_deploys,
CELLS_CREATED_COUNT as number_of_table_cells_created,
CELLS_COMPLETED_COUNT as number_of_table_cells_completed,
MACHINE_IDENTIFIED_CELLS_COUNT as number_of_table_cells_machine_identified,
HUMAN_IDENTIFIED_CELLS_COUNT as number_of_cells_manually_identified,
MANUAL_CHECKED_NLC_PAGE_COUNT as qa_responses_on_manual_non_structured_classification,
MANUAL_CORRECT_NLC_PAGE_COUNT as qa_correct_responses_on_manual_non_structured_classification,
SYSTEM_CHECKED_NLC_PAGE_COUNT as qa_responses_on_system_non_structured_classification,
SYSTEM_CORRECT_NLC_PAGE_COUNT as qa_correct_responses_on_system_non_structured_classification,
MACHINE_CHECKED_NLC_PAGE_COUNT as qa_responses_on_machine_non_structured_classification,
MACHINE_CORRECT_NLC_PAGE_COUNT as qa_correct_responses_on_machine_non_structured_classification,
FLEEX_TRANSCRIBED_FIELDS_COUNT as number_of_fields_extracted_in_flexible_extraction,
FLEEX_TRANSCRIBED_TABLE_CELLS_COUNT as number_of_table_cells_extracted_in_flexible_extraction,
CUSTOM_SUPERVISION_TRANSCRIBED_FIELDS_COUNT as number_of_fields_extracted_in_custom_supervision,
split_part(report_id,'_',0) as customer_int,
CASE WHEN customer_int = 'irc-prod' then 'IRC'
     WHEN customer_int = 'promomash-prod' then 'Promomash'
     WHEN customer_int in ('transflo-prod') then 'Transflo prod'
     when customer_int in ('rtstransflo-prod') then 'RTS Transflo prod'
     WHEN customer_int = 'benefitmall-prod' then 'BenefitMall'
     WHEN customer_int = 'kovack-prod' then 'Kovack'
     ELSE 'non-prod' end as customer
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."TRANSFLO_PROD_BACKFILL"
order by customer, period_start asc
),

fct_saas_usage as (
select * from raw_data
UNION 
select * from backfill
UNION 
select * from transflo_backfill
order by customer, period_start asc 
)

select * from fct_saas_usage

