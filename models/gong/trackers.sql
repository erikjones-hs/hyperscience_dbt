{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'GONG'
)
}}

with tracker_cleanup as (
select distinct name_int from {{ref('trackers_cleanup')}}
),

trackers_int as (
select distinct 
ct.CONVERSATION_KEY,
ct.TRACKER_ID,
t.NAME as tracker_name,
t.tracker_type,
t.keywords,
CASE WHEN t.name ilike 'AWS%' then 'AWS'
     WHEN t.name ilike 'Claims Processing%' then 'Claims Processing'
     when t.name ilike 'Cloud%' then 'Cloud'
     when t.name ilike 'Collation%' then 'Collation'
     when (t.name ilike 'Competitors%' or t.name in ('Abbyy','Instabase','Insta Base','Kofax','MuleSoft','Ocrolus')) then 'Competition'
     when (t.name ilike 'Doc Types & Process-General%' or t.name ilike 'Document Types & Processes%') then 'Document Types and Processes'
     when t.name ilike 'Fin Serv Docs & Process%' then 'Financial Services Docs & Processes'
     when t.name ilike 'Healthcare Docs & process%' then 'Healthcare Docs & Processes'
     when t.name ilike 'Insurance Docs & Process%' then 'Insurance Docs & Processes'
     when t.name ilike 'Knowledge Workers%' then 'Knowledge Workers'
     when t.name ilike 'Legal Docs & Processes%' then 'Legal Docs & Processes'
     when t.name ilike 'Loan Origination%' then 'Loan Origination'
     when t.name ilike 'Logistics Docs and Process%' then 'Logistics Docs & Processes'
     when t.name ilike 'Mobile%' then 'Mobile'
     when t.name ilike 'Mortgage Docs and Process%' then 'Mortgage Docs & Processes'
     when t.name ilike 'OCR%' then 'OCR'
     when t.name ilike 'OEM%' then 'OEM'
     when t.name ilike 'Objections%' then 'Objections'
     when t.name ilike 'Partnerships%' then 'Partnerships'
     when t.name ilike 'Pharma docs & process%' then 'Pharma Docs & Processes'
     when t.name ilike 'SaaS Offering%' then 'SaaS'
     when t.name ilike 'Tables%' then 'Tables'
     when t.name ilike 'Verticalization%' then 'Verticalization: P&C Lending'
     when t.name ilike 'Gvm%' then 'Government Docs & Processes'
     when t.name ilike 'Kubernetes%' then 'Kubernetes'
     when t.name ilike 'Models%' then 'Models'
     when t.name ilike 'Supporting Docs%' then 'Supporting Docs'
     when t.name ilike 'Unstructured%' then 'Unstructured'
     when t.name ilike '%- MEDDIC Questions' then 'MEDDIC'
     ELSE t.name end as tracker_category,
split_part(lower(t.name),'/',-1) as tracker_specific_int,
CASE when trim(tracker_specific_int) in ('bad scan','bad scans') then 'bad scan'
     when trim(tracker_specific_int) in ('bank statement', 'bank statements') then 'bank statements'
     when trim(tracker_specific_int) in ('claim','claim form') then 'claim'
     when trim(tracker_specific_int) in ('concerned', 'concern','concerns','my concern') then 'concerns'
     when trim(tracker_specific_int) in ('envelope','envelopes') then 'envelope'
     when trim(tracker_specific_int) in ('foldering','folders') then 'folders'
     when trim(tracker_specific_int) in ('grid','griddable') then 'grid'
     when trim(tracker_specific_int) in ('group','grouping','groups') then 'groups'
     when trim(tracker_specific_int) in ('hcfa','hcfas') then 'hcfa'
     when trim(tracker_specific_int) in ('index','indexing') then 'index'
     when trim(tracker_specific_int) in ('legal','legality') then 'legal'
     when trim(tracker_specific_int) in ('mortgages','mortgage loan') then 'mortgage'
     when trim(tracker_specific_int) in ('passport','passports') then 'passports'
     when trim(tracker_specific_int) in ('struggle','struggling','struggling to understand') then 'struggle'
     when trim(tracker_specific_int) in ('to be blunt','to be frank','frankly','to be honest') then 'to be blunt'
     when trim(tracker_specific_int) in ('w-2', 'w2') then 'w2'
     when trim(tracker_specific_int) in ('app', 'application') then 'app'
     when trim(tracker_specific_int) in ('change of address', 'address change') then 'address change'
     when trim(tracker_specific_int) in ('doc types & process-general', 'document types & processes') then 'document types & process'
     when trim(tracker_specific_int) in ('eob', 'explanation of benefits') then 'explanation of benefits'
     when trim(tracker_specific_int) in ('fnol', 'first notice of loss') then 'first notice of loss'
     when trim(tracker_specific_int) in ('fraud', 'fraud detection') then 'fraud'
     when trim(tracker_specific_int) in ('pega', 'pegasystems') then 'pega systems'
     when trim(tracker_specific_int) in ('saas', 'saas offering') then 'saas'
     when trim(tracker_specific_int) in ('supporting docs', 'supporting documents','supporting materials') then 'supporting documents'
     when trim(tracker_specific_int) in ('unstructured', 'unstructured mentions') then 'unstructured'
     when trim(tracker_specific_int) in ('waste time', 'waste your time','wasting your time','wasting our time') then 'waste time'
     when trim(tracker_specific_int) in ('ibm', 'ibm datacap','datacap') then 'ibm datacap'
     when trim(tracker_specific_int) in ('kyc', 'know your customer') then 'know your customer'
     when trim(tracker_specific_int) in ('cloud', 'cloud offering') then 'cloud'
     else trim(tracker_specific_int) end as tracker_specific, 
ct.COUNT as tracker_count -- count of tracker mentions in a call
from GONG.HYPERSCIENCE_GONG.CONVERSATION_TRACKERS ct -- trackers in calls
join GONG.HYPERSCIENCE_GONG.TRACKERS t on (ct.TRACKER_ID = t.TRACKER_ID)
),

trackers_int2 as (
select distinct
conversation_key,
tracker_id,
tracker_name,
tracker_type,
keywords,
CASE WHEN lower(tracker_name) in (select distinct name_int from tracker_cleanup) then tracker_name else tracker_category end as tracker_category,
CASE WHEN lower(tracker_name) in (select distinct name_int from tracker_cleanup) then tracker_name else tracker_specific end as tracker_specific,
tracker_count,
CASE WHEN (lower(tracker_name) in (select distinct name_int from tracker_cleanup) or tracker_category in ('Competition','MEDDIC')) then 1 else 0 end as keep_flag, 
CASE WHEN contains(tracker_name, '/') then 1 else 0 end as has_slash_flag
from trackers_int
),

fct_trackers as (
select distinct
conversation_key,
tracker_id,
tracker_name,
tracker_type,
keywords,
tracker_category,
tracker_specific,
tracker_count
from trackers_int2
where (keep_flag = 1 or has_slash_flag = 1) 
)

select * from fct_trackers 

