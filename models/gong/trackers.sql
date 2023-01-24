{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'GONG'
)
}}

with trackers as (
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
     when (t.name ilike 'Competitors%' or t.name in ('Abbyy','Instabase','Insta Base','Kofax','Mulesoft','Ocrolus')) then 'Competition'
     when (t.name ilike 'Doc Types & Process-General%' or t.name ilike 'Document Types & Processes%') then 'Document Types and Processes'
     when t.name ilike 'Fin Serv Docs & Process%' then 'Financial Services'
     when t.name ilike 'Healthcare Docs & process%' then 'Healthcare'
     when t.name ilike 'Insurance Docs & Process%' then 'Insurance'
     when t.name ilike 'Knowledge Workers%' then 'Knowledge Workers'
     when t.name ilike 'Legal Docs & Processes%' then 'Legal'
     when t.name ilike 'Loan Origination%' then 'Loan Origination'
     when t.name ilike 'Logistics Docs and Process%' then 'Logistics'
     when t.name ilike 'Mobile%' then 'Mobile'
     when t.name ilike 'Mortgage Docs and Processes%' then 'Mortgage'
     when t.name ilike 'OCR%' then 'OCR'
     when t.name ilike 'OEM%' then 'OEM'
     when t.name ilike 'Objections%' then 'Objections'
     when t.name ilike 'Partnerships%' then 'Partnerships'
     when t.name ilike 'Pharma docs & process%' then 'Pharma'
     when t.name ilike 'SaaS Offering%' then 'SaaS'
     when t.name ilike 'Tables%' then 'Tables'
     when t.name ilike 'Verticalization%' then 'Verticalization'
     when t.name ilike 'Gvm%' then 'Government'
     when t.name ilike 'Kubernetes%' then 'Kubernetes'
     when t.name ilike 'Models%' then 'Models'
     when t.name ilike 'Supporting Docs%' then 'Supporting Docs'
     when t.name ilike 'Unstructured%' then 'Unstructured'
     ELSE t.name end as tracker_category,
split_part(t.name,'/',-1) as tracker_specific,   
ct.COUNT as tracker_count -- count of tracker mentions in a call
from GONG.HYPERSCIENCE_GONG.CONVERSATION_TRACKERS ct -- trackers in calls
join GONG.HYPERSCIENCE_GONG.TRACKERS t on (ct.TRACKER_ID = t.TRACKER_ID)
)

select * from trackers