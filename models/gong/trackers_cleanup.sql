{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'GONG'
)
}}

with trackers_int as (
select distinct
t.tracker_id,
t.name,
CASE WHEN t.name ilike 'AWS%' then 'AWS'
     WHEN t.name ilike 'Claims Processing%' then 'Claims Processing'
     when t.name ilike 'Cloud%' then 'Cloud'
     when t.name ilike 'Collation%' then 'Collation'
     when (t.name ilike 'Competitors%' or t.name in ('Abbyy','Instabase','Insta Base','Kofax','MuleSoft','Ocrolus')) then 'Competition'
     when (t.name ilike 'Doc Types & Process-General%' or t.name ilike 'Document Types & Processes%') then 'Document Types and Processes'
     when t.name ilike 'Fin Serv Docs & Process%' then 'Financial Services'
     when t.name ilike 'Healthcare Docs & process%' then 'Healthcare'
     when t.name ilike 'Insurance Docs & Process%' then 'Insurance'
     when t.name ilike 'Knowledge Workers%' then 'Knowledge Workers'
     when t.name ilike 'Legal Docs & Processes%' then 'Legal'
     when t.name ilike 'Loan Origination%' then 'Loan Origination'
     when t.name ilike 'Logistics Docs and Process%' then 'Logistics'
     when t.name ilike 'Mobile%' then 'Mobile'
     when t.name ilike 'Mortgage Docs and Process%' then 'Mortgage Docs'
     when t.name ilike 'OCR%' then 'OCR'
     when t.name ilike 'OEM%' then 'OEM'
     when t.name ilike 'Objections%' then 'Objections'
     when t.name ilike 'Partnerships%' then 'Partnerships'
     when t.name ilike 'Pharma docs & process%' then 'Pharma'
     when t.name ilike 'SaaS Offering%' then 'SaaS'
     when t.name ilike 'Tables%' then 'Tables'
     when t.name ilike 'Verticalization%' then 'Verticalization: P&C Lending'
     when t.name ilike 'Gvm%' then 'Government'
     when t.name ilike 'Kubernetes%' then 'Kubernetes'
     when t.name ilike 'Models%' then 'Models'
     when t.name ilike 'Supporting Docs%' then 'Supporting Docs'
     when t.name ilike 'Unstructured%' then 'Unstructured'
     when t.name ilike '%- MEDDIC Questions' then 'MEDDIC'
     ELSE t.name end as tracker_name,
trim(split_part(lower(tracker_name),'/',1)) as name_int
from "GONG"."HYPERSCIENCE_GONG"."TRACKERS" as t
order by name
),

trackers_int2 as (
select distinct
name_int,
count(name_int) as num_trackers
from trackers_int
where tracker_name not in ('Competition')
group by name_int
order by num_trackers asc, name_int
)

select * from trackers_int2
where num_trackers = 1

