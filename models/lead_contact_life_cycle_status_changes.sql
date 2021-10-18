{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MARKETING'
)
}}

WITH SOURCE AS (
    
SELECT 
    
    PERSON_ID,
    LEAD_ID,
    CONTACT_ID,
    CREATED_DATE,
    MEL_DATE,
    MQL_DATE,
    SAL_DATE,
    SEL_DATE,
    SQL_DATE,
    CONVERTED_CONTACT_DATE,
    DQ_DATE,
    MQL_DATE_FROM_SAL

FROM {{ ref('salesforce_leads_and_contacts') }} LEAD
    
),

base_table AS (
    
SELECT PERSON_ID, LEAD_ID, CONTACT_ID, CREATED_DATE as date, 'Created' as STATUS_CHANGE FROM SOURCE
UNION
SELECT PERSON_ID, LEAD_ID, CONTACT_ID, MEL_DATE as date, 'MEL' as STATUS_CHANGE FROM SOURCE WHERE MEL_DATE IS NOT NULL
UNION
SELECT PERSON_ID, LEAD_ID, CONTACT_ID, MQL_DATE as date, 'MQL' as STATUS_CHANGE FROM SOURCE WHERE MQL_DATE IS NOT NULL
UNION
SELECT PERSON_ID, LEAD_ID, CONTACT_ID, SAL_DATE as date, 'SAL' as STATUS_CHANGE FROM SOURCE WHERE SAL_DATE IS NOT NULL 
UNION
SELECT PERSON_ID, LEAD_ID, CONTACT_ID, SEL_DATE as date, 'SEL' as STATUS_CHANGE FROM SOURCE WHERE SEL_DATE IS NOT NULL 
UNION
SELECT PERSON_ID, LEAD_ID, CONTACT_ID, CONVERTED_CONTACT_DATE as date, 'Converted' as STATUS_CHANGE FROM SOURCE WHERE CONVERTED_CONTACT_DATE IS NOT NULL
UNION
SELECT PERSON_ID, LEAD_ID, CONTACT_ID, SQL_DATE as date, 'SQL' as STATUS_CHANGE FROM SOURCE WHERE SQL_DATE IS NOT NULL
UNION
SELECT PERSON_ID, LEAD_ID, CONTACT_ID, DQ_DATE as date, 'DQ' as STATUS_CHANGE FROM SOURCE WHERE DQ_DATE IS NOT NULL
UNION
SELECT PERSON_ID, LEAD_ID, CONTACT_ID, MQL_DATE_FROM_SAL as date, 'MQL from SAL' as STATUS_CHANGE FROM SOURCE WHERE MQL_DATE_FROM_SAL IS NOT NULL

),

fct_status_stg as (
    SELECT
    PERSON_ID,
    LEAD_ID,
    CONTACT_ID,
    DATE(DATE) as DATE,
    STATUS_CHANGE,
    CASE WHEN lead_id is null then '1000000001' else lead_id end as lead_id_surrogate,
    CASE WHEN contact_id is null then '1000000001' else contact_id end as contact_id_surrogate,
    person_id||'-'||lead_id_surrogate||'-'||contact_id_surrogate||'-'||to_date(date)||'-'||status_change as id 
FROM base_table
ORDER BY STATUS_CHANGE
),

fct_status as (
    SELECT 
    id,
    person_id,
    lead_id,
    contact_id,
    date,
    status_change
    from fct_status_stg
)

select * from fct_status