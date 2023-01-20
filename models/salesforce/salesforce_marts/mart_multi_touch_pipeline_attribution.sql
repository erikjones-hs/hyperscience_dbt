WITH
  "dim_campaigns" AS (
    SELECT
      *,
      SPLIT_PART(NAME, ' - ', -3) AS campaign_region,
      SPLIT_PART(NAME, ' - ', -2) AS campaign_type,
      SPLIT_PART(NAME, ' - ', -1) AS campaign_name
    FROM
      fivetran_database.salesforce.campaign
    WHERE
      is_deleted = FALSE
      AND NAME LIKE '%REGIONAL%'
      OR NAME LIKE '%GLOBAL%'
  ),
  "dim_leads" AS (
    SELECT
      *
    FROM
      prod.salesforce.dim_leads_with_owner
  ),
  "dim_campaign_member" AS (
    SELECT
      id,
      lead_id,
      contact_id,
      campaign_id,
      created_date,
      has_responded,
      status,
      first_responded_date,
      TYPE
    FROM
      fivetran_database.salesforce.campaign_member
    WHERE
      is_deleted = FALSE
      AND (
        lead_id IS NOT NULL
        OR contact_id IS NOT NULL
      )
  ),
  "dim_contacts" AS (
    SELECT
      *
    FROM
      prod.salesforce.dim_contacts_with_owner
  ),
  "fct_campaign_engagement" AS (
    SELECT
      cm.id,
      contact_id,
      lead_id,
      cm.first_responded_date AS created_date,
      CASE
        WHEN c.name LIKE '%Demo%' THEN 'Demo'
        WHEN c.name LIKE '%C_report%' THEN 'Report'
        WHEN c.name LIKE '%C_ebook%' THEN 'eBook'
        WHEN c.name LIKE '%Contact%' THEN 'Contact Us Request'
        WHEN c.name LIKE '%DR%' THEN 'Drift'
        WHEN c.type = 'Website' THEN 'Web Content'
        ELSE c.type
      END AS campaign_type,
      c.name AS campaign_name
    FROM
      "dim_campaign_member" cm
      LEFT JOIN "dim_leads" USING (lead_id)
      LEFT JOIN "dim_campaigns" c ON cm.campaign_id = c.id
    WHERE
      first_responded_date IS NOT NULL
      AND (
        c.type != 'Operational'
        OR c.name LIKE '% TP %'
      )
  ),
  "campaign_member_accounts" AS (
    WITH
      contact_accounts AS (
        SELECT
          a.id,
          a.lead_id,
          a.contact_id,
          b.account_id,
          b.sales_region,
          a.created_date,
          campaign_type,
          campaign_name
        FROM
          "fct_campaign_engagement" a
          LEFT JOIN "dim_contacts" b USING (contact_id)
        WHERE
          contact_id IS NOT NULL
      ),
      lead_accounts AS (
        SELECT
          a.id,
          a.lead_id,
          a.contact_id,
          b.account_id,
          b.sales_region,
          a.created_date,
          campaign_type,
          campaign_name
        FROM
          "fct_campaign_engagement" a
          LEFT JOIN "dim_leads" b USING (lead_id)
        WHERE
          contact_id IS NULL
      )
    SELECT
      *
    FROM
      contact_accounts
    UNION ALL
    SELECT
      *
    FROM
      lead_accounts
  ),
  "dim_opportunity" AS (
    SELECT
      *
    FROM
      prod.salesforce.dim_opportunity
    WHERE
      is_deleted = FALSE
  ),
  "account_opportunity_influence" AS (
    SELECT
      id,
      opp_id,
      opp_arr,
      b.created_date AS engagement_date,
      opp_created_dte,
      CASE
        WHEN opp_stage_name = 'Closed Won' THEN opp_closed_won_dte
        WHEN opp_stage_name = 'Closed Lost' THEN opp_dq_dte
        ELSE opp_close_dte
      END AS opp_close_dte,
      opp_stage_name,
      opp_revenue_type,
      opp_lead_source,
      CASE
        WHEN datediff('day', opp_created_dte, engagement_date) >= -365
        AND datediff('day', opp_created_dte, engagement_date) < 0 THEN 'Influenced'
        WHEN datediff('day', opp_created_dte, engagement_date) < -365 THEN 'Not Influenced'
        WHEN engagement_date > (
          CASE
            WHEN opp_stage_name = 'Closed Won' THEN opp_closed_won_dte
            WHEN opp_stage_name = 'Closed Lost' THEN opp_dq_dte
            ELSE opp_close_dte
          END
        ) THEN 'Not Influenced'
        ELSE 'Accelerated'
      END AS influence_type,
      campaign_type,
      campaign_name
    FROM
      "dim_opportunity" a
      LEFT JOIN "campaign_member_accounts" b USING (account_id)
    WHERE
      b.created_date IS NOT NULL
      AND account_id != '0011R000021BlB0QAK'
      AND account_id IS NOT NULL
  ),
  "dim_dates" AS (
    SELECT
      *
    FROM
      dev.marts.fy_calendar
  ),
  "fct_marketing_touched_opps" AS (
    SELECT
      *
    FROM
      "account_opportunity_influence"
    WHERE
      influence_type != 'Not Influenced'
  ),
  DATA AS (
    SELECT
      id,
      opp_id,
      opp_arr,
      engagement_date,
      opp_created_dte,
      fy_qtr_year AS opp_created_fy_qtr,
      opp_close_dte,
      opp_stage_name,
      opp_revenue_type,
      opp_lead_source,
      influence_type,
      campaign_type,
      campaign_name,
      div0(
        1,
        COUNT(id) OVER (
          PARTITION BY
            opp_id
        )
      ) AS mt_model
    FROM
      "fct_marketing_touched_opps"
      LEFT JOIN "dim_dates" ON DATE(opp_created_dte) = dte
    WHERE
      opp_revenue_type = 'New Customer'
  )
  
    SELECT
      *,
      mt_model * opp_arr AS mt_model_arr,
      SUM(mt_model * opp_arr) OVER (
        PARTITION BY
          campaign_type
      ) AS mt_model_arr_total
    FROM
      DATA