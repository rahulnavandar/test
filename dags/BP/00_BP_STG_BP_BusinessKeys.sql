  SELECT
    PARTNER
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but0id`
  WHERE
    TYPE = 'ZGR'
  UNION DISTINCT
  SELECT
    PARTNER
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_zbutmetapress`
  UNION DISTINCT
  SELECT
    PARTNER
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but0id`
  WHERE
    TYPE = 'ZGR'
  UNION DISTINCT
  SELECT
    PARTNER
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but000`
  UNION DISTINCT
  SELECT
    PARTNER
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but000`
UNION DISTINCT
SELECT DISTINCT UsageBusinessPartner FROM `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.BP_Usage` 