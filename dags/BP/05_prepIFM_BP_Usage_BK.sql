create or replace table `{{ params.project }}.{{ params.environment }}_DWH_Usage.BusinessPartner_usageBK`
as
select distinct UsageBusinessPartner as partner  from
`{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Slink_BP_Base_Reporting_Monthly` slink where slink.UsageBusinessPartner != ""
UNION DISTINCT
select distinct UsageBusinessPartner as partner from
`{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Magnus_BP_Base_Reporting_Monthly` mag where mag.UsageBusinessPartner != ""
UNION DISTINCT
select distinct UsageBusinessPartner as partner from
`{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Database_BP_Base_Reporting_Monthly` dat where dat.UsageBusinessPartner != ""
UNION DISTINCT
select distinct UsageBusinessPartner as partner from
`{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Nature_BP_Base_Reporting_Monthly` nature where nature.UsageBusinessPartner != ""