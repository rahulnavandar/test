create or replace table `{{ params.project }}.{{ params.environment }}_IFM_Usage.BusinessPartner_usageAggr`
as
select * from `{{ params.project }}.{{ params.environment }}_IFM_Usage.v_BusinessPartner_usageAggr`