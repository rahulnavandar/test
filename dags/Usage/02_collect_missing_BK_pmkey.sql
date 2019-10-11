create or replace table `{{ params.project }}.{{ params.environment }}_DWH_Usage.bk_missing_pmkeys`
as
select a.* from `{{ params.project }}.{{ params.environment }}_DWH_Usage.ProductMaster_usageBK` as a
  left join `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Articles` as b
   on a.pmkey = b.doi
  left join `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Chapters` as c
   on a.pmkey = c.ItemDoi

 where b.doi is null and c.ItemDoi is null
