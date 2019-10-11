with
  chapters_actual as ( select * from `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Chapters`),
  chapters_usage as ( select * from `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Chapters_usage` where ItemDOI not in (select ItemDOI from chapters_actual ) )

select * from chapters_actual
union distinct
select * from chapters_usage
