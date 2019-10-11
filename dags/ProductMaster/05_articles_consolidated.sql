with
  articles_actual as ( select * from `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Articles`),
  articles_usage as ( select * from `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Articles_usage` where DOI not in (select DOI from articles_actual ) )

select * from articles_actual
union distinct
select * from articles_usage
