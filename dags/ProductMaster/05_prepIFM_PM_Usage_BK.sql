create or replace table `{{ params.project }}.{{ params.environment }}_DWH_Usage.ProductMaster_usageBK`
as
select distinct
  usageitemdoi as pmkey,
  'SLINK' as srcTable,
  case
    when usagepublishertype = 'ARTICLE' then 'ARTICLE'
    else 'CHAPTER'
  end as doiType,
  case
    when usagepublishertype != 'ARTICLE' then usageitemdoi
    else null
  end as chapterDOI,
  case
    when usagepublishertype = 'ARTICLE' then usageitemdoi
    else null
  end as articleDOI
FROM `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Slink_Material_Base_Reporting_Monthly`
union distinct
select distinct
  usageitemdoi as pmkey,
  'Nature' as srcTable,
  'ARTICLE' as doiType,
  '' as chapterDOI,
  case
    when usagepublishertype = 'ARTICLE' then usageitemdoi
    else null
  end as articleDOI  FROM `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Nature_Material_Base_Reporting_Monthly`
union distinct
select distinct
  usageitemdoi as pmkey,
  'Magnus' as srcTable,
  case
    when usagepublishertype = 'ARTICLE' then 'ARTICLE'
    else 'CHAPTER'
  end as doiType,
  case
    when usagepublishertype != 'ARTICLE' then usageitemdoi
    else null
  end as chapterDOI,
  case
    when usagepublishertype = 'ARTICLE' then usageitemdoi
    else null
  end as articleDOI FROM `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Magnus_Material_Base_Reporting_Monthly`
