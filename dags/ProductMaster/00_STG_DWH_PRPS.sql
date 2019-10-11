select
-- business key
  a.posid as psp,
  substr( a.posid, 0, 1) as pspkind,
  case
    when substr( a.posid, 0, 1) in ('B', 'T', 'L') then 'BO'
    when substr( a.posid, 0, 1) in ('S') then 'JR'
    else 'SO'
  end as sbz,
-- book master data
  if(substr( a.posid, 0, 1) in ('B', 'T', 'L'), a.usr00, '') as isbn,
  if(substr( a.posid, 0, 1) in ('B', 'T', 'L'), a.usr02, '') as book,
-- journal master data
  if(substr( a.posid, 0, 1) in ('S'), a.usr00, '') as issn,
  if(substr( a.posid, 0, 1) in ('S'), ltrim(substr(a.posid, 6, 6), '0'), '') as jsnr,
  if(substr( a.posid, 0, 1) in ('S'), ltrim(substr(a.posid, 12, 4), '0'), '') as volume,
  if(substr( a.posid, 0, 1) in ('S'), ltrim(substr(a.posid, 16, 3), '0'), '') as issue, -- that's tricky, not yet completed
-- general
  substr( a.posid, 2, 4) as comp_code,
  ltrim(substr( a.usr03, 5, 4), '0') as responsible,
  b.source_entity as entity,
  case
    when substr( a.posid, 2, 4) = b.source_entity then 'Y'
    else 'N'
  end as IPOWNER,
  a.zzplaner,
  a.zzplanername
FROM `usage-data-reporting.DEV_LDZ_ProductMaster.erp_prps` as a
left join `usage-data-reporting.DEV_LDZ_ProductMaster.texts_gpu` as b on ltrim(substr( a.usr03, 5, 4), '0') = cast(b.PS_CODE as string)
--where
--a.usr00 <> ''
--a.usr00 = '978-3-658-06037-4'