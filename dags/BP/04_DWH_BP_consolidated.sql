with
  bp_actual as ( select * from `usage-data-reporting.DEV_DWH_BusinessPartner.BusinessPartner_backup`),
  bp_hist as ( select * from `usage-data-reporting.DEV_DWH_BusinessPartner.BusinessPartner_historical` where partner not in (select partner from bp_actual ) )

select * from bp_actual
union distinct
select * from bp_hist
