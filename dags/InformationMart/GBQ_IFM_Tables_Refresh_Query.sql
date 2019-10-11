-------------------------------------------------------IFM Queries--------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------

--UsageProductMaster
Create or replace table `usage-data-reporting.DEV_IFM_Usage.UsageProductMaster` as
Select * from `usage-data-reporting.DEV_IFM_Usage.v_UsageProductMaster`

--UsageBPMaster
Create or replace table `usage-data-reporting.DEV_IFM_Usage.UsageBPMaster` as
Select * from `usage-data-reporting.DEV_IFM_Usage.v_UsageBPMaster`

--Usage By BP Daily SPL
Create or replace table `usage-data-reporting.DEV_IFM_Usage.mv_usage_bp_daily_spl` 
Partition by Event_Date
cluster by Event_Year, Event_Year_Month, BP_ID, BP_Name
as 
Select * from `usage-data-reporting.DEV_IFM_Usage.v_usage_bp_daily_spl`

--Usage By BP monthly SPL
Create or replace table `usage-data-reporting.DEV_IFM_Usage.mv_usage_bp_monthly_spl` 
Partition by Event_Date
cluster by Event_Year, Event_Year_Month, BP_ID, BP_Name
as 
Select * from `usage-data-reporting.DEV_IFM_Usage.v_usage_bp_monthly_spl` 

--Usage by BP Daily NAT
Create or replace table `usage-data-reporting.DEV_IFM_Usage.mv_usage_bp_daily_nat` 
partition by Event_Date
cluster by Event_Year, Event_Year_Month, UsageSiteID
as
Select * from `usage-data-reporting.DEV_IFM_Usage.v_usage_bp_daily_nat`

--Usage by BP Monthly Nat
Create or replace table `usage-data-reporting.DEV_IFM_Usage.mv_usage_bp_monthly_nat` 
partition by Event_Date
cluster by Event_Year, Event_Year_Month, UsageSiteID
as
Select * from `usage-data-reporting.DEV_IFM_Usage.v_usage_bp_monthly_nat`
 
--Usage by BP Daily DB
Create or replace table `usage-data-reporting.DEV_IFM_Usage.mv_usage_bp_daily_db` 
partition by Event_Date
cluster by Event_Year, Event_Year_Month, BusinessPartner_ID, BP_Name
as
Select * from `usage-data-reporting.DEV_IFM_Usage.v_usage_bp_daily_db`
 
--Usage by Mat Monthly
Create or replace table `usage-data-reporting.DEV_IFM_Usage.mv_usage_mat_monthly`
partition by Event_date
cluster by Event_year_month,Event_year,JournalTitleno,Ebook_ISBN
as
Select * from  `usage-data-reporting.DEV_IFM_Usage.v_usage_mat_monthly_spl`
union all
Select * from  `usage-data-reporting.DEV_IFM_Usage.v_usage_mat_monthly_nat`

--Usage by Mat Daily
Create or replace table `usage-data-reporting.DEV_IFM_Usage.mv_usage_mat_daily`
partition by Event_date
cluster by Event_year_month,Event_year,JournalTitleno,Ebook_ISBN
as
Select * from  `usage-data-reporting.DEV_IFM_Usage.v_usage_mat_daily_spl`
union all
Select * from  `usage-data-reporting.DEV_IFM_Usage.v_usage_mat_daily_nat`

--Usage by Mat db
Create or replace table `usage-data-reporting.DEV_IFM_Usage.mv_usage_mat_daily_db`
partition by event_date
cluster by Event_year,Event_year_month,Platform,usagecontenttype
as Select * from `usage-data-reporting.DEV_IFM_Usage.v_usage_mat_daily_db`