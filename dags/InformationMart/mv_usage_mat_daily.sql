--------------------------------------------------------------------------
--------------------------------------------------------------------------
-------------------------------IFM Layer Queries--------------------------------
--------------------------------------------------------------------------

--Change Log
--2019/08/01: AP

--DAILY SPL and Nature Usage by Material table--
 --Materialise v_usage_mat_daily_spl and _nat into a single table mv_usage_mat_daily
Create or replace table `{{ params.project }}.{{ params.environment }}_IFM_Usage.mv_usage_mat_daily`
partition by Calendar_date
cluster by Calendar_year_month,Calendar_year,Journal_Title_no,Ebook_ISBN
as
Select * from  `{{ params.project }}.{{ params.environment }}_IFM_Usage.v_usage_mat_daily_spl`
union all
Select * from  `{{ params.project }}.{{ params.environment }}_IFM_Usage.v_usage_mat_daily_nat`