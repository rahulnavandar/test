--------------------------------------------------------------------------
--------------------------------------------------------------------------
-------------------------------IFM Layer Queries--------------------------------
--------------------------------------------------------------------------

--Change Log
--2019/08/01: AP

--MONTHLY SPL and Nature Usage by Material table--
--Materialise v_usage_mat_monthly_spl and _nat into as ingle table mv_usage_mat_monthly
Create or replace table `{{ params.project }}.{{ params.environment }}_IFM_Usage.mv_usage_mat_monthly`
partition by Calendar_date
cluster by Calendar_year_month,Calendar_year,Journal_Title_no,Ebook_ISBN
as
Select * from  `{{ params.project }}.{{ params.environment }}_IFM_Usage.v_usage_mat_monthly_spl`
union all
Select * from  `{{ params.project }}.{{ params.environment }}_IFM_Usage.v_usage_mat_monthly_nat`