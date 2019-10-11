--------------------------------------------------------------------------
--------------------------------------------------------------------------
-------------------------------IFM Layer Queries--------------------------------
--------------------------------------------------------------------------

--Change Log
--2019/08/01: AP

--MONTHLY NATURE Usage by BP table--
--Materialise v_usage_bp_monthly_nat
Create or replace table `{{ params.project }}.{{ params.environment }}_IFM_Usage.mv_usage_bp_monthly_nat` 
 partition by Calendar_Date
 cluster by Calendar_Year, Calendar_Year_Month, Site_ID
 as
 Select * from `{{ params.project }}.{{ params.environment }}_IFM_Usage.v_usage_bp_monthly_nat`