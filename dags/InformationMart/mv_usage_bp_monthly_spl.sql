--------------------------------------------------------------------------
--------------------------------------------------------------------------
-------------------------------IFM Layer Queries--------------------------------
--------------------------------------------------------------------------

--Change Log
--2019/08/01: AP

--MONTHLY SPL Usage by BP table--
--Materialise v_usage_bp_monthly_spl
CREATE OR REPLACE TABLE `{{ params.project }}.{{ params.environment }}_IFM_Usage.mv_usage_bp_monthly_spl`
PARTITION BY Calendar_Date
CLUSTER BY Calendar_Year, Calendar_Year_Month, BP_ID, BP_Name
AS 
SELECT * FROM `{{ params.project }}.{{ params.environment }}_IFM_Usage.v_usage_bp_monthly_spl`