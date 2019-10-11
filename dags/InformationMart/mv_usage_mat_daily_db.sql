--------------------------------------------------------------------------
--------------------------------------------------------------------------
-------------------------------IFM Layer Queries--------------------------------
--------------------------------------------------------------------------

--Change Log
--2019/08/01: AP
--2019/10/04: FK new logic retrieved from AP via Slack

--DAILY Database Usage by Material table--
--Materialise v_usage_mat_daily_db
CREATE OR REPLACE TABLE `{{ params.project }}.{{ params.environment }}_IFM_Usage.mv_usage_mat_daily_db`
PARTITION BY Calendar_date
CLUSTER BY Calendar_year,Calendar_year_month,Platform,Download_Denial_Type
AS
SELECT * FROM `{{ params.project }}.{{ params.environment }}_IFM_Usage.v_usage_mat_daily_db`