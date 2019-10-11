--------------------------------------------------------------------------
--------------------------------------------------------------------------
-------------------------------IFM Layer Queries--------------------------------
--------------------------------------------------------------------------

--Change Log
--2019/08/01: AP

--MASTER DATA table--
--Materialise v_UsageBPMaster
CREATE OR REPLACE TABLE `{{ params.project }}.{{ params.environment }}_IFM_Usage.UsageBPMaster`
AS
SELECT * FROM `{{ params.project }}.{{ params.environment }}_IFM_Usage.v_UsageBPMaster`