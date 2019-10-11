--------------------------------------------------------------------------
--------------------------------------------------------------------------
-------------------------------IFM Layer Queries--------------------------------
--------------------------------------------------------------------------

--Change Log
--2019/08/01: AP

--MASTER DATA table--
--Materialise v_UsageProductMaster
CREATE OR REPLACE TABLE `{{ params.project }}.{{ params.environment }}_IFM_Usage.ProductMaster_usageAggr`
AS
SELECT * FROM `{{ params.project }}.{{ params.environment }}_IFM_Usage.v_ProductMaster_usageAggr`