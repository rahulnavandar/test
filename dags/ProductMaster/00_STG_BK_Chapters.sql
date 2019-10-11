SELECT
    DISTINCT upper(CHAPTERDOI) ItemDOI
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.dds_chapters_full`
  UNION DISTINCT
  SELECT
    DISTINCT upper(CHAPTERDOI) ItemDOI
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.dds_chapters_diff`
--UNION DISTINCT
--SELECT DISTINCT upper(UsageItemDOI)
--FROM `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.PM_UsageChapterBook`
--- TO DO - Add distinct on all DOI's in Chapter dimension table to make sure we also include article business keys from records that are not available in the source
