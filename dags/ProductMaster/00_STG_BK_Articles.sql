SELECT
DISTINCT Upper(DOI) DOI
FROM
`{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.dds_article_full`
--UNION DISTINCT
--SELECT DISTINCT replace(Upper(UsageItemDOI),' ','') DOI
--FROM `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.PM_UsageArticle`
UNION DISTINCT
SELECT DISTINCT Upper(ITEMDOI) DOI
FROM `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.magnus_products`
UNION DISTINCT
SELECT DISTINCT Upper(ArtDOI) DOI
FROM `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.dds_articles_missing`
--- TO DO - Add distinct on all DOI's in Article dimension table to make sure we also include article business keys from records that are not available in the source
