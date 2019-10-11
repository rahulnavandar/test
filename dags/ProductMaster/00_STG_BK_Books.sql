  SELECT
    DISTINCT ORDER_NUMBER BookKey
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.bflux_books_diff`
  UNION DISTINCT
  SELECT
    DISTINCT ORDER_NUMBER BookKey
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.bflux_books_full` 
--- TO DO - Add distinct on all DOI's in Books Hist dimension table to make sure we also include book business keys from records that are not available in the source
