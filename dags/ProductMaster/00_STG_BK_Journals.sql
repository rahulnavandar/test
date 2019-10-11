SELECT DISTINCT JOURNAL_KEY JournalKey
  from `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jfw_journals`
UNION DISTINCT
SELECT DISTINCT PRODUCT_ID
  from `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jfw_non_regular_journals`
--- TO DO - Add distinct on all DOI's in Journal Hist dimension table to make sure we also include journal business keys from records that are not available in the source

