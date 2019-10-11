INSERT INTO `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Issues_Hist`
(IssueKey, IssueAvailable, IssueCostUnit,IssueCoverDate, IssueDeliveryDate, IssueDistributionDate, IssueImplementationDate, IssueInitDistrDate, IssueIDEnd, IssueIDStart, IssueModificationDate, IssueNumber, IssueType, IssueJournalTitleNumber, IssueKindofIssue, IssueNbrofArticlesYear, IssueNbrofIssuesYear, IssueNumberofIssues, IssueNumberofPages, IssueObjectname, IssueOnlineIssuePublished, IssuePhysical, IssuePlannedNoofPages, IssuePlannedPublicationDate, IssuePODnotSuitableReason, IssuePODeSuitable, IssuePriceListYear, IssuePrintPDFAvailable, IssuePrintRunMarketing, IssuePrintRunOthers, IssuePrintRunRegular, IssueProductionDate, IssueProductionSite, IssuePublishedOnlineDateDDS, IssuePublisher, IssueSalesFormat, IssueTitle, IssueVolume, IssueVolumeNumber, IssueVolumeNumberIssue, IssueWBSElement, IssueLastUploadDate,IssueReadyForIssueBuilding,IssueProductionWFStart,IssueSentForIssueTypesetting,IssueInIssueTypesetting,IssueTypesettingDone,IssueCheckQuality,IssueQualityCheckDisapproved,IssueQualityCheckApproved,IssueSentToPrinter,IssuePrepareOnlineIssuePublished,IssueSendingToDDS,IssueSentToDDS,IssueReceivedByDDS,IssueJournalPrinter,IssueCurrentStatus,IssueNumberOfArticlePages,IssueProductionSystem,IssueVolumeIssueNr,IssuePSPElement,IssueTypeCode,IssueBinding,IssuePaperStockText,IssueCoverPrinting,IssueCoverPaperStock,IssueCoverProductionInfoFinishing,IssueJournalPrintQuality,IssueFirstPage,IssueLastPage,IssueOtherAPages,IssueAdditionalPages,IssueContentInJWF,IssueWeight,IssueEditorialPages,IssueDistributionWFStart,IssueAtPrinter,IssuePrintedShipped)
SELECT IssueKey, IssueAvailable, IssueCostUnit,IssueCoverDate, IssueDeliveryDate, IssueDistributionDate, IssueImplementationDate, IssueInitDistrDate, IssueIDEnd, IssueIDStart, IssueModificationDate, IssueNumber, IssueType, IssueJournalTitleNumber, IssueKindofIssue, IssueNbrofArticlesYear, IssueNbrofIssuesYear, IssueNumberofIssues, IssueNumberofPages, IssueObjectname, IssueOnlineIssuePublished, IssuePhysical, IssuePlannedNoofPages, IssuePlannedPublicationDate, IssuePODnotSuitableReason, IssuePODeSuitable, IssuePriceListYear, IssuePrintPDFAvailable, IssuePrintRunMarketing, IssuePrintRunOthers, IssuePrintRunRegular, IssueProductionDate, IssueProductionSite, IssuePublishedOnlineDateDDS, IssuePublisher, IssueSalesFormat, IssueTitle, IssueVolume, IssueVolumeNumber, IssueVolumeNumberIssue, IssueJournalTitleNumber, IssueLastUploadDate,IssueReadyForIssueBuilding,IssueProductionWFStart,IssueSentForIssueTypesetting,IssueInIssueTypesetting,IssueTypesettingDone,IssueCheckQuality,IssueQualityCheckDisapproved,IssueQualityCheckApproved,IssueSentToPrinter,IssuePrepareOnlineIssuePublished,IssueSendingToDDS,IssueSentToDDS,IssueReceivedByDDS,IssueJournalPrinter,IssueCurrentStatus,IssueNumberOfArticlePages,IssueProductionSystem,IssueVolumeIssueNr,IssuePSPElement,IssueTypeCode,IssueBinding,IssuePaperStockText,IssueCoverPrinting,IssueCoverPaperStock,IssueCoverProductionInfoFinishing,IssueJournalPrintQuality,IssueFirstPage,IssueLastPage,IssueOtherAPages,IssueAdditionalPages,IssueContentInJWF,IssueWeight,IssueEditorialPages,IssueDistributionWFStart,IssueAtPrinter,IssuePrintedShipped
FROM `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Issues`