/*
    FileName: PARTY_CDALIFCOMLIFE_PARTY_MASTER_OF_MASTERS_XREF.sql
    Author: MM14803
    Subject Area : Party
    Source:CDA  LIF COM
    Create Date:2021-08-25
       
    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    TERSUN-3389             Party-Tier2    05/31                Initial version      
    ------------------------------------------------------------------------------------------------------------------
*/

/* truncate pre_work and work */

TRUNCATE TABLE EDW_STAGING.PARTY_CDALIFCOMLIFE_PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK;
TRUNCATE TABLE EDW_WORK.PARTY_CDALIFCOMLIFE_PARTY_MASTER_OF_MASTERS_XREF;


/* insert into pre_work from soruce */
INSERT /*+direct*/ INTO EDW_STAGING.PARTY_CDALIFCOMLIFE_PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK
(
	DIM_PARTY_NATURAL_KEY_HASH_UUID,
	DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
	PARTY_ID,
	PARTY_PRIOR_ID,
	SOR_PARTY_ID,
	PARTY_ID_TYPE_CDE,
	BEGIN_DT,
	BEGIN_DTM,
	ROW_PROCESS_DTM,
	AUDIT_ID,
	LOGICAL_DELETE_IND,
	CHECK_SUM,
	CURRENT_ROW_IND,
	END_DT,
	END_DTM,
	SOURCE_SYSTEM_ID,
	RESTRICTED_ROW_IND,
	UPDATE_AUDIT_ID,
	SOURCE_DELETE_IND
)
SELECT       
DIM_PARTY_NATURAL_KEY_HASH_UUID,
DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
PARTY_ID,
PARTY_PRIOR_ID,
SOR_PARTY_ID,
PARTY_ID_TYPE_CDE,
BEGIN_DT,
BEGIN_DTM,
ROW_PROCESS_DTM,
AUDIT_ID,
LOGICAL_DELETE_IND,
CHECK_SUM,
CURRENT_ROW_IND,
END_DT,
END_DTM,
SOURCE_SYSTEM_ID,
RESTRICTED_ROW_IND,
UPDATE_AUDIT_ID,
SOURCE_DELETE_IND
FROM 
(
    SELECT       
    DIM_PARTY_NATURAL_KEY_HASH_UUID,
    DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    VOLTAGEPROTECT(PARTY_ID,'sorparty')                                    AS PARTY_ID,
    VOLTAGEPROTECT(PARTY_PRIOR_ID,'sorparty')                              AS PARTY_PRIOR_ID,
    VOLTAGEPROTECT(SOR_PARTY_ID,'sorparty')                                AS SOR_PARTY_ID,
    PARTY_ID_TYPE_CDE,
    BEGIN_DT,
    BEGIN_DTM,
    ROW_PROCESS_DTM,
    AUDIT_ID,
    LOGICAL_DELETE_IND,
    CHECK_SUM,
    CURRENT_ROW_IND,
    END_DT,
    END_DTM,
    SOURCE_SYSTEM_ID,
    RESTRICTED_ROW_IND,
    UPDATE_AUDIT_ID,
    SOURCE_DELETE_IND,
    ROW_NUMBER() OVER(PARTITION BY
	DIM_PARTY_NATURAL_KEY_HASH_UUID,
	PARTY_ID,
	PARTY_ID_TYPE_CDE ORDER BY BENEFICIARY_EFFECTIVE_DT) RNK                                        
    FROM 
    (
		SELECT 
		CASE WHEN (BENE_ROW_ADM_SYS_NAME,BENE_ROW_CTRT_PREFIX,BENE_ROW_CTRT_NO,BENE_ROW_CTRT_SUFFIX,BENE_ROW_CNTR) IS NULL 
			THEN UUID_GEN(NULL)::UUID 
			ELSE UUID_GEN(COALESCE(BENE_ROW_ADM_SYS_NAME,'')||COALESCE(BENE_ROW_CTRT_PREFIX,'')||COALESCE(BENE_ROW_CTRT_NO,'')||COALESCE(BENE_ROW_CTRT_SUFFIX,'')||COALESCE(BENE_ROW_CNTR,''))::UUID END AS DIM_PARTY_NATURAL_KEY_HASH_UUID,
		CASE WHEN PARTY_ID_TYPE_CDE = 'Beneficiary' OR (BENE_ROW_ADM_SYS_NAME,BENE_ROW_CTRT_PREFIX,BENE_ROW_CTRT_NO,BENE_ROW_CTRT_SUFFIX,BENE_ROW_CNTR) IS NULL 
			THEN UUID_GEN(NULL)::UUID 
			ELSE UUID_GEN(COALESCE(BENE_ROW_ADM_SYS_NAME,'')||COALESCE(BENE_ROW_CTRT_PREFIX,'')||COALESCE(BENE_ROW_CTRT_NO,'')||COALESCE(BENE_ROW_CTRT_SUFFIX,'')||COALESCE(BENE_ROW_CNTR,''))::UUID END AS DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
		COALESCE(BENE_ROW_ADM_SYS_NAME,'')||COALESCE(BENE_ROW_CTRT_PREFIX,'')||COALESCE(BENE_ROW_CTRT_NO,'')||COALESCE(BENE_ROW_CTRT_SUFFIX,'')||COALESCE(BENE_ROW_CNTR,'')	 AS PARTY_ID,
		CASE WHEN  PARTY_ID_TYPE_CDE = 'Mstr_prty_id' 
			THEN COALESCE(BENE_ROW_ADM_SYS_NAME,'')||COALESCE(BENE_ROW_CTRT_PREFIX,'')||COALESCE(BENE_ROW_CTRT_NO,'')||COALESCE(BENE_ROW_CTRT_SUFFIX,'')||COALESCE(BENE_ROW_CNTR,'')
			ELSE NULL END 												   AS PARTY_PRIOR_ID,
		NULL                                                               AS SOR_PARTY_ID,
		PARTY_ID_TYPE_CDE                                                  AS PARTY_ID_TYPE_CDE,
		'0001-01-01'::DATE                                                 AS BEGIN_DT,
		'0001-01-01'::TIMESTAMP(6)                                         AS BEGIN_DTM,
		CURRENT_TIMESTAMP                                                  AS ROW_PROCESS_DTM,
		:audit_id                                                          AS AUDIT_ID,
		FALSE                                                              AS LOGICAL_DELETE_IND,
		UUID_GEN(FALSE)::UUID                                              AS CHECK_SUM,
		TRUE                                                               AS CURRENT_ROW_IND,
		'12/31/9999'::DATE                                                 AS END_DT,
		'12/31/9999 00:00:00'::TIMESTAMP(6)                                AS END_DTM,
		'50'                                                               AS SOURCE_SYSTEM_ID,
		FALSE                                                              AS RESTRICTED_ROW_IND,
		:audit_id                                                          AS UPDATE_AUDIT_ID,
		FALSE                                                              AS SOURCE_DELETE_IND,
		BENEFICIARY_EFFECTIVE_DT                                           AS BENEFICIARY_EFFECTIVE_DT
		FROM 
		(
			SELECT 
			CLEAN_STRING(VOLTAGEACCESS(BENE_NAME_FIRST,'name'))            AS BENE_NAME_FIRST, 
			CLEAN_STRING(VOLTAGEACCESS(BENE_NAME_MIDDLE,'name'))           AS BENE_NAME_MIDDLE,
			CLEAN_STRING(VOLTAGEACCESS(BENE_NAME_LAST,'name'))             AS BENE_NAME_LAST, 
			LPAD(CLEAN_STRING(BENE_ROW_CTRT_NO),20,'0')                    AS BENE_ROW_CTRT_NO,
			CLEAN_STRING(VOLTAGEACCESS(BENE_ARRNGMT,'freeform'))           AS BENE_ARRNGMT,
			CLEAN_STRING(BENE_ROW_CNTR::VARCHAR)                           AS BENE_ROW_CNTR,
			BENE_EFF_DT                                                    AS BENEFICIARY_EFFECTIVE_DT, 
			'Mstr_prty_id'                                                 AS PARTY_ID_TYPE_CDE,
			UDF_ISNUM_LPAD(CLEAN_STRING(BENE_ROW_CTRT_PREFIX),20,'0',TRUE) AS BENE_ROW_CTRT_PREFIX,
			UDF_ISNUM_LPAD(CLEAN_STRING(BENE_ROW_CTRT_SUFFIX),20,'0',TRUE) AS BENE_ROW_CTRT_SUFFIX,
			CLEAN_STRING(BENE_ROW_ADM_SYS_NAME)                            AS BENE_ROW_ADM_SYS_NAME
			FROM EDW_STAGING.cda_lifcom_life_edw_bene_delta  SRC
			UNION 
			SELECT 
			CLEAN_STRING(VOLTAGEACCESS(BENE_NAME_FIRST,'name'))            AS BENE_NAME_FIRST, 
			CLEAN_STRING(VOLTAGEACCESS(BENE_NAME_MIDDLE,'name'))           AS BENE_NAME_MIDDLE,
			CLEAN_STRING(VOLTAGEACCESS(BENE_NAME_LAST,'name'))             AS BENE_NAME_LAST, 
			LPAD(CLEAN_STRING(BENE_ROW_CTRT_NO),20,'0')                    AS BENE_ROW_CTRT_NO,
			CLEAN_STRING(VOLTAGEACCESS(BENE_ARRNGMT,'freeform'))           AS BENE_ARRNGMT,
			CLEAN_STRING(BENE_ROW_CNTR::VARCHAR)                           AS BENE_ROW_CNTR,
			BENE_EFF_DT                                                    AS BENEFICIARY_EFFECTIVE_DT, 
			'Beneficiary'                                                  AS PARTY_ID_TYPE_CDE,
			UDF_ISNUM_LPAD(CLEAN_STRING(BENE_ROW_CTRT_PREFIX),20,'0',TRUE) AS BENE_ROW_CTRT_PREFIX,
			UDF_ISNUM_LPAD(CLEAN_STRING(BENE_ROW_CTRT_SUFFIX),20,'0',TRUE) AS BENE_ROW_CTRT_SUFFIX,
			CLEAN_STRING(BENE_ROW_ADM_SYS_NAME)                            AS BENE_ROW_ADM_SYS_NAME
			FROM EDW_STAGING.cda_lifcom_life_edw_bene_delta SRC
		) SOURCE_DATASET 
	)FULL_DATASET
)DEDUP WHERE RNK=1;

COMMIT;

SELECT ANALYZE_STATISTICS('EDW_STAGING.PARTY_CDALIFCOMLIFE_PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK');

/* insert new records into work  */




INSERT /*+direct*/ INTO EDW_WORK.PARTY_CDALIFCOMLIFE_PARTY_MASTER_OF_MASTERS_XREF
(
	DIM_PARTY_NATURAL_KEY_HASH_UUID,
	DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
	PARTY_ID,
	PARTY_PRIOR_ID,
	SOR_PARTY_ID,
	PARTY_ID_TYPE_CDE,
	BEGIN_DT,
	BEGIN_DTM,
	ROW_PROCESS_DTM,
	AUDIT_ID,
	LOGICAL_DELETE_IND,
	CHECK_SUM,
	CURRENT_ROW_IND,
	END_DT,
	END_DTM,
	SOURCE_SYSTEM_ID,
	RESTRICTED_ROW_IND,
	UPDATE_AUDIT_ID,
	PARTY_ID_CLASS_TYPE_CDE,
	PARTY_ID_STATUS_TYPE_CDE,
	SOURCE_PARTY_ID_TYPE_CDE,
	SOURCE_PARTY_ID_STATUS_TYPE_CDE,
	SOURCE_PARTY_ID_CLASS_TYPE_CDE,
	SOURCE_DELETE_IND
)
SELECT 
SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID,
SRC.DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
SRC.PARTY_ID,
SRC.PARTY_PRIOR_ID,
SRC.SOR_PARTY_ID,
SRC.PARTY_ID_TYPE_CDE,
SRC.BEGIN_DT,
SRC.BEGIN_DTM,
SRC.ROW_PROCESS_DTM,
SRC.AUDIT_ID,
SRC.LOGICAL_DELETE_IND,
SRC.CHECK_SUM,
SRC.CURRENT_ROW_IND,
SRC.END_DT,
SRC.END_DTM,
SRC.SOURCE_SYSTEM_ID,
SRC.RESTRICTED_ROW_IND,
SRC.UPDATE_AUDIT_ID,
SRC.PARTY_ID_CLASS_TYPE_CDE,
SRC.PARTY_ID_STATUS_TYPE_CDE,
SRC.SOURCE_PARTY_ID_TYPE_CDE,
SRC.SOURCE_PARTY_ID_STATUS_TYPE_CDE,
SRC.SOURCE_PARTY_ID_CLASS_TYPE_CDE,
SRC.SOURCE_DELETE_IND
FROM EDW_STAGING.PARTY_CDALIFCOMLIFE_PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK SRC
LEFT JOIN EDW.PARTY_MASTER_OF_MASTERS_XREF XREF 
ON SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID = XREF.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND SRC.DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID = XREF.DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID
AND SRC.PARTY_ID = XREF.PARTY_ID
AND COALESCE(SRC.SOR_PARTY_ID,'-') = COALESCE(XREF.SOR_PARTY_ID,'-')
AND COALESCE(SRC.PARTY_ID_TYPE_CDE,'-') = COALESCE(XREF.PARTY_ID_TYPE_CDE,'-')
AND XREF.SOURCE_SYSTEM_ID IN ('50','238')
WHERE XREF.ROW_SID IS NULL; 

COMMIT;
         
SELECT ANALYZE_STATISTICS('EDW_WORK.PARTY_CDALIFCOMLIFE_PARTY_MASTER_OF_MASTERS_XREF');