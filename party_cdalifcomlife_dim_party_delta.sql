/*
    FileName: party_cdalifcomlife_dim_party.sql
    Author: MM69917
    SUBJECT AREA : Party
    SOURCE: CDA_LIFCOMLIFE
    Teradata Source Code: 50
    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    JIRA 3425               Party-Tier2    5/30                Initial version
    ------------------------------------------------------------------------------------------------------------------
--*/

TRUNCATE TABLE EDW_STAGING.PARTY_CDALIFCOMLIFE_DIM_PARTY_PRE_WORK;

TRUNCATE TABLE EDW_WORK.PARTY_CDALIFCOMLIFE_DIM_PARTY;



INSERT INTO EDW_STAGING.PARTY_CDALIFCOMLIFE_DIM_PARTY_PRE_WORK
(
    DIM_PARTY_NATURAL_KEY_HASH_UUID
   ,PARTY_ID
   ,FIRST_NM
   ,MIDDLE_NM
   ,LAST_NM
   ,GENDER_CDE
   ,BEGIN_DT
   ,BEGIN_DTM
   ,ROW_PROCESS_DTM
   ,AUDIT_ID
   ,LOGICAL_DELETE_IND
   ,CHECK_SUM
   ,CURRENT_ROW_IND
   ,END_DT
   ,END_DTM
   ,SOURCE_SYSTEM_ID
   ,RESTRICTED_ROW_IND
   ,UPDATE_AUDIT_ID
   ,PREFIX_NM
   ,SUFFIX_NM
   ,SOURCE_DELETE_IND
)
SELECT 
    DIM_PARTY_NATURAL_KEY_HASH_UUID,
    VOLTAGEPROTECT(PARTY_ID,'sorparty') AS PARTY_ID,
    FIRST_NM,
    MIDDLE_NM,
    LAST_NM,
    GENDER_CDE,
    BEGIN_DT,
    BEGIN_DTM,
    ROW_PROCESS_DTM,
    AUDIT_ID,
    LOGICAL_DELETE_IND,
    UUID_GEN(SOURCE_DELETE_IND,FIRST_NM,MIDDLE_NM,LAST_NM,GENDER_CDE,PREFIX_NM,SUFFIX_NM)::UUID AS CHECK_SUM,
    CURRENT_ROW_IND,
    END_DT,
    END_DTM,
    SOURCE_SYSTEM_ID,
    RESTRICTED_ROW_IND,
    UPDATE_AUDIT_ID,
    PREFIX_NM,
    SUFFIX_NM,
    SOURCE_DELETE_IND
FROM
(
SELECT 
    CASE WHEN (BENE_ROW_ADM_SYS_NAME,BENE_ROW_CTRT_PREFIX,BENE_ROW_CTRT_NO,BENE_ROW_CTRT_SUFFIX,BENE_ROW_CNTR) IS NULL 
		THEN UUID_GEN(NULL)::uuid
		ELSE UUID_GEN(COALESCE(BENE_ROW_ADM_SYS_NAME,'')||COALESCE(BENE_ROW_CTRT_PREFIX,'')||COALESCE(BENE_ROW_CTRT_NO,'')||COALESCE(BENE_ROW_CTRT_SUFFIX,'')||COALESCE(BENE_ROW_CNTR,''))::UUID END AS DIM_PARTY_NATURAL_KEY_HASH_UUID,
	COALESCE(bene_row_adm_sys_name,'')
	||COALESCE(bene_row_ctrt_prefix,'')
	||COALESCE(bene_row_ctrt_no,'')
	||COALESCE(bene_row_ctrt_suffix,'')
	||COALESCE(bene_row_cntr,'') AS PARTY_ID, 
	VOLTAGEPROTECT(BENE_NAME_FIRST,'name') 		AS FIRST_NM,
	VOLTAGEPROTECT(BENE_NAME_MIDDLE,'name') 	AS MIDDLE_NM,
	VOLTAGEPROTECT(BENE_NAME_LAST,'name') 		AS LAST_NM,
    bene_sex                   AS GENDER_CDE,
    CURRENT_DATE               AS BEGIN_DT,
    CURRENT_TIMESTAMP(6)       AS BEGIN_DTM,
    CURRENT_TIMESTAMP(6)       AS ROW_PROCESS_DTM,
    :audit_id                  AS AUDIT_ID, 
    FALSE::BOOLEAN             AS LOGICAL_DELETE_IND,
    TRUE::BOOLEAN              AS CURRENT_ROW_IND,
    '9999-12-31'::DATE         AS END_DT, 
    '9999-12-31'::TIMESTAMP(6) AS END_DTM,
    '50'                       AS SOURCE_SYSTEM_ID, 
    FALSE::BOOLEAN             AS RESTRICTED_ROW_IND,
    :audit_id                  AS UPDATE_AUDIT_ID,
    VOLTAGEPROTECT(bene_name_prefix,'name') AS PREFIX_NM,
    VOLTAGEPROTECT(bene_name_suffix,'name') AS SUFFIX_NM,
    source_delete_ind AS SOURCE_DELETE_IND,
	ROW_NUMBER() OVER(PARTITION BY bene_row_adm_sys_name, bene_row_ctrt_prefix, bene_row_ctrt_no, bene_row_ctrt_suffix,
								   bene_row_cntr ORDER BY BENEFICIARY_EFFECTIVE_DT desc) AS RNK
FROM
(
SELECT 
    CLEAN_STRING(VOLTAGEACCESS(bene_name_first,'name'))       AS BENE_NAME_FIRST,
    CLEAN_STRING(VOLTAGEACCESS(bene_name_middle,'name'))      AS BENE_NAME_MIDDLE,
    CLEAN_STRING(VOLTAGEACCESS(bene_name_last,'name'))        AS BENE_NAME_LAST,
    CLEAN_STRING(bene_row_adm_sys_name)                       AS BENE_ROW_ADM_SYS_NAME,
    UDF_ISNUM_LPAD(CLEAN_STRING(bene_row_ctrt_prefix),20,'0',TRUE) AS BENE_ROW_CTRT_PREFIX,
    LPAD(CLEAN_STRING(bene_row_ctrt_no),20,'0')                    AS BENE_ROW_CTRT_NO,
    UDF_ISNUM_LPAD(CLEAN_STRING(bene_row_ctrt_suffix),20,'0',TRUE) AS BENE_ROW_CTRT_SUFFIX,
    CLEAN_STRING(VOLTAGEACCESS(bene_arrngmt,'freeform'))           AS BENE_ARRNGMT,
    CLEAN_STRING(bene_row_cntr::VARCHAR)   AS BENE_ROW_CNTR,
    COALESCE(CLEAN_STRING(bene_sex),'Unk') AS BENE_SEX,
    CLEAN_STRING(VOLTAGEACCESS(bene_name_prefix,'name'))      AS BENE_NAME_PREFIX,
    CLEAN_STRING(VOLTAGEACCESS(bene_name_suffix,'name'))      AS BENE_NAME_SUFFIX,
	FALSE::BOOLEAN      AS SOURCE_DELETE_IND,
	BENE_EFF_DT                                                    AS BENEFICIARY_EFFECTIVE_DT
FROM EDW_STAGING.CDA_LIFCOM_LIFE_EDW_BENE_DELTA
)SOURCE_DATASET
)FULL_DATASET WHERE RNK=1;

COMMIT;


/* insert new records into work  */

INSERT INTO  EDW_WORK.PARTY_CDALIFCOMLIFE_DIM_PARTY
(
    DIM_PARTY_NATURAL_KEY_HASH_UUID
   ,PARTY_ID
   ,FIRST_NM
   ,MIDDLE_NM
   ,LAST_NM
   ,GENDER_CDE
   ,BEGIN_DT
   ,BEGIN_DTM
   ,ROW_PROCESS_DTM
   ,AUDIT_ID
   ,LOGICAL_DELETE_IND
   ,CHECK_SUM
   ,CURRENT_ROW_IND
   ,END_DT
   ,END_DTM
   ,SOURCE_SYSTEM_ID
   ,RESTRICTED_ROW_IND
   ,UPDATE_AUDIT_ID
   ,PREFIX_NM
   ,SUFFIX_NM
   ,SOURCE_DELETE_IND
)
SELECT 
    SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID
   ,SRC.PARTY_ID
   ,SRC.FIRST_NM
   ,SRC.MIDDLE_NM
   ,SRC.LAST_NM
   ,SRC.GENDER_CDE
   ,'01/01/0001'::DATE                  AS BEGIN_DT
   ,'01/01/0001 00:00:00'::TIMESTAMP(6) AS BEGIN_DTM
   ,SRC.ROW_PROCESS_DTM
   ,SRC.AUDIT_ID
   ,SRC.LOGICAL_DELETE_IND
   ,SRC.CHECK_SUM
   ,SRC.CURRENT_ROW_IND
   ,SRC.END_DT
   ,SRC.END_DTM
   ,SRC.SOURCE_SYSTEM_ID
   ,SRC.RESTRICTED_ROW_IND
   ,SRC.UPDATE_AUDIT_ID
   ,SRC.PREFIX_NM
   ,SRC.SUFFIX_NM
   ,SRC.SOURCE_DELETE_IND
FROM EDW_STAGING.PARTY_CDALIFCOMLIFE_DIM_PARTY_PRE_WORK SRC
LEFT JOIN EDW.DIM_PARTY PTY
ON SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID = PTY.DIM_PARTY_NATURAL_KEY_HASH_UUID
WHERE PTY.DIM_PARTY_NATURAL_KEY_HASH_UUID IS NULL;

/* insert updated records from target into work */

INSERT INTO  EDW_WORK.PARTY_CDALIFCOMLIFE_DIM_PARTY
(
    DIM_PARTY_NATURAL_KEY_HASH_UUID
   ,PARTY_ID
   ,FIRST_NM
   ,MIDDLE_NM
   ,LAST_NM
   ,GENDER_CDE
   ,BEGIN_DT
   ,BEGIN_DTM
   ,ROW_PROCESS_DTM
   ,AUDIT_ID
   ,LOGICAL_DELETE_IND
   ,CHECK_SUM
   ,CURRENT_ROW_IND
   ,END_DT
   ,END_DTM
   ,SOURCE_SYSTEM_ID
   ,RESTRICTED_ROW_IND
   ,UPDATE_AUDIT_ID
   ,PREFIX_NM
   ,SUFFIX_NM
   ,SOURCE_DELETE_IND
)
SELECT 
    TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID
   ,TGT.PARTY_ID
   ,TGT.FIRST_NM
   ,TGT.MIDDLE_NM
   ,TGT.LAST_NM
   ,TGT.GENDER_CDE
   ,TGT.BEGIN_DT
   ,TGT.BEGIN_DTM
   ,CURRENT_TIMESTAMP                      AS ROW_PROCESS_DTM
   ,TGT.AUDIT_ID
   ,TGT.LOGICAL_DELETE_IND
   ,TGT.CHECK_SUM
   ,FALSE                                  AS CURRENT_ROW_IND
   ,SRC.BEGIN_DT - INTERVAL '1' DAY        AS END_DT
   ,SRC.BEGIN_DTM	- INTERVAL '1' SECOND  AS END_DTM
   ,TGT.SOURCE_SYSTEM_ID
   ,TGT.RESTRICTED_ROW_IND
   ,SRC.UPDATE_AUDIT_ID                    AS UPDATE_AUDIT_ID
   ,TGT.PREFIX_NM
   ,TGT.SUFFIX_NM
   ,TGT.SOURCE_DELETE_IND
FROM EDW_STAGING.PARTY_CDALIFCOMLIFE_DIM_PARTY_PRE_WORK SRC
JOIN EDW.DIM_PARTY TGT
ON SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID = TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND TGT.CURRENT_ROW_IND = TRUE 
WHERE (TGT.CHECK_SUM <> SRC.CHECK_SUM);

COMMIT;

/* insert updated records from source into work */

INSERT INTO  EDW_WORK.PARTY_CDALIFCOMLIFE_DIM_PARTY
(
    DIM_PARTY_NATURAL_KEY_HASH_UUID
   ,PARTY_ID
   ,FIRST_NM
   ,MIDDLE_NM
   ,LAST_NM
   ,GENDER_CDE
   ,BEGIN_DT
   ,BEGIN_DTM
   ,ROW_PROCESS_DTM
   ,AUDIT_ID
   ,LOGICAL_DELETE_IND
   ,CHECK_SUM
   ,CURRENT_ROW_IND
   ,END_DT
   ,END_DTM
   ,SOURCE_SYSTEM_ID
   ,RESTRICTED_ROW_IND
   ,UPDATE_AUDIT_ID
   ,PREFIX_NM
   ,SUFFIX_NM
   ,SOURCE_DELETE_IND
)
SELECT 
    SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID
   ,SRC.PARTY_ID
   ,SRC.FIRST_NM
   ,SRC.MIDDLE_NM
   ,SRC.LAST_NM
   ,SRC.GENDER_CDE
   ,SRC.BEGIN_DT
   ,SRC.BEGIN_DTM
   ,CURRENT_TIMESTAMP AS ROW_PROCESS_DTM
   ,SRC.AUDIT_ID
   ,SRC.LOGICAL_DELETE_IND
   ,SRC.CHECK_SUM
   ,SRC.CURRENT_ROW_IND
   ,SRC.END_DT
   ,SRC.END_DTM
   ,SRC.SOURCE_SYSTEM_ID
   ,SRC.RESTRICTED_ROW_IND
   ,SRC.UPDATE_AUDIT_ID
   ,SRC.PREFIX_NM
   ,SRC.SUFFIX_NM
   ,SRC.SOURCE_DELETE_IND
FROM EDW_STAGING.PARTY_CDALIFCOMLIFE_DIM_PARTY_PRE_WORK SRC
LEFT JOIN EDW.DIM_PARTY TGT
ON SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID = TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND TGT.CURRENT_ROW_IND = TRUE  
WHERE 
(
    TGT.ROW_SID IS NULL 
    AND SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID 
    IN
    (	
     SELECT DISTINCT DP.DIM_PARTY_NATURAL_KEY_HASH_UUID  
     FROM EDW_STAGING.PARTY_CDALIFCOMLIFE_DIM_PARTY_PRE_WORK S
     INNER JOIN edw.dim_party DP
     ON S.DIM_PARTY_NATURAL_KEY_HASH_UUID = DP.DIM_PARTY_NATURAL_KEY_HASH_UUID
    )
OR --checksum changed
   (TGT.ROW_SID IS NOT NULL AND (TGT.CHECK_SUM <> SRC.CHECK_SUM)) 
);

COMMIT;