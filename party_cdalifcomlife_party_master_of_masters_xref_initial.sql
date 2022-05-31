/*
    FileName: party_cdalicom_party_master_of_masters_xref.sql
    Author: MM14803
    Subject Area : Party
    Source: CDA LIF COM
    Create Date:2021-08-25
       
    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    TERSUN-3426             Party-Tier2    05/31                Initial version      
    ------------------------------------------------------------------------------------------------------------------
*/

/* truncate work */
TRUNCATE TABLE EDW_WORK.PARTY_CDALIFCOMLIFE_PARTY_MASTER_OF_MASTERS_XREF;

/* insert mastered records into work from tersun source view */


INSERT /*+direct*/ INTO EDW_WORK.PARTY_CDALIFCOMLIFE_PARTY_MASTER_OF_MASTERS_XREF
(
	 DIM_PARTY_NATURAL_KEY_HASH_UUID  
	,DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID  
	,PARTY_ID 
	,PARTY_PRIOR_ID 
	,SOR_PARTY_ID
	,PARTY_ID_TYPE_CDE 
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
	,SOURCE_DELETE_IND 
)
SELECT
 UUID_GEN(PARTY_ID)::uuid AS DIM_PARTY_NATURAL_KEY_HASH_UUID  
,UUID_GEN(PARTY_PRIOR_ID)::uuid AS DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID  
,VOLTAGEPROTECT(PARTY_ID,'sorparty')                                 AS PARTY_ID 
,VOLTAGEPROTECT(PARTY_PRIOR_ID,'sorparty')                           AS PARTY_PRIOR_ID  
,VOLTAGEPROTECT(SOR_PARTY_ID,'sorparty')                             AS SOR_PARTY_ID
,PARTY_ID_TYPE_CDE
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
,SOURCE_DELETE_IND  
FROM
(
	SELECT 
	COALESCE(CARR_ADMIN_SYS_CD,'')||COALESCE(HLDG_KEY_PFX,'')||COALESCE(HLDG_KEY,'')||COALESCE(HLDG_KEY_SFX,'')||COALESCE(BEN_ROW_CNTR_CD,'') AS PARTY_ID
	,COALESCE(CARR_ADMIN_SYS_CD,'')||COALESCE(HLDG_KEY_PFX,'')||COALESCE(HLDG_KEY,'')||COALESCE(HLDG_KEY_SFX,'')||COALESCE(BEN_ROW_CNTR_CD,'') AS PARTY_PRIOR_ID  
	,SOR_PARTY_ID
	,PARTY_ID_TYPE_CDE 
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
	,SOURCE_DELETE_IND  
	FROM 
	(
		SELECT 
		CLEAN_STRING(VOLTAGEACCESS(BEN_FRST_NM,'name'))              AS BEN_FRST_NM, 
		CLEAN_STRING(VOLTAGEACCESS(BEN_MDL_NM,'name'))               AS BEN_MDL_NM,
		CLEAN_STRING(VOLTAGEACCESS(BEN_LST_NM,'name'))               AS BEN_LST_NM, 
		LPAD(CLEAN_STRING(HLDG_KEY),20,'0')                          AS HLDG_KEY,
		CLEAN_STRING(VOLTAGEACCESS(BEN_ARGMT_TXT,'freeform'))        AS BEN_ARGMT_TXT,
		CLEAN_STRING(BEN_ROW_CNTR_CD)                                AS BEN_ROW_CNTR_CD,
		NULL                                                         AS SOR_PARTY_ID,
		'Mstr_prty_id'                                               AS PARTY_ID_TYPE_CDE,
		'0001-01-01'::DATE                                           AS BEGIN_DT,
		'0001-01-01'::TIMESTAMP(6)                                   AS BEGIN_DTM,
		CURRENT_TIMESTAMP                                            AS ROW_PROCESS_DTM,
		:audit_id                                                    AS AUDIT_ID,
		FALSE                                                        AS LOGICAL_DELETE_IND,
		UUID_GEN(FALSE)::UUID                                        AS CHECK_SUM,
		TRUE                                                         AS CURRENT_ROW_IND,
		'12/31/9999'::DATE                                           AS END_DT,
		'12/31/9999 00:00:00'::TIMESTAMP(6)                          AS END_DTM,
		'238'                                                        AS SOURCE_SYSTEM_ID,
		FALSE                                                        AS RESTRICTED_ROW_IND,
		:audit_id                                                    AS UPDATE_AUDIT_ID,
		FALSE::BOOLEAN                                               AS SOURCE_DELETE_IND,
		CLEAN_STRING(CARR_ADMIN_SYS_CD)                              AS CARR_ADMIN_SYS_CD,
		UDF_ISNUM_LPAD(CLEAN_STRING(HLDG_KEY_PFX), 20, '0', TRUE)    AS HLDG_KEY_PFX,
		UDF_ISNUM_LPAD(CLEAN_STRING(HLDG_KEY_SFX), 20, '0', TRUE)    AS HLDG_KEY_SFX,
		ROW_NUMBER() OVER(PARTITION BY
		CLEAN_STRING(CARR_ADMIN_SYS_CD),
		UDF_ISNUM_LPAD(CLEAN_STRING(HLDG_KEY_PFX), 20, '0', TRUE),
		LPAD(CLEAN_STRING(HLDG_KEY),20,'0'),  
		UDF_ISNUM_LPAD(CLEAN_STRING(HLDG_KEY_SFX), 20, '0', TRUE),
		CLEAN_STRING(BEN_ROW_CNTR_CD) ORDER BY BEN_DATA_TO_DT DESC) RNK
		FROM PROD_STND_VW_TERSUN.BEN_DATA_VW SRC
		WHERE SRC.SRC_SYS_ID = 50 
	)SOURCE_DATASET WHERE RNK=1
)FINAL_DATASET;

COMMIT;

/* insert master records into work from tersun source view */

INSERT /*+direct*/ INTO EDW_WORK.PARTY_CDALIFCOMLIFE_PARTY_MASTER_OF_MASTERS_XREF
(
	 DIM_PARTY_NATURAL_KEY_HASH_UUID  
	,DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID  
	,PARTY_ID 
	,PARTY_PRIOR_ID 
	,SOR_PARTY_ID
	,PARTY_ID_TYPE_CDE 
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
	,SOURCE_DELETE_IND 
)
SELECT
 UUID_GEN(PARTY_ID)::uuid AS DIM_PARTY_NATURAL_KEY_HASH_UUID  
,UUID_GEN(NULL)::uuid AS DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID  
,VOLTAGEPROTECT(PARTY_ID,'sorparty')                                 AS PARTY_ID 
,VOLTAGEPROTECT(PARTY_PRIOR_ID,'sorparty')                           AS PARTY_PRIOR_ID  
,VOLTAGEPROTECT(SOR_PARTY_ID,'sorparty')                             AS SOR_PARTY_ID
,PARTY_ID_TYPE_CDE 
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
,SOURCE_DELETE_IND  
FROM
(
	SELECT 
	COALESCE(CARR_ADMIN_SYS_CD,'')
	||COALESCE(HLDG_KEY_PFX,'')
	||COALESCE(HLDG_KEY,'')
	||COALESCE(HLDG_KEY_SFX,'')
	||COALESCE(BEN_ROW_CNTR_CD,'') 										AS PARTY_ID
	,PARTY_PRIOR_ID  
	,SOR_PARTY_ID
	,PARTY_ID_TYPE_CDE 
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
	,SOURCE_DELETE_IND  
	FROM 
	(
		SELECT 
		CLEAN_STRING(VOLTAGEACCESS(BEN_FRST_NM,'name'))              AS BEN_FRST_NM, 
		CLEAN_STRING(VOLTAGEACCESS(BEN_MDL_NM,'name'))               AS BEN_MDL_NM,
		CLEAN_STRING(VOLTAGEACCESS(BEN_LST_NM,'name'))               AS BEN_LST_NM, 
		LPAD(CLEAN_STRING(HLDG_KEY),20,'0')                          AS HLDG_KEY,
		CLEAN_STRING(VOLTAGEACCESS(BEN_ARGMT_TXT,'freeform'))        AS BEN_ARGMT_TXT,
		CLEAN_STRING(BEN_ROW_CNTR_CD)                                AS BEN_ROW_CNTR_CD,
		NULL                                                         AS PARTY_PRIOR_ID,
		NULL                                                         AS SOR_PARTY_ID,
		'Beneficiary'                                                AS PARTY_ID_TYPE_CDE,
		'0001-01-01'::DATE                                           AS BEGIN_DT,
		'0001-01-01'::TIMESTAMP(6)                                   AS BEGIN_DTM,
		CURRENT_TIMESTAMP                                            AS ROW_PROCESS_DTM,
		:audit_id                                                    AS AUDIT_ID,
		FALSE                                                        AS LOGICAL_DELETE_IND,
		UUID_GEN(FALSE)::UUID                                        AS CHECK_SUM,
		TRUE                                                         AS CURRENT_ROW_IND,
		'12/31/9999'::DATE                                           AS END_DT,
		'12/31/9999 00:00:00'::TIMESTAMP(6)                          AS END_DTM,
		'238'                                                        AS SOURCE_SYSTEM_ID,
		FALSE                                                        AS RESTRICTED_ROW_IND,
		:audit_id                                                    AS UPDATE_AUDIT_ID,
		FALSE::BOOLEAN                                               AS SOURCE_DELETE_IND,
		CLEAN_STRING(CARR_ADMIN_SYS_CD)                              AS CARR_ADMIN_SYS_CD,
		UDF_ISNUM_LPAD(CLEAN_STRING(HLDG_KEY_PFX), 20, '0', TRUE)    AS HLDG_KEY_PFX,
		UDF_ISNUM_LPAD(CLEAN_STRING(HLDG_KEY_SFX), 20, '0', TRUE)    AS HLDG_KEY_SFX,
		ROW_NUMBER() OVER(PARTITION BY 
		CLEAN_STRING(CARR_ADMIN_SYS_CD),
		UDF_ISNUM_LPAD(CLEAN_STRING(HLDG_KEY_PFX), 20, '0', TRUE),
		LPAD(CLEAN_STRING(HLDG_KEY),20,'0'),
		UDF_ISNUM_LPAD(CLEAN_STRING(HLDG_KEY_SFX), 20, '0', TRUE),
		CLEAN_STRING(BEN_ROW_CNTR_CD)ORDER BY BEN_DATA_TO_DT DESC) RNK
		FROM PROD_STND_VW_TERSUN.BEN_DATA_VW SRC
		WHERE SRC.SRC_SYS_ID = 50 
	)SOURCE_DATASET WHERE RNK=1
)FINAL_DATASET;

COMMIT;

/* delete pe1 records  from party_master */

DELETE FROM EDW.PARTY_MASTER_OF_MASTERS_XREF WHERE SOURCE_SYSTEM_ID IN ('50','238');

COMMIT;

/* insert work records into edw table */

INSERT /*+direct*/ INTO EDW.PARTY_MASTER_OF_MASTERS_XREF
(
	 DIM_PARTY_NATURAL_KEY_HASH_UUID  
	,DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID  
	,PARTY_ID 
	,PARTY_PRIOR_ID  
	,PARTY_ID_TYPE_CDE 
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
	,SOURCE_DELETE_IND 
)
SELECT
 DIM_PARTY_NATURAL_KEY_HASH_UUID  
,DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID  
,PARTY_ID 
,PARTY_PRIOR_ID  
,PARTY_ID_TYPE_CDE 
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
,SOURCE_DELETE_IND  
FROM EDW_WORK.PARTY_CDALIFCOMLIFE_PARTY_MASTER_OF_MASTERS_XREF;

COMMIT;