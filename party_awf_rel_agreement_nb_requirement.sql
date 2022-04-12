/*
    FileName: party_awf_rel_agreement_nb_requirement.sql
    Author: mma2156
    SUBJECT AREA : Party
    Table_Name :rel_agreement_nb_requirement
    SOURCE: AWF
    Teradata Source Code: 82                        
    Create Date:2022-02-04
        
    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    JIRA 3952               Party-Tier2         02/09           First Version Tier-2 
    ------------------------------------------------------------------------------------------------------------------
    */ 

DROP TABLE IF EXISTS TEMP_PREWORK;

CREATE LOCAL TEMPORARY TABLE TEMP_PREWORK ON COMMIT PRESERVE ROWS AS 
SELECT * FROM EDW_WORK.PARTY_TPP_REL_AGREEMENT_NB_REQUIREMENT 
WHERE 1<>1;


SELECT ANALYZE_STATISTICS('PROD_NBR_VW_TERSUN.NB_REQ_VW');
SELECT ANALYZE_STATISTICS('PROD_NBR_VW_TERSUN.NB_APPL_VW');

DROP TABLE IF EXISTS TEMP_NB_APPL_VW;

--loading NB_APPL_VW data into temp table
CREATE LOCAL TEMPORARY TABLE TEMP_NB_APPL_VW ON COMMIT PRESERVE ROWS AS 
/* +DIRECT */
SELECT APPL_ID, HLDG_KEY, HLDG_KEY_PFX, HLDG_KEY_SFX, CARR_ADMIN_SYS_CD,CAS_ID
	FROM (
	SELECT APPL_ID, 
	CLEAN_STRING(HLDG_KEY) AS HLDG_KEY, 
	CLEAN_STRING(HLDG_KEY_PFX) AS HLDG_KEY_PFX, 
	CLEAN_STRING(HLDG_KEY_SFX) AS HLDG_KEY_SFX, 
	CLEAN_STRING(CARR_ADMIN_SYS_CD) AS CARR_ADMIN_SYS_CD,
	CAS_ID AS CAS_ID,
	ROW_NUMBER() OVER(PARTITION BY APPL_ID,HLDG_KEY,HLDG_KEY_PFX,CARR_ADMIN_SYS_CD ORDER BY  APPL_DATA_FR_DT DESC) AS RNK
	FROM PROD_NBR_VW_TERSUN.NB_APPL_VW WHERE  SRC_SYS_ID='82' 
	)Q_1
 WHERE RNK=1;


SELECT ANALYZE_STATISTICS('TEMP_NB_APPL_VW ');

COMMIT;

DROP TABLE IF EXISTS TEMP_NB_REQ_VW;

--loading NB_REQ_VW data into temp table
CREATE LOCAL TEMPORARY TABLE TEMP_NB_REQ_VW ON COMMIT PRESERVE ROWS AS 
/* +DIRECT */
SELECT 
	APPL_ID AS APPL_ID,
	COALESCE(CLEAN_STRING(REQ_CD), 'UNK') AS REQ_CD,
	CLEAN_STRING(CAS_REQ_ID) AS CAS_REQ_ID,
	CLEAN_STRING(REQ_COLL_METH) AS REQ_COLL_METH,
	CLEAN_STRING(REQ_CTG) AS REQ_CTG,
	CLEAN_STRING(VOLTAGEACCESS(REQ_CMNT,'freeform')) AS REQ_CMNT,
	CLEAN_STRING(REQ_STUS_CD) AS REQ_STUS_CD,
	CLEAN_STRING(SRC_REQ_STUS_CD) AS SRC_REQ_STUS_CD,
	REQ_ORDR_DT  AS REQ_ORDR_DT,
	CLEAN_STRING(SRC_WRKBNCH_COLL_ID) AS SRC_WRKBNCH_COLL_ID,
	CLEAN_STRING(WRKBNCH_COLL_METH) AS WRKBNCH_COLL_METH,
	CLEAN_STRING(VOLTAGEACCESS(PHY_NM,'name')) AS PHY_NM,
	CLEAN_STRING(SRC_REQ_ID) AS SRC_REQ_ID,
	REQ_STUS_DT AS REQ_STUS_DT,
	CLEAN_STRING(SRC_REQ_CTG) AS SRC_REQ_CTG,
	REQ_FR_DT AS REQ_FR_DT,
	--REQ_FR_DT::TIMESTAMP(6) AS REQ_FR_DT,
	REQ_TO_DT AS REQ_TO_DT,
	--REQ_TO_DT::TIMESTAMP(6) AS REQ_TO_DT,
	CLEAN_STRING(SRC_DEL_IND) AS SRC_DEL_IND
	FROM 
	PROD_NBR_VW_TERSUN.NB_REQ_VW
WHERE SRC_SYS_ID ='82';


SELECT ANALYZE_STATISTICS('TEMP_NB_REQ_VW');

COMMIT;

DROP TABLE IF EXISTS TEMP_SRC;

--joining above tables into another temp table according to lookupstructure
CREATE LOCAL TEMPORARY TABLE TEMP_SRC ON COMMIT PRESERVE ROWS AS 
/* +DIRECT */
SELECT 
	LPAD(appl.HLDG_KEY,20,'0')               AS HLDG_KEY,
	appl.HLDG_KEY_PFX                        AS HLDG_KEY_PFX,
	appl.HLDG_KEY_SFX                        AS HLDG_KEY_SFX,
	COALESCE(appl.CARR_ADMIN_SYS_CD,'Unk')   AS CARR_ADMIN_SYS_CD,
	appl.CAS_ID                              AS CAS_ID,
	src.REQ_CD                               AS REQ_CD,
	src.CAS_REQ_ID                           AS CAS_REQ_ID,
	src.REQ_COLL_METH                        AS REQ_COLL_METH,
	src.REQ_CTG                              AS REQ_CTG,
	src.REQ_CMNT                             AS REQ_CMNT,
	src.REQ_STUS_CD                          AS REQ_STUS_CD,
	src.SRC_REQ_STUS_CD                      AS SRC_REQ_STUS_CD,
	src.REQ_ORDR_DT                          AS REQ_ORDR_DT, 
	src.SRC_WRKBNCH_COLL_ID                  AS SRC_WRKBNCH_COLL_ID,
	src.WRKBNCH_COLL_METH                    AS WRKBNCH_COLL_METH,
	src.PHY_NM                               AS PHY_NM,   
	src.SRC_REQ_ID                           AS SRC_REQ_ID, 
	src.REQ_STUS_DT                          AS REQ_STUS_DT,
	src.SRC_REQ_CTG                          AS SRC_REQ_CTG,
	src.REQ_FR_DT                            AS REQ_FR_DT,
	src.REQ_TO_DT                            AS REQ_TO_DT,
	src.SRC_DEL_IND                          AS SRC_DEL_IND 
	FROM TEMP_NB_REQ_VW src
	LEFT JOIN TEMP_NB_APPL_VW appl
	ON src.APPL_ID = appl.APPL_ID;

SELECT ANALYZE_STATISTICS('TEMP_SRC');

COMMIT;

--moving source data into prework table 
INSERT /*+DIRECT*/ INTO TEMP_PREWORK(
	DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
    REF_REQUIREMENT_TYPE_NATURAL_KEY_HASH_UUID,
    REQUIREMENT_CASE_ID,
    COLLECTION_METHORD_CDE,
    REQUIREMENT_CATEGORY_CDE,
    REQUIREMENT_COMMENT_TXT,
    REQUIREMENT_STATUS_CDE,
    SOURCE_REQUIREMENT_STATUS_CDE,
    REQUIREMENT_ORDER_DT,
    WORKBENCH_COLLECTION_ID,
    WORKBENCH_COLLECTION_METHORD_CDE,
    PHYSICIAN_FULL_NM,
    SOURCE_REQUIREMENT_CDE,
    REQUIREMENT_STATUS_DT,
    SOURCE_REQUIREMENT_CATEGORY, 
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
	DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
    REF_REQUIREMENT_TYPE_NATURAL_KEY_HASH_UUID,
    REQUIREMENT_CASE_ID,
    COLLECTION_METHORD_CDE,
    REQUIREMENT_CATEGORY_CDE,
    REQUIREMENT_COMMENT_TXT,
    REQUIREMENT_STATUS_CDE,
    SOURCE_REQUIREMENT_STATUS_CDE,
    REQUIREMENT_ORDER_DT,
    WORKBENCH_COLLECTION_ID,
    WORKBENCH_COLLECTION_METHORD_CDE,
    PHYSICIAN_FULL_NM,
    SOURCE_REQUIREMENT_CDE,
    REQUIREMENT_STATUS_DT,
    SOURCE_REQUIREMENT_CATEGORY, 
    BEGIN_DT,
    BEGIN_DTM,
    ROW_PROCESS_DTM,
    AUDIT_ID,
    LOGICAL_DELETE_IND,
    UUID_GEN(SOURCE_DELETE_IND, COLLECTION_METHORD_CDE, REQUIREMENT_CATEGORY_CDE,REQUIREMENT_COMMENT_TXT,REQUIREMENT_STATUS_CDE, SOURCE_REQUIREMENT_STATUS_CDE, 
             REQUIREMENT_ORDER_DT, WORKBENCH_COLLECTION_ID, WORKBENCH_COLLECTION_METHORD_CDE, PHYSICIAN_FULL_NM,REQUIREMENT_STATUS_DT,
             SOURCE_REQUIREMENT_CATEGORY )::UUID AS CHECK_SUM,
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
    UUID_GEN(CARR_ADMIN_SYS_CD, 'Appl', HLDG_KEY_PFX, HLDG_KEY, HLDG_KEY_SFX,'0')::UUID  AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, 
    UUID_GEN(REQ_CD)::UUID     AS REF_REQUIREMENT_TYPE_NATURAL_KEY_HASH_UUID,
    CAS_REQ_ID                 AS REQUIREMENT_CASE_ID,
    REQ_COLL_METH              AS COLLECTION_METHORD_CDE,
    REQ_CTG                    AS REQUIREMENT_CATEGORY_CDE,
    VOLTAGEPROTECT(REQ_CMNT,'freeform')  AS REQUIREMENT_COMMENT_TXT,
    REQ_STUS_CD                AS REQUIREMENT_STATUS_CDE,
    SRC_REQ_STUS_CD            AS SOURCE_REQUIREMENT_STATUS_CDE,
    REQ_ORDR_DT                AS REQUIREMENT_ORDER_DT,
    SRC_WRKBNCH_COLL_ID        AS WORKBENCH_COLLECTION_ID,
    WRKBNCH_COLL_METH          AS WORKBENCH_COLLECTION_METHORD_CDE,
    VOLTAGEPROTECT(PHY_NM,'name')        AS PHYSICIAN_FULL_NM,
    SRC_REQ_ID                 AS SOURCE_REQUIREMENT_CDE,
    REQ_STUS_DT::DATE          AS REQUIREMENT_STATUS_DT,
    SRC_REQ_CTG                AS SOURCE_REQUIREMENT_CATEGORY,
    REQ_FR_DT::DATE            AS BEGIN_DT,
    REQ_FR_DT::TIMESTAMP(6)    AS BEGIN_DTM,
    CURRENT_TIMESTAMP(6)       AS ROW_PROCESS_DTM,
    :audit_id                  AS AUDIT_ID,
    FALSE::BOOLEAN             AS LOGICAL_DELETE_IND,
    CASE WHEN REQ_TO_DT::DATE='12-31-9999' THEN TRUE ELSE FALSE END AS CURRENT_ROW_IND,
    REQ_TO_DT::DATE            AS END_DT,
    REQ_TO_DT::TIMESTAMP(6)    AS END_DTM,
    '341'                      AS SOURCE_SYSTEM_ID,
    FALSE::BOOLEAN             AS RESTRICTED_ROW_IND,
    :audit_id                  AS UPDATE_AUDIT_ID,
    CASE WHEN SRC_DEL_IND='Y' THEN TRUE ELSE FALSE END AS SOURCE_DELETE_IND
    FROM TEMP_SRC
	)Q_1;


SELECT ANALYZE_STATISTICS('TEMP_PREWORK');

COMMIT;

 
DROP TABLE IF EXISTS VT_ORDER_BY;


/* generate RW_NUM to calculate the end_dt */
CREATE LOCAL TEMPORARY TABLE VT_ORDER_BY ON COMMIT PRESERVE ROWS AS 
SELECT *, ROW_NUMBER() OVER(PARTITION BY DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
REF_REQUIREMENT_TYPE_NATURAL_KEY_HASH_UUID, REQUIREMENT_CASE_ID,SOURCE_REQUIREMENT_CDE
ORDER BY BEGIN_DTM ,END_DTM ) AS RW_NUM 
FROM TEMP_PREWORK
ORDER BY DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, 
REF_REQUIREMENT_TYPE_NATURAL_KEY_HASH_UUID, 
REQUIREMENT_CASE_ID,
END_DTM,
BEGIN_DTM;


SELECT ANALYZE_STATISTICS('VT_ORDER_BY');

COMMIT;


TRUNCATE TABLE EDW_WORK.PARTY_TPP_REL_AGREEMENT_NB_REQUIREMENT;


/* insert from temp to work and calculate current_row_ind, End_dt and End_dtm as per Standards EDW_WORK.PARTY_TPP_REL_AGREEMENT_NB_REQUIREMENT*/
INSERT INTO EDW_WORK.PARTY_TPP_REL_AGREEMENT_NB_REQUIREMENT
(
    DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
    REF_REQUIREMENT_TYPE_NATURAL_KEY_HASH_UUID,
    REQUIREMENT_CASE_ID,
    COLLECTION_METHORD_CDE,
    REQUIREMENT_CATEGORY_CDE,
    REQUIREMENT_COMMENT_TXT,
    REQUIREMENT_STATUS_CDE,
    SOURCE_REQUIREMENT_STATUS_CDE,
    REQUIREMENT_ORDER_DT,
    WORKBENCH_COLLECTION_ID,
    WORKBENCH_COLLECTION_METHORD_CDE,
    PHYSICIAN_FULL_NM,
    SOURCE_REQUIREMENT_CDE,
    REQUIREMENT_STATUS_DT,
    SOURCE_REQUIREMENT_CATEGORY, 
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
    A.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
    A.REF_REQUIREMENT_TYPE_NATURAL_KEY_HASH_UUID,
    A.REQUIREMENT_CASE_ID,
    A.COLLECTION_METHORD_CDE,
    A.REQUIREMENT_CATEGORY_CDE,
    A.REQUIREMENT_COMMENT_TXT,
    A.REQUIREMENT_STATUS_CDE,
    A.SOURCE_REQUIREMENT_STATUS_CDE,
    A.REQUIREMENT_ORDER_DT,
    A.WORKBENCH_COLLECTION_ID,
    A.WORKBENCH_COLLECTION_METHORD_CDE,
    A.PHYSICIAN_FULL_NM,
    A.SOURCE_REQUIREMENT_CDE,
    A.REQUIREMENT_STATUS_DT,
    A.SOURCE_REQUIREMENT_CATEGORY, 
    A.BEGIN_DT,
    A.BEGIN_DTM,
    A.ROW_PROCESS_DTM,
    A.AUDIT_ID,
    A.LOGICAL_DELETE_IND,
    A.CHECK_SUM,
	CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 THEN FALSE ELSE A.CURRENT_ROW_IND END AS CURRENT_ROW_IND,
   CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 AND  A.END_DT > B.BEGIN_DT - INTERVAL '1' DAY THEN B.BEGIN_DT - INTERVAL '1' DAY 
      ELSE A.END_DT END AS END_DT,
    CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 AND  A.END_DTM > B.BEGIN_DTM - INTERVAL '1' SECOND THEN B.BEGIN_DTM - INTERVAL '1' SECOND 
      ELSE A.END_DTM END AS END_DTM, 
    A.SOURCE_SYSTEM_ID,
    A.RESTRICTED_ROW_IND,
    A.UPDATE_AUDIT_ID,
    A.SOURCE_DELETE_IND
FROM VT_ORDER_BY A 
LEFT JOIN VT_ORDER_BY B 
ON A.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID = B.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
AND A.REF_REQUIREMENT_TYPE_NATURAL_KEY_HASH_UUID = B.REF_REQUIREMENT_TYPE_NATURAL_KEY_HASH_UUID
AND COALESCE(A.REQUIREMENT_CASE_ID,'-') = COALESCE(B.REQUIREMENT_CASE_ID,'-')
AND COALESCE(A.SOURCE_REQUIREMENT_CDE,'-')=COALESCE(B.SOURCE_REQUIREMENT_CDE,'-')
AND A.RW_NUM = B.RW_NUM-1;

COMMIT;


DELETE FROM EDW.REL_AGREEMENT_NB_REQUIREMENTS WHERE SOURCE_SYSTEM_ID IN ('341');

COMMIT;

/* insert into target table from work */

INSERT INTO  EDW.REL_AGREEMENT_NB_REQUIREMENT 
(
    DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
    REF_REQUIREMENT_TYPE_NATURAL_KEY_HASH_UUID,
    REQUIREMENT_CASE_ID,
    COLLECTION_METHORD_CDE,
    REQUIREMENT_CATEGORY_CDE,
    REQUIREMENT_COMMENT_TXT,
    REQUIREMENT_STATUS_CDE,
    SOURCE_REQUIREMENT_STATUS_CDE,
    REQUIREMENT_ORDER_DT,
    WORKBENCH_COLLECTION_ID,
    WORKBENCH_COLLECTION_METHORD_CDE,
    PHYSICIAN_FULL_NM,
    SOURCE_REQUIREMENT_CDE,
    REQUIREMENT_STATUS_DT,
    SOURCE_REQUIREMENT_CATEGORY, 
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
    DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
    REF_REQUIREMENT_TYPE_NATURAL_KEY_HASH_UUID,
    REQUIREMENT_CASE_ID,
    COLLECTION_METHORD_CDE,
    REQUIREMENT_CATEGORY_CDE,
    REQUIREMENT_COMMENT_TXT,
    REQUIREMENT_STATUS_CDE,
    SOURCE_REQUIREMENT_STATUS_CDE,
    REQUIREMENT_ORDER_DT,
    WORKBENCH_COLLECTION_ID,
    WORKBENCH_COLLECTION_METHORD_CDE,
    PHYSICIAN_FULL_NM,
    SOURCE_REQUIREMENT_CDE,
    REQUIREMENT_STATUS_DT,
    SOURCE_REQUIREMENT_CATEGORY, 
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
FROM EDW_WORK.PARTY_TPP_REL_AGREEMENT_NB_REQUIREMENT;

COMMIT;















