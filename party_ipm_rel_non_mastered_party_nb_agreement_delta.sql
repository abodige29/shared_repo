/*
    FileName: party_ipm_rel_non_mastered_party_nb_agreement.sql
    Author: mma2156
    SUBJECT AREA : Party
    Table_Name :REL_NON_MASTERED_PARTY_NB_AGREEMENT
    SOURCE: ipm
    Teradata Source Code: 14                       
    Create Date:2022-04-20
        
    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    		               Party-Tier2         22/04           First Version Tier-2 
    ------------------------------------------------------------------------------------------------------------------
*/ 

TRUNCATE TABLE EDW_STAGING.PARTY_IPM_REL_NON_MASTERED_PARTY_NB_AGREEMENT_PRE_WORK;

INSERT
    /*DIRECT*/
    INTO
    EDW_STAGING.PARTY_IPM_REL_NON_MASTERED_PARTY_NB_AGREEMENT_PRE_WORK
    ( DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
    DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
    REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
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
    OPERATOR_IND )
SELECT
    DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
    DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
    REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
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
    OPERATOR_IND
FROM
    (
    SELECT
        DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
        UUID_GEN(LPAD(BTRIM(POL_NR),20,'0') || INSD_NM || INSD_MID_NM || INSD_LST_NM || SOURCE_SYSTEM_ID)::UUID AS DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
        REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
        BEGIN_DT,
        BEGIN_DTM,
        CURRENT_TIMESTAMP(6) AS ROW_PROCESS_DTM,
        :audit_id AS AUDIT_ID,
        FALSE AS LOGICAL_DELETE_IND,
        UUID_GEN(SOURCE_DELETE_IND)::uuid AS CHECK_SUM,
        TRUE AS CURRENT_ROW_IND,
        END_DT,
        END_DTM,
        SOURCE_SYSTEM_ID,
        FALSE AS RESTRICTED_ROW_IND,
        :audit_id AS UPDATE_AUDIT_ID,
        SOURCE_DELETE_IND,
        OPERATOR_IND,
        ROW_NUMBER() OVER (PARTITION BY DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
        DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
        REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID
    ORDER BY
        BEGIN_DTM DESC,
        END_DTM DESC) AS RNK
    FROM
        (
        SELECT
            POL_NR,
            CLEAN_STRING(INSD_NM) AS INSD_NM,
            CLEAN_STRING(INSD_MID_NM) AS INSD_MID_NM,
            CLEAN_STRING(INSD_LST_NM) AS INSD_LST_NM,
            UUID_GEN(CLEAN_STRING(FK_ADMN_SYS_CDE),CLEAN_STRING('IPM'),NULL,LPAD(BTRIM(POL_NR),20,'0'),NULL)::UUID AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
            UUID_GEN('Insd')::uuid AS REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
            DELTA_DT::DATE AS BEGIN_DT,
            DELTA_DT::TIMESTAMP(6) AS BEGIN_DTM,
            '9999-12-31'::DATE AS END_DT,
            '9999-12-31'::TIMESTAMP(6) AS END_DTM,
            '14' AS SOURCE_SYSTEM_ID,
            FALSE AS SOURCE_DELETE_IND ,
            CASE
                WHEN UPPER(BTRIM(DELTA_TYP))= 'D' THEN 'D'
                WHEN UPPER(BTRIM(DELTA_TYP))= 'U' THEN 'U'
                ELSE 'I'
            END AS OPERATOR_IND
        from
            EDW_STAGING.IPM_POL )Q_1 )Q_2
WHERE
    RNK = 1
    AND OPERATOR_IND <> 'D';
  
COMMIT;
   
INSERT
    /*DIRECT*/
    INTO
    EDW_STAGING.PARTY_IPM_REL_NON_MASTERED_PARTY_NB_AGREEMENT_PRE_WORK( 
    DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
    DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
    REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
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
    OPERATOR_IND )
SELECT
    DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
    DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
    REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
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
    OPERATOR_IND
FROM
    (
    SELECT
        DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
        UUID_GEN(LPAD(BTRIM(POL_NR),20,'0') || INSD_NM || INSD_MID_NM || INSD_LST_NM || SOURCE_SYSTEM_ID)::UUID AS DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
        REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
        BEGIN_DT,
        BEGIN_DTM,
        CURRENT_TIMESTAMP(6) AS ROW_PROCESS_DTM,
        :audit_id AS AUDIT_ID,
        FALSE AS LOGICAL_DELETE_IND,
        UUID_GEN(SOURCE_DELETE_IND)::uuid AS CHECK_SUM,
        FALSE  AS CURRENT_ROW_IND,
        END_DT,
        END_DTM,
        SOURCE_SYSTEM_ID,
        FALSE AS RESTRICTED_ROW_IND,
        :audit_id AS UPDATE_AUDIT_ID,
        SOURCE_DELETE_IND,
        OPERATOR_IND,
        ROW_NUMBER() OVER (PARTITION BY DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
        DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
        REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID
    ORDER BY
        BEGIN_DTM DESC,
        END_DTM DESC) AS RNK
    FROM
        (
        SELECT
            POL_NR,
            CLEAN_STRING(INSD_NM) AS INSD_NM,
            CLEAN_STRING(INSD_MID_NM) AS INSD_MID_NM,
            CLEAN_STRING(INSD_LST_NM) AS INSD_LST_NM,
            UUID_GEN(CLEAN_STRING(FK_ADMN_SYS_CDE),CLEAN_STRING('IPM'),NULL,LPAD(BTRIM(POL_NR),20,'0'),NULL)::UUID AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
            UUID_GEN('Insd')::uuid AS REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
            DELTA_DT::DATE AS BEGIN_DT,
            DELTA_DT::TIMESTAMP(6) AS BEGIN_DTM,
            '9999-12-31'::DATE AS END_DT,
            '9999-12-31'::TIMESTAMP(6) AS END_DTM,
            '14' AS SOURCE_SYSTEM_ID,
            TRUE AS SOURCE_DELETE_IND ,
            CASE
                WHEN UPPER(BTRIM(DELTA_TYP))= 'D' THEN 'D'
                WHEN UPPER(BTRIM(DELTA_TYP))= 'U' THEN 'U'
                ELSE 'I'
            END AS OPERATOR_IND
        from
            EDW_STAGING.IPM_POL )Q_1 )Q_2
WHERE
    RNK = 1
    AND OPERATOR_IND = 'D';
 
COMMIT;
/* WORK TABLE - INSERTS 
 * 
 * this script is used to load the records that don't have a record in target - TOTAL NEW INSERTS
 * */
TRUNCATE TABLE EDW_WORK.PARTY_IPM_REL_NON_MASTERED_PARTY_NB_AGREEMENT;


INSERT /*DIRECT*/ INTO EDW_WORK.PARTY_IPM_REL_NON_MASTERED_PARTY_NB_AGREEMENT
(
DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
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
SRC.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
SRC.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
SRC.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
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
SRC.SOURCE_DELETE_IND
FROM EDW_STAGING.PARTY_IPM_REL_NON_MASTERED_PARTY_NB_AGREEMENT_PRE_WORK SRC
LEFT JOIN
EDW.REL_NON_MASTERED_PARTY_NB_AGREEMENT TGT
ON SRC.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID=TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID AND
SRC.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID=TGT.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID AND
SRC.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID=TGT.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID
WHERE TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID IS NULL;

COMMIT;
/* WORK TABLE - UPDATE TGT RECORD
 * 
 * This script finds records where the new record from the source has a different check_sum than the current EDW.REL_NON_MASTERED_PARTY_NB_AGREEMENT record or the record is being ended/deleted. 
 * The current record in the target will be ended since the source record will be inserted in the next step.
 * */
INSERT /*DIRECT*/ INTO EDW_WORK.PARTY_IPM_REL_NON_MASTERED_PARTY_NB_AGREEMENT
(
DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
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
TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
TGT.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
TGT.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
TGT.BEGIN_DT,
TGT.BEGIN_DTM,
CURRENT_TIMESTAMP(6) AS ROW_PROCESS_DTM,
TGT.AUDIT_ID,
TGT.LOGICAL_DELETE_IND,
TGT.CHECK_SUM,
FALSE AS CURRENT_ROW_IND,
SRC.BEGIN_DT - INTERVAL '1' DAY AS END_DT,
SRC.BEGIN_DTM - INTERVAL '1' SECOND AS  END_DTM,
TGT.SOURCE_SYSTEM_ID,
TGT.RESTRICTED_ROW_IND,
TGT.UPDATE_AUDIT_ID,
TGT.SOURCE_DELETE_IND
FROM
EDW_STAGING.PARTY_IPM_REL_NON_MASTERED_PARTY_NB_AGREEMENT_PRE_WORK SRC
LEFT JOIN
EDW.REL_NON_MASTERED_PARTY_NB_AGREEMENT TGT
ON SRC.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID=TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID AND
SRC.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID=TGT.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID AND
SRC.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID=TGT.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID AND
TGT.CURRENT_ROW_IND=TRUE
WHERE SRC.CHECK_SUM<>TGT.CHECK_SUM;

COMMIT;
/* WORK TABLE - UPDATE WHERE RECORD ALREADY EXISTS IN target 
 *  
 * */
INSERT /*DIRECT*/ INTO EDW_WORK.PARTY_IPM_REL_NON_MASTERED_PARTY_NB_AGREEMENT
(
DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
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
SRC.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
SRC.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
SRC.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
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
SRC.SOURCE_DELETE_IND
FROM
EDW_STAGING.PARTY_IPM_REL_NON_MASTERED_PARTY_NB_AGREEMENT_PRE_WORK SRC
LEFT JOIN
EDW_VW.REL_NON_MASTERED_PARTY_NB_AGREEMENT_VW TGT
ON SRC.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID=TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID AND
SRC.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID=TGT.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID AND
SRC.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID=TGT.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID 
WHERE(  --handle when there isn't a current record in target but there are historical records and a delta coming through
       TGT.ROW_SID IS NULL AND
       (SRC.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
        SRC.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
        SRC.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID) IN
        (SELECT DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID
        FROM EDW_VW.REL_NON_MASTERED_PARTY_NB_AGREEMENT_VW)
        )
        OR(--check_sum has changed
        TGT.ROW_SID IS NOT NULL AND SRC.CHECK_SUM<>TGT.CHECK_SUM );
        
       
COMMIT;