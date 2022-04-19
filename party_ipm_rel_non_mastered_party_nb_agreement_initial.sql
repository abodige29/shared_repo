

DROP TABLE IF EXISTS TEMP_POL_VW;

CREATE LOCAL TEMPORARY TABLE TEMP_POL_VW ON COMMIT PRESERVE ROWS AS
SELECT * FROM EDW_VW.REL_NON_MASTERED_PARTY_NB_AGREEMENT_VW WHERE 1<>1;


INSERT /*DIRECT*/ INTO TEMP_POL_VW(
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
DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
UUID_GEN(PARTY_ID)::UUID AS DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
'0001-01-01'::DATE AS BEGIN_DT,
'0001-01-01'::TIMESTAMP(6) AS BEGIN_DTM,
CURRENT_TIMESTAMP(6) AS  ROW_PROCESS_DTM,
:audit_id AS AUDIT_ID,
FALSE AS LOGICAL_DELETE_IND,
UUID_GEN(SOURCE_DELETE_IND)::uuid AS CHECK_SUM,
TRUE AS CURRENT_ROW_IND,
'9999-12-31'::DATE AS  END_DT,
'9999-12-31'::TIMESTAMP(6) AS END_DTM,
'343' AS SOURCE_SYSTEM_ID,
FALSE AS RESTRICTED_ROW_IND,
:audit_id AS UPDATE_AUDIT_ID,
SOURCE_DELETE_IND
FROM(
SELECT
CLEAN_STRING(VOLTAGEACCESS(INSD_NM,'name')) AS INSIDE_NM,
CLEAN_STRING(VOLTAGEACCESS(INSD_MID_NM,'name')) AS INSIDE_MID_NM,
CLEAN_STRING(VOLTAGEACCESS(INSD_LST_NM,'name')) AS INSIDE_LST_NM,
UUID_GEN(CLEAN_STRING(ADMIN_SYS_CODE), 'IPA', NULL, BTRIM(POL_NR), NULL)::UUID AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
BTRIM(POL_NR) || INSIDE_NM || INSIDE_MID_NM || INSIDE_LST_NM ||'343'  AS PARTY_ID,
UUID_GEN('Insd')::uuid AS REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
FALSE AS SOURCE_DELETE_IND  FROM
PROD_NBR_VW_TERSUN.IPM_POL_VW WHERE SRC_SYS_ID='14'
)Q_1

COMMIT;

DROP TABLE IF EXISTS VT_ORDER_BY;

CREATE LOCAL TEMPORARY TABLE VT_ORDER_BY ON COMMIT PRESERVE ROWS AS 
SELECT *, ROW_NUMBER() OVER(PARTITION BY DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID, DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, 
            REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID ORDER BY END_DTM DESC, BEGIN_DTM DESC) AS RW_NUM 
FROM TEMP_POL_VW
ORDER BY DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID, 
DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, 
REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
END_DTM,
BEGIN_DTM;

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
A.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
A.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
A.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
A.BEGIN_DT,
A.BEGIN_DTM,
A.ROW_PROCESS_DTM,
A.AUDIT_ID,
A.LOGICAL_DELETE_IND,
A.CHECK_SUM,
CASE WHEN A.RW_NUM > B.RW_NUM AND B.RW_NUM = A.RW_NUM-1 AND A.CURRENT_ROW_IND=TRUE THEN FALSE ELSE A.CURRENT_ROW_IND END AS CURRENT_ROW_IND,
CASE WHEN A.RW_NUM>B.RW_NUM AND B.RW_NUM=A.RW_NUM-1 AND A.CURRENT_ROW_IND=TRUE THEN B.BEGIN_DT - INTERVAL '1' DAY 
         WHEN A.RW_NUM=1 AND A.CURRENT_ROW_IND=TRUE AND A.END_DT<>'9999-12-31' THEN '9999-12-31'::DATE
         ELSE A.END_DT END AS END_DT,
CASE WHEN A.RW_NUM>B.RW_NUM AND B.RW_NUM=A.RW_NUM-1 AND A.CURRENT_ROW_IND=TRUE THEN B.BEGIN_DTM - INTERVAL '1' SECOND 
         WHEN A.RW_NUM=1 AND A.CURRENT_ROW_IND=TRUE AND A.END_DT<>'9999-12-31' THEN '9999-12-31'::TIMESTAMP(6)
         ELSE A.END_DTM END AS END_DTM,
A.SOURCE_SYSTEM_ID,
A.RESTRICTED_ROW_IND,
A.UPDATE_AUDIT_ID,
A.SOURCE_DELETE_IND
FROM VT_ORDER_BY A
LEFT JOIN 
VT_ORDER_BY B
ON 
A.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID=B.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID AND 
A.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID=B.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID AND 
A.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID =B.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID AND 
A.RW_NUM = B.RW_NUM+1;

COMMIT;

DELETE FROM EDW.REL_NON_MASTERED_PARTY_NB_AGREEMENT
WHERE SOURCE_SYSTEM_ID IN ('14','343');

COMMIT;

INSERT /*DIRECT*/ INTO EDW.REL_NON_MASTERED_PARTY_NB_AGREEMENT
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
FROM 
EDW_WORK.PARTY_IPM_REL_NON_MASTERED_PARTY_NB_AGREEMENT;

COMMIT;
