/*
    FileName: party_cdalifcomlife_rel_party_agreement.sql
    Author: MM14295
    Subject Area : Party
    Source: CDA LIFCOM LIFE
    Create Date:2021-08-26
       
    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    TERSUN-3432             Party-Tier2        05/31           Initial version      
    ------------------------------------------------------------------------------------------------------------------
*/

TRUNCATE TABLE EDW_STAGING.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT_PRE_WORK;

TRUNCATE TABLE EDW_WORK.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT;

 
---------- GENERATING UUID FOR CONTRACT AGREEMENTS TO JOIN WITH BENE TABLE ------------

DROP TABLE IF EXISTS TEMP_LIFCOM_CTRT_DIM_AGREEMENT_TBL ;

CREATE LOCAL TEMPORARY TABLE TEMP_LIFCOM_CTRT_DIM_AGREEMENT_TBL ON COMMIT PRESERVE ROWS AS
SELECT 
		CASE WHEN ctrt_row_adm_sys_name is null 
			and ctrt_row_ctrt_prefix is null 
			and ctrt_row_ctrt_no is null 
			and ctrt_row_ctrt_suffix is null 
			THEN UUID_GEN(NULL)::UUID
			ELSE UUID_GEN(ctrt_row_adm_sys_name, 'Ipa', ctrt_row_ctrt_prefix, ctrt_row_ctrt_no, ctrt_row_ctrt_suffix)::UUID
		END	AS 	DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
FROM (			
		SELECT DISTINCT
		        CLEAN_STRING(ctrt_row_adm_sys_name)		AS CTRT_ROW_ADM_SYS_NAME, 
				UDF_ISNUM_LPAD(CLEAN_STRING(ctrt_row_ctrt_prefix), 20, '0', TRUE)		AS CTRT_ROW_CTRT_PREFIX,
				LPAD(CLEAN_STRING(ctrt_row_ctrt_no),20,'0')		AS CTRT_ROW_CTRT_NO,
				UDF_ISNUM_LPAD(CLEAN_STRING(ctrt_row_ctrt_suffix), 20, '0', TRUE)		AS CTRT_ROW_CTRT_SUFFIX
		FROM EXT_EDAP_STAGING.CDA_LIFCOM_LIFE_EDW_CONTRACT_FULL SRC1 
		
		UNION
		
		SELECT DISTINCT
				        CLEAN_STRING(ctrt_row_adm_sys_name)		AS CTRT_ROW_ADM_SYS_NAME, 
						UDF_ISNUM_LPAD(CLEAN_STRING(ctrt_row_ctrt_prefix), 20, '0', TRUE)		AS CTRT_ROW_CTRT_PREFIX,
						LPAD(CLEAN_STRING(ctrt_row_ctrt_no),20,'0')		AS CTRT_ROW_CTRT_NO,
						UDF_ISNUM_LPAD(CLEAN_STRING(ctrt_row_ctrt_suffix), 20, '0', TRUE)		AS CTRT_ROW_CTRT_SUFFIX
		FROM EXT_EDAP_STAGING.CDA_LIFCOM_LIFE_EDW_CONTRACT_DELTA SRC2
		WHERE PROCESS_IND='D'
		
	  ) A ;
	  
	  
-------- CREATING AGRMNT_OUT TEMP TABLE ----------

DROP TABLE IF EXISTS TEMP_ECDM_C_B_PARTY_AGRMNT_OUT_SNAPSHOT;

CREATE LOCAL TEMPORARY TABLE TEMP_ECDM_C_B_PARTY_AGRMNT_OUT_SNAPSHOT ON COMMIT PRESERVE ROWS AS
/*+DIRECT*/
SELECT *
FROM EDW_STAGING.ECDM_C_B_PARTY_AGRMNT_OUT_SNAPSHOT
ORDER BY CARR_ADMN_SYS_CD,ROLE_TP,SUB_ROLE_TP,HLDG_KEY_PFX,HLDG_KEY_SFX,AGRMNT_NUM,MEMBER_ID
SEGMENTED BY hash(AGRMNT_NUM) ALL NODES;


UPDATE TEMP_ECDM_C_B_PARTY_AGRMNT_OUT_SNAPSHOT
SET AGRMNT_NUM = CLEAN_STRING(VOLTAGEACCESS(AGRMNT_NUM,'ssn_char')),
    MEMBER_ID  = CLEAN_STRING(VOLTAGEACCESS(MEMBER_ID,'sorparty')) ;
 
COMMIT ;

-------- CREATING BENE_DSGN_OUT TEMP TABLE ----------

DROP TABLE IF EXISTS TEMP_ECDM_C_B_BENE_DSGN_OUT_SNAPSHOT;

CREATE LOCAL TEMPORARY TABLE TEMP_ECDM_C_B_BENE_DSGN_OUT_SNAPSHOT ON COMMIT PRESERVE ROWS AS
/*+DIRECT*/
SELECT *
FROM EDW_STAGING.ECDM_C_B_BENE_DSGN_OUT_SNAPSHOT
ORDER BY CARR_ADMN_SYS_CD,ROLE_TP,SUB_ROLE_TP,HLDG_KEY_PFX,HLDG_KEY_SFX,AGRMNT_NUM,MEMBER_ID
SEGMENTED BY hash(AGRMNT_NUM) ALL NODES;


UPDATE TEMP_ECDM_C_B_BENE_DSGN_OUT_SNAPSHOT
SET AGRMNT_NUM = CLEAN_STRING(VOLTAGEACCESS(AGRMNT_NUM,'ssn_char')),
    MEMBER_ID  = CLEAN_STRING(VOLTAGEACCESS(MEMBER_ID,'sorparty')) ;
 
COMMIT ;
 
----------- PULLING ACTIVE AGREEMENTS FROM REL_PARTY_AGREEMENT CORE TABLE WHICH ARE LOADED BY SOURCE ECDM_C_B_BENE_DSGN_OUT TABLE ------------

/* PULL ALL, BOTH ACTIVE/DELETED, BENE AGREEMENTS INTO VT */

DROP TABLE IF EXISTS VT_BENE_AGREEMENTS;

CREATE LOCAL TEMPORARY TABLE VT_BENE_AGREEMENTS ON COMMIT PRESERVE ROWS AS
/*+DIRECT*/
SELECT DISTINCT UUID_GEN(CLEAN_STRING(AGT.CARR_ADMN_SYS_CD), 
						 CASE WHEN CLEAN_STRING(AGT.HLDG_KEY_SFX) IN ('Mca') THEN CLEAN_STRING('MCA') ELSE CLEAN_STRING('IPA') END,
						 CLEAN_STRING(AGT.HLDG_KEY_PFX),
						 CLEAN_STRING(AGT.AGRMNT_NUM),
						 CLEAN_STRING(AGT.HLDG_KEY_SFX) )::UUID    AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
	            UUID_GEN(BTRIM(AGT.MEMBER_ID))::UUID                   AS DIM_PARTY_NATURAL_KEY_HASH_UUID, 
	            UUID_GEN(CLEAN_STRING(AGT.ROLE_TP))::UUID              AS REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
	            UUID_GEN(CLEAN_STRING(AGT.SUB_ROLE_TP))::UUID          AS REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
	            AGT.CARR_ADMN_SYS_CD,
	            AGT.HLDG_KEY_PFX,
	            AGT.AGRMNT_NUM,
	            AGT.HLDG_KEY_SFX,
	            AGT.MEMBER_ID,
	            AGT.ROLE_TP,
	            AGT.SUB_ROLE_TP,
	            BENE.BENE_SUB_CLASS_NR
FROM TEMP_ECDM_C_B_PARTY_AGRMNT_OUT_SNAPSHOT AGT
INNER JOIN TEMP_ECDM_C_B_BENE_DSGN_OUT_SNAPSHOT BENE
   ON    
       COALESCE(CLEAN_STRING(AGT.HLDG_KEY_PFX),'UNK') = COALESCE(CLEAN_STRING(BENE.HLDG_KEY_PFX),'UNK')
   AND COALESCE(AGT.AGRMNT_NUM,'UNK') = COALESCE(BENE.AGRMNT_NUM,'UNK')
   AND COALESCE(CLEAN_STRING(AGT.HLDG_KEY_SFX),'UNK') = COALESCE(CLEAN_STRING(BENE.HLDG_KEY_SFX),'UNK')
   AND COALESCE(AGT.MEMBER_ID,'UNK') = COALESCE(BENE.MEMBER_ID,'UNK')
   AND COALESCE(CLEAN_STRING(AGT.CARR_ADMN_SYS_CD),'UNK') = COALESCE(CLEAN_STRING(BENE.CARR_ADMN_SYS_CD),'UNK')
   AND COALESCE(CLEAN_STRING(AGT.ROLE_TP),'UNK') = COALESCE(CLEAN_STRING(BENE.ROLE_TP),'UNK')
   AND COALESCE(CLEAN_STRING(AGT.SUB_ROLE_TP),'UNK') = COALESCE(CLEAN_STRING(BENE.SUB_ROLE_TP),'UNK')
ORDER BY
BENE_SUB_CLASS_NR,
REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
DIM_PARTY_NATURAL_KEY_HASH_UUID;

COMMIT; 

/* PULL ACTIVE ECDM AGREEMENTS FROM RPA */ 

DROP TABLE IF EXISTS VT_DIM_AGREEMENTS; 

CREATE LOCAL TEMPORARY TABLE VT_DIM_AGREEMENTS ON COMMIT PRESERVE ROWS AS
/*+DIRECT*/
SELECT RPA.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
FROM edw_vw.rel_party_agreement_vw RPA 
JOIN VT_BENE_AGREEMENTS BENE 
	ON RPA.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID = BENE.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
	AND RPA.DIM_PARTY_NATURAL_KEY_HASH_UUID = BENE.DIM_PARTY_NATURAL_KEY_HASH_UUID
	AND RPA.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID = BENE.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID
	AND RPA.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID = BENE.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID
	AND COALESCE(RPA.BENEFICIARY_SUB_CLASS_NR,'-999') = COALESCE(BENE.BENE_SUB_CLASS_NR,'-999')
	AND RPA.CURRENT_ROW_IND 
	AND RPA.SOURCE_SYSTEM_ID IN ('230','45');

COMMIT;

	
/* INSERT SCRIPT FOR PRE WORK TABLE  */



INSERT INTO EDW_STAGING.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT_PRE_WORK
(
	DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
	DIM_PARTY_NATURAL_KEY_HASH_UUID,
	REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
	REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
	DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
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
	DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
	ADDRESS_TYPE_CDE,
	ATTENTION_LINE_TXT,
	DIM_PHONE_NATURAL_KEY_HASH_UUID,
	DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
	DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
	BENEFICIARY_SUB_CLASS_NR,
	BENEFICIARY_RELATIONSHIP_NM,
	BENEFICIARY_ALLOCATION_PCT,
	BENEFICIARY_ALLOCATION_AMT,
	BENEFICIARY_AGREEMENT_TXT,
	SOURCE_PARTY_ROLE_CDE,
	SOURCE_PARTY_SUB_ROLE_CDE,
	SOURCE_ADDRESS_TYPE_CDE,
	SOURCE_DELETE_IND,
	BENEFICIARY_ISS_PER_STIRPES_CDE,
	BUSINESS_STRT_DT,
	BUSINESS_END_DT,
	ADVISOR_FIRST_YEAR_COMMISSION,
	ADVISOR_RENEWAL_COMMISSION,
	ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	SOLICIT_WRITING_AGENT_IND,
	SERVICE_AGENT_IND,
	CAREER_CORPORATION_IND,
	SOURCE_PACKAGE_ID,
	SOURCE_STATUS_CDE,
	SOURCE_STATUS_REASON_CDE,
	AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
	ADVISOR_ORDER_NR,
	AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	ADVISOR_COMMISSION_PCT,
	BENEFICIARY_ROLE_POSITION_CDE,
	SOURCE_BENEFICIARY_ROLE_CDE,
	SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
	BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	BENEFICIARY_EFFECTIVE_DT,
	BENEFICIARY_TRUST_DT,
	BENEFICIARY_SPECIAL_DESTINATION_CDE,
	BENEFICIARY_ROW_CONTROL_CDE,
	BENEFICIARY_DESTINATION_AMT,
	DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	BENEFICIARY_CLASS_CDE,
	BENEFICIARY_DESIGNATION_TXT,
	BENEFICIARY_NAME_FORMAT_CDE,
	OPERATOR_IND,
	beneficiary_unformatted_nm
)
SELECT 
	DEDUP.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
	DIM_PARTY_NATURAL_KEY_HASH_UUID,
	REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
	REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
	DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
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
	DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
	ADDRESS_TYPE_CDE,
	ATTENTION_LINE_TXT,
	DIM_PHONE_NATURAL_KEY_HASH_UUID,
	DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
	DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
	BENEFICIARY_SUB_CLASS_NR,
	BENEFICIARY_RELATIONSHIP_NM,
	BENEFICIARY_ALLOCATION_PCT,
	BENEFICIARY_ALLOCATION_AMT,
	BENEFICIARY_AGREEMENT_TXT,
	SOURCE_PARTY_ROLE_CDE,
	SOURCE_PARTY_SUB_ROLE_CDE,
	SOURCE_ADDRESS_TYPE_CDE,
	SOURCE_DELETE_IND,
	BENEFICIARY_ISS_PER_STIRPES_CDE,
	BUSINESS_STRT_DT,
	BUSINESS_END_DT,
	ADVISOR_FIRST_YEAR_COMMISSION,
	ADVISOR_RENEWAL_COMMISSION,
	ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	SOLICIT_WRITING_AGENT_IND,
	SERVICE_AGENT_IND,
	CAREER_CORPORATION_IND,
	SOURCE_PACKAGE_ID,
	SOURCE_STATUS_CDE,
	SOURCE_STATUS_REASON_CDE,
	AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
	ADVISOR_ORDER_NR,
	AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	ADVISOR_COMMISSION_PCT,
	BENEFICIARY_ROLE_POSITION_CDE,
	SOURCE_BENEFICIARY_ROLE_CDE,
	SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
	BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	BENEFICIARY_EFFECTIVE_DT,
	BENEFICIARY_TRUST_DT,
	BENEFICIARY_SPECIAL_DESTINATION_CDE,
	BENEFICIARY_ROW_CONTROL_CDE,
	BENEFICIARY_DESTINATION_AMT,
	DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	BENEFICIARY_CLASS_CDE,
	BENEFICIARY_DESIGNATION_TXT,
	BENEFICIARY_NAME_FORMAT_CDE,
	OPERATOR_IND,
	beneficiary_unformatted_nm
FROM 
(
	SELECT 
		DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
		DIM_PARTY_NATURAL_KEY_HASH_UUID,
		REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
		REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
		DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
		BEGIN_DT,
		BEGIN_DTM,
		ROW_PROCESS_DTM,
		AUDIT_ID,
		LOGICAL_DELETE_IND,
		uuid_gen(source_delete_ind, beneficiary_relationship_nm, beneficiary_allocation_pct,beneficiary_agreement_txt,beneficiary_role_position_cde,
		source_beneficiary_role_cde, source_beneficiary_role_position_cde, beneficiary_role_sequence_nr_txt, 
		source_beneficiary_role_sequence_nr_txt, beneficiary_effective_dt, beneficiary_trust_dt, beneficiary_special_destination_cde,
		beneficiary_row_control_cde, beneficiary_class_cde, beneficiary_designation_txt, beneficiary_name_format_cde,
		VOLTAGEPROTECT(beneficiary_unformatted_nm,'name'))::uuid		AS		check_sum,
		CURRENT_ROW_IND,
		END_DT,
		END_DTM,
		SOURCE_SYSTEM_ID,
		RESTRICTED_ROW_IND,
		UPDATE_AUDIT_ID,
		DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
		ADDRESS_TYPE_CDE,
		ATTENTION_LINE_TXT,
		DIM_PHONE_NATURAL_KEY_HASH_UUID,
		DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
		DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
		BENEFICIARY_SUB_CLASS_NR,
		BENEFICIARY_RELATIONSHIP_NM,
		BENEFICIARY_ALLOCATION_PCT,
		BENEFICIARY_ALLOCATION_AMT,
		BENEFICIARY_AGREEMENT_TXT,
		SOURCE_PARTY_ROLE_CDE,
		SOURCE_PARTY_SUB_ROLE_CDE,
		SOURCE_ADDRESS_TYPE_CDE,
		SOURCE_DELETE_IND,
		BENEFICIARY_ISS_PER_STIRPES_CDE,
		BUSINESS_STRT_DT,
		BUSINESS_END_DT,
		ADVISOR_FIRST_YEAR_COMMISSION,
		ADVISOR_RENEWAL_COMMISSION,
		ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
		FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
		SOLICIT_WRITING_AGENT_IND,
		SERVICE_AGENT_IND,
		CAREER_CORPORATION_IND,
		SOURCE_PACKAGE_ID,
		SOURCE_STATUS_CDE,
		SOURCE_STATUS_REASON_CDE,
		AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
		ADVISOR_ORDER_NR,
		AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
		ADVISOR_COMMISSION_PCT,
		BENEFICIARY_ROLE_POSITION_CDE,
		SOURCE_BENEFICIARY_ROLE_CDE,
		SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
		BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
		SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
		BENEFICIARY_EFFECTIVE_DT,
		BENEFICIARY_TRUST_DT,
		BENEFICIARY_SPECIAL_DESTINATION_CDE,
		BENEFICIARY_ROW_CONTROL_CDE,
		BENEFICIARY_DESTINATION_AMT,
		DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
		BENEFICIARY_CLASS_CDE,
		BENEFICIARY_DESIGNATION_TXT,
		BENEFICIARY_NAME_FORMAT_CDE,
		OPERATOR_IND,
		VOLTAGEPROTECT(beneficiary_unformatted_nm,'name')	AS 	beneficiary_unformatted_nm,
		ROW_NUMBER() OVER(PARTITION BY DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, 
									DIM_PARTY_NATURAL_KEY_HASH_UUID, 
									REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID, 
									REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID
						  ORDER BY BENEFICIARY_EFFECTIVE_DT DESC)		AS		RNK
	FROM
	(
	SELECT 
		CASE WHEN bene_row_adm_sys_name is null 
			and bene_row_ctrt_prefix is null 
			and bene_row_ctrt_no is null 
			and bene_row_ctrt_suffix is null 
			THEN UUID_GEN(NULL)::UUID
			ELSE UUID_GEN(bene_row_adm_sys_name, 'Ipa', bene_row_ctrt_prefix, bene_row_ctrt_no, bene_row_ctrt_suffix)::UUID
			END													AS 		DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
		CASE WHEN 
			bene_row_adm_sys_name is null
			and bene_row_ctrt_prefix is null
			and bene_row_ctrt_no is null 
			and bene_row_ctrt_suffix is null 
			and bene_row_cntr is null 
			THEN UUID_GEN(NULL)::UUID 
			ELSE UUID_GEN(bene_row_adm_sys_name,bene_row_ctrt_prefix,bene_row_ctrt_no,bene_row_ctrt_suffix,  bene_row_cntr)::UUID 
			END		 											AS		DIM_PARTY_NATURAL_KEY_HASH_UUID,
		UUID_GEN(bene_role_cd)::UUID	 						AS		REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
		UUID_GEN('Unk')::UUID	 								AS		REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
		UUID_GEN(NULL)::UUID	 								AS		DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
		begin_dt	 											AS		BEGIN_DT,
		begin_dtm	 											AS	BEGIN_DTM,
		row_process_dtm	 										AS		ROW_PROCESS_DTM,
		audit_id	 											AS		AUDIT_ID,
		logical_delete_ind  									AS		LOGICAL_DELETE_IND,
		current_row_ind											AS		CURRENT_ROW_IND,
		end_dt													AS		END_DT,
		end_dtm													AS		END_DTM,
		source_system_id										AS		SOURCE_SYSTEM_ID,
		restricted_row_ind										AS		RESTRICTED_ROW_IND,
		update_audit_id											AS		UPDATE_AUDIT_ID,
		uuid_gen(null)::uuid									AS		DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
		uuid_gen(null)::uuid									AS		DIM_PHONE_NATURAL_KEY_HASH_UUID,
		uuid_gen(null)::uuid									AS		DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
		uuid_gen(null)::uuid									AS		DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
		NULL													AS		ADDRESS_TYPE_CDE,
		NULL													AS		ATTENTION_LINE_TXT,
		NULL													AS		BENEFICIARY_SUB_CLASS_NR,
		beneficiary_relationship_nm								AS		BENEFICIARY_RELATIONSHIP_NM,
		beneficiary_allocation_pct								AS		BENEFICIARY_ALLOCATION_PCT,
		VOLTAGEPROTECT(beneficiary_agreement_txt,'freeform') 	AS BENEFICIARY_AGREEMENT_TXT,
		NULL													AS		BENEFICIARY_ALLOCATION_AMT,
		NULL													AS		SOURCE_PARTY_ROLE_CDE,
		NULL													AS		SOURCE_PARTY_SUB_ROLE_CDE,
		NULL													AS		SOURCE_ADDRESS_TYPE_CDE,
		source_delete_ind										AS		SOURCE_DELETE_IND,
		NULL													AS		BENEFICIARY_ISS_PER_STIRPES_CDE,
		COALESCE(beneficiary_effective_dt,CURRENT_TIMESTAMP::date)		AS		BUSINESS_STRT_DT,
		'12-31-9999'::date										AS		BUSINESS_END_DT,
		NULL													AS		ADVISOR_FIRST_YEAR_COMMISSION,
		NULL													AS		ADVISOR_RENEWAL_COMMISSION,
		uuid_gen(null)::uuid									AS		ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
		uuid_gen(null)::uuid									AS		FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
		NULL													AS		SOLICIT_WRITING_AGENT_IND,
		NULL													AS		SERVICE_AGENT_IND,
		NULL													AS		CAREER_CORPORATION_IND,
		NULL													AS		SOURCE_PACKAGE_ID,
		NULL													AS		SOURCE_STATUS_CDE,
		NULL													AS		SOURCE_STATUS_REASON_CDE,
		NULL													AS		AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
		NULL													AS		ADVISOR_ORDER_NR,
		NULL													AS		ADVISOR_COMMISSION_PCT,
		beneficiary_role_position_cde							AS		BENEFICIARY_ROLE_POSITION_CDE,
		source_beneficiary_role_cde								AS		SOURCE_BENEFICIARY_ROLE_CDE,
		source_beneficiary_role_position_cde					AS		SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
		beneficiary_role_sequence_nr							AS		BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
		source_beneficiary_role_sequence_nr						AS		SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
		beneficiary_effective_dt								AS		BENEFICIARY_EFFECTIVE_DT,
		beneficiary_trust_dt									AS		BENEFICIARY_TRUST_DT,
		beneficiary_special_destination_cde						AS		BENEFICIARY_SPECIAL_DESTINATION_CDE,
		beneficiary_row_control_cde								AS		BENEFICIARY_ROW_CONTROL_CDE,
		NULL													AS		BENEFICIARY_DESTINATION_AMT,
		uuid_gen(null)::uuid									AS		AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
		uuid_gen(null)::uuid									AS		DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
		beneficiary_class_cde									AS		BENEFICIARY_CLASS_CDE,
		beneficiary_designation_txt								AS		BENEFICIARY_DESIGNATION_TXT,
		beneficiary_name_format_cde								AS		BENEFICIARY_NAME_FORMAT_CDE,
		operator_ind											AS		OPERATOR_IND,
		case when UPPER(BENEFICIARY_NAME_FORMAT_CDE) = 'U'
		     and bene_name_first 				is null
		     and bene_name_middle 				is null
		     and bene_name_last 				is null
		     and BENE_NAME_SUFFIX 				is null
		     and BENE_NAME_PREFIX 				is null 
			 then NULL
		when UPPER(BENEFICIARY_NAME_FORMAT_CDE) = 'U'
		and (  bene_name_first 				    is not null
		    or bene_name_middle 				is not null
		    or bene_name_last 					is not null
		    or BENE_NAME_SUFFIX 				is not null
		    or BENE_NAME_PREFIX 				is not null
		    )
		then ( coalesce(BENE_NAME_PREFIX,'') || coalesce(BENE_NAME_FIRST,'') || coalesce(BENE_NAME_MIDDLE,'')
		      || coalesce(BENE_NAME_LAST,'') || coalesce(BENE_NAME_SUFFIX,'')
			 )
		ELSE NULL END 							as beneficiary_unformatted_nm
	FROM
		(   
		SELECT 
		CLEAN_STRING(bene_row_adm_sys_name)		AS BENE_ROW_ADM_SYS_NAME, 
		UDF_ISNUM_LPAD(CLEAN_STRING(bene_row_ctrt_prefix), 20, '0', TRUE)		AS BENE_ROW_CTRT_PREFIX,
		LPAD(CLEAN_STRING(bene_row_ctrt_no),20,'0')		AS BENE_ROW_CTRT_NO,
		UDF_ISNUM_LPAD(CLEAN_STRING(bene_row_ctrt_suffix), 20, '0', TRUE)		AS BENE_ROW_CTRT_SUFFIX,
		CLEAN_STRING(VOLTAGEACCESS(bene_name_first,'name'))		AS BENE_NAME_FIRST, 
		CLEAN_STRING(VOLTAGEACCESS(bene_name_middle,'name'))	AS BENE_NAME_MIDDLE,
		CLEAN_STRING(VOLTAGEACCESS(bene_name_last,'name'))		AS BENE_NAME_LAST,
		CLEAN_STRING(VOLTAGEACCESS(bene_name_prefix,'name'))	AS BENE_NAME_PREFIX,
		CLEAN_STRING(VOLTAGEACCESS(bene_name_suffix,'name'))	AS BENE_NAME_SUFFIX, 
		CLEAN_STRING(VOLTAGEACCESS(bene_arrngmt,'freeform'))	AS BENE_ARRNGMT,
		bene_row_cntr											AS BENE_ROW_CNTR,
		COALESCE(CLEAN_STRING(bene_role_cd),'Bene')				AS BENE_ROLE_CD,
		CLEAN_STRING(bene_class_cd)								AS BENE_CLASS_CD,
		CURRENT_TIMESTAMP::date									AS BEGIN_DT, 
		CURRENT_TIMESTAMP::timestamp(6)							AS BEGIN_DTM, 
		CURRENT_TIMESTAMP::timestamp(6)							AS ROW_PROCESS_DTM,
		:audit_id												AS AUDIT_ID,
		FALSE													AS LOGICAL_DELETE_IND,
		TRUE													AS CURRENT_ROW_IND, 
		'12-31-9999'::date										AS END_DT,
		'12-31-9999'::timestamp(6)								AS END_DTM,
		'50'													AS SOURCE_SYSTEM_ID, 
		FALSE													AS RESTRICTED_ROW_IND,
		:audit_id												AS UPDATE_AUDIT_ID,
		CLEAN_STRING(bene_relationship)							AS BENEFICIARY_RELATIONSHIP_NM,
		bene_pct::numeric(9,6)									AS BENEFICIARY_ALLOCATION_PCT,
		CLEAN_STRING(VOLTAGEACCESS(bene_arrngmt,'freeform')) 	AS BENEFICIARY_AGREEMENT_TXT,
		bene_role_position_cd::varchar							AS BENEFICIARY_ROLE_POSITION_CDE, 
		COALESCE(CLEAN_STRING(bene_role_cd),'Bene')				AS	SOURCE_BENEFICIARY_ROLE_CDE,
		bene_role_position_cd::varchar							AS SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
		bene_role_seq_no										AS BENEFICIARY_ROLE_SEQUENCE_NR,
		bene_role_seq_no										AS SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR,
		(bene_eff_dt::varchar)::date							AS BENEFICIARY_EFFECTIVE_DT,
		(bene_trst_dt::varchar)::date							AS BENEFICIARY_TRUST_DT,
		CLEAN_STRING(bene_spec_dsgntn)							AS BENEFICIARY_SPECIAL_DESTINATION_CDE, 
		bene_row_cntr::varchar 									AS BENEFICIARY_ROW_CONTROL_CDE, 
		COALESCE(CLEAN_STRING(bene_class_cd),'Unk')				AS BENEFICIARY_CLASS_CDE, 
		COALESCE(CLEAN_STRING(bene_prof_desig),'Unk')			AS BENEFICIARY_DESIGNATION_TXT,
		CLEAN_STRING(bene_name_format_cd)						AS BENEFICIARY_NAME_FORMAT_CDE,
		CASE WHEN PROCESS_IND = 'D' THEN TRUE ELSE FALSE END	AS SOURCE_DELETE_IND,
		process_ind												AS OPERATOR_IND
		FROM edw_staging.cda_lifcom_life_edw_bene_delta src
		) SRC_DATASET
	) FINAL_DATASET
)DEDUP
INNER JOIN TEMP_LIFCOM_CTRT_DIM_AGREEMENT_TBL CTRT ON CTRT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID=DEDUP.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID   
WHERE RNK=1 AND DEDUP.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID NOT IN   --- IGNORE AGREEMENTS WHICH ARE ALREADY PROCESSED BY ECDM BENE SOURCE
             ( 
               SELECT DIM_AGREEMENT_NATURAL_KEY_HASH_UUID FROM VT_DIM_AGREEMENTS
              ) ;


COMMIT ;


/* WORK TABLE - INSERTS 
 * 
 * this script is used to load the records that don't have a record in target
 * */

INSERT INTO EDW_WORK.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT
(
	DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
	DIM_PARTY_NATURAL_KEY_HASH_UUID,
	REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
	REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
	DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
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
	DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
	ADDRESS_TYPE_CDE,
	ATTENTION_LINE_TXT,
	DIM_PHONE_NATURAL_KEY_HASH_UUID,
	DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
	DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
	BENEFICIARY_SUB_CLASS_NR,
	BENEFICIARY_RELATIONSHIP_NM,
	BENEFICIARY_ALLOCATION_PCT,
	BENEFICIARY_ALLOCATION_AMT,
	BENEFICIARY_AGREEMENT_TXT,
	SOURCE_PARTY_ROLE_CDE,
	SOURCE_PARTY_SUB_ROLE_CDE,
	SOURCE_ADDRESS_TYPE_CDE,
	SOURCE_DELETE_IND,
	BENEFICIARY_ISS_PER_STIRPES_CDE,
	BUSINESS_STRT_DT,
	BUSINESS_END_DT,
	ADVISOR_FIRST_YEAR_COMMISSION,
	ADVISOR_RENEWAL_COMMISSION,
	ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	SOLICIT_WRITING_AGENT_IND,
	SERVICE_AGENT_IND,
	CAREER_CORPORATION_IND,
	SOURCE_PACKAGE_ID,
	SOURCE_STATUS_CDE,
	SOURCE_STATUS_REASON_CDE,
	AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
	ADVISOR_ORDER_NR,
	AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	ADVISOR_COMMISSION_PCT,
	BENEFICIARY_ROLE_POSITION_CDE,
	SOURCE_BENEFICIARY_ROLE_CDE,
	SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
	BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	BENEFICIARY_EFFECTIVE_DT,
	BENEFICIARY_TRUST_DT,
	BENEFICIARY_SPECIAL_DESTINATION_CDE,
	BENEFICIARY_ROW_CONTROL_CDE,
	BENEFICIARY_DESTINATION_AMT,
	DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	BENEFICIARY_CLASS_CDE,
	BENEFICIARY_DESIGNATION_TXT,
	BENEFICIARY_NAME_FORMAT_CDE,
	beneficiary_unformatted_nm
)
SELECT 
	SRC.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
	SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID,
	SRC.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
	SRC.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
	SRC.DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
	'01-01-0001'::DATE		AS BEGIN_DT,
	'01-01-0001'::TIMESTAMP(6)		AS BEGIN_DTM,
	CURRENT_TIMESTAMP	AS ROW_PROCESS_DTM,
	SRC.AUDIT_ID,
	SRC.LOGICAL_DELETE_IND,
	SRC.CHECK_SUM,
	SRC.CURRENT_ROW_IND,
	SRC.END_DT,
	SRC.END_DTM,
	SRC.SOURCE_SYSTEM_ID,
	SRC.RESTRICTED_ROW_IND,
	SRC.UPDATE_AUDIT_ID,
	SRC.DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
	SRC.ADDRESS_TYPE_CDE,
	SRC.ATTENTION_LINE_TXT,
	SRC.DIM_PHONE_NATURAL_KEY_HASH_UUID,
	SRC.DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
	SRC.DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
	SRC.BENEFICIARY_SUB_CLASS_NR,
	SRC.BENEFICIARY_RELATIONSHIP_NM,
	SRC.BENEFICIARY_ALLOCATION_PCT,
	SRC.BENEFICIARY_ALLOCATION_AMT,
	SRC.BENEFICIARY_AGREEMENT_TXT,
	SRC.SOURCE_PARTY_ROLE_CDE,
	SRC.SOURCE_PARTY_SUB_ROLE_CDE,
	SRC.SOURCE_ADDRESS_TYPE_CDE,
	SRC.SOURCE_DELETE_IND,
	SRC.BENEFICIARY_ISS_PER_STIRPES_CDE,
	CASE WHEN SRC.BENEFICIARY_EFFECTIVE_DT <> '01-01-0001'::DATE
		 THEN SRC.BENEFICIARY_EFFECTIVE_DT 
		 ELSE '01-01-0001'::DATE
		 END AS BUSINESS_STRT_DT,
	SRC.BUSINESS_END_DT,
	SRC.ADVISOR_FIRST_YEAR_COMMISSION,
	SRC.ADVISOR_RENEWAL_COMMISSION,
	SRC.ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	SRC.FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	SRC.SOLICIT_WRITING_AGENT_IND,
	SRC.SERVICE_AGENT_IND,
	SRC.CAREER_CORPORATION_IND,
	SRC.SOURCE_PACKAGE_ID,
	SRC.SOURCE_STATUS_CDE,
	SRC.SOURCE_STATUS_REASON_CDE,
	SRC.AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
	SRC.ADVISOR_ORDER_NR,
	SRC.AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	SRC.ADVISOR_COMMISSION_PCT,
	SRC.BENEFICIARY_ROLE_POSITION_CDE,
	SRC.SOURCE_BENEFICIARY_ROLE_CDE,
	SRC.SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
	SRC.BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	SRC.SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	SRC.BENEFICIARY_EFFECTIVE_DT,
	SRC.BENEFICIARY_TRUST_DT,
	SRC.BENEFICIARY_SPECIAL_DESTINATION_CDE,
	SRC.BENEFICIARY_ROW_CONTROL_CDE,
	SRC.BENEFICIARY_DESTINATION_AMT,
	SRC.DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	SRC.BENEFICIARY_CLASS_CDE,
	SRC.BENEFICIARY_DESIGNATION_TXT,
	SRC.BENEFICIARY_NAME_FORMAT_CDE,
	SRC.beneficiary_unformatted_nm
FROM EDW_STAGING.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT_PRE_WORK SRC
LEFT JOIN edw.rel_party_agreement TGT
ON SRC.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID= TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
AND SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID= TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND SRC.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID= TGT.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID
AND SRC.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID= TGT.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID
WHERE TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID IS NULL ;


COMMIT ;

  /* WORK TABLE - UPDATE TGT RECORD
 * 
 * This script finds records where the new record from the source has a different check_sum than the current target record or the record is being ended/deleted. 
 * The current record in the target will be ended since the source record will be inserted in the next step.
 * */

INSERT INTO EDW_WORK.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT
(
	DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
	DIM_PARTY_NATURAL_KEY_HASH_UUID,
	REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
	REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
	DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
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
	DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
	ADDRESS_TYPE_CDE,
	ATTENTION_LINE_TXT,
	DIM_PHONE_NATURAL_KEY_HASH_UUID,
	DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
	DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
	BENEFICIARY_SUB_CLASS_NR,
	BENEFICIARY_RELATIONSHIP_NM,
	BENEFICIARY_ALLOCATION_PCT,
	BENEFICIARY_ALLOCATION_AMT,
	BENEFICIARY_AGREEMENT_TXT,
	SOURCE_PARTY_ROLE_CDE,
	SOURCE_PARTY_SUB_ROLE_CDE,
	SOURCE_ADDRESS_TYPE_CDE,
	SOURCE_DELETE_IND,
	BENEFICIARY_ISS_PER_STIRPES_CDE,
	BUSINESS_STRT_DT,
	BUSINESS_END_DT,
	ADVISOR_FIRST_YEAR_COMMISSION,
	ADVISOR_RENEWAL_COMMISSION,
	ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	SOLICIT_WRITING_AGENT_IND,
	SERVICE_AGENT_IND,
	CAREER_CORPORATION_IND,
	SOURCE_PACKAGE_ID,
	SOURCE_STATUS_CDE,
	SOURCE_STATUS_REASON_CDE,
	AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
	ADVISOR_ORDER_NR,
	AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	ADVISOR_COMMISSION_PCT,
	BENEFICIARY_ROLE_POSITION_CDE,
	SOURCE_BENEFICIARY_ROLE_CDE,
	SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
	BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	BENEFICIARY_EFFECTIVE_DT,
	BENEFICIARY_TRUST_DT,
	BENEFICIARY_SPECIAL_DESTINATION_CDE,
	BENEFICIARY_ROW_CONTROL_CDE,
	BENEFICIARY_DESTINATION_AMT,
	DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	BENEFICIARY_CLASS_CDE,
	BENEFICIARY_DESIGNATION_TXT,
	BENEFICIARY_NAME_FORMAT_CDE,
	beneficiary_unformatted_nm
)
SELECT 
	TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
	TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID,
	TGT.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
	TGT.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
	TGT.DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
	TGT.BEGIN_DT,
	TGT.BEGIN_DTM,
	CURRENT_TIMESTAMP::TIMESTAMP(6)	AS ROW_PROCESS_DTM,
	TGT.AUDIT_ID,
	TGT.LOGICAL_DELETE_IND,
	TGT.CHECK_SUM,
	FALSE AS CURRENT_ROW_IND,
	SRC.BEGIN_DT - INTERVAL '1' DAY AS END_DT,
	SRC.BEGIN_DTM - INTERVAL '1' SECOND AS END_DTM,
	TGT.SOURCE_SYSTEM_ID,
	TGT.RESTRICTED_ROW_IND,
	SRC.UPDATE_AUDIT_ID,
	TGT.DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
	TGT.ADDRESS_TYPE_CDE,
	TGT.ATTENTION_LINE_TXT,
	TGT.DIM_PHONE_NATURAL_KEY_HASH_UUID,
	TGT.DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
	TGT.DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
	TGT.BENEFICIARY_SUB_CLASS_NR,
	TGT.BENEFICIARY_RELATIONSHIP_NM,
	TGT.BENEFICIARY_ALLOCATION_PCT,
	TGT.BENEFICIARY_ALLOCATION_AMT,
	TGT.BENEFICIARY_AGREEMENT_TXT,
	TGT.SOURCE_PARTY_ROLE_CDE,
	TGT.SOURCE_PARTY_SUB_ROLE_CDE,
	TGT.SOURCE_ADDRESS_TYPE_CDE,
	TGT.SOURCE_DELETE_IND,
	TGT.BENEFICIARY_ISS_PER_STIRPES_CDE,
	TGT.BUSINESS_STRT_DT,	
	CASE WHEN SRC.BUSINESS_STRT_DT <> '01-01-0001'::DATE THEN SRC.BUSINESS_STRT_DT - INTERVAL '1' DAY
	     ELSE CURRENT_DATE - 1 END AS BUSINESS_END_DT,
	TGT.ADVISOR_FIRST_YEAR_COMMISSION,
	TGT.ADVISOR_RENEWAL_COMMISSION,
	TGT.ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	TGT.FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	TGT.SOLICIT_WRITING_AGENT_IND,
	TGT.SERVICE_AGENT_IND,
	TGT.CAREER_CORPORATION_IND,
	TGT.SOURCE_PACKAGE_ID,
	TGT.SOURCE_STATUS_CDE,
	TGT.SOURCE_STATUS_REASON_CDE,
	TGT.AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
	TGT.ADVISOR_ORDER_NR,
	TGT.AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	TGT.ADVISOR_COMMISSION_PCT,
	TGT.BENEFICIARY_ROLE_POSITION_CDE,
	TGT.SOURCE_BENEFICIARY_ROLE_CDE,
	TGT.SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
	TGT.BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	TGT.SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	TGT.BENEFICIARY_EFFECTIVE_DT,
	TGT.BENEFICIARY_TRUST_DT,
	TGT.BENEFICIARY_SPECIAL_DESTINATION_CDE,
	TGT.BENEFICIARY_ROW_CONTROL_CDE,
	TGT.BENEFICIARY_DESTINATION_AMT,
	TGT.DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	TGT.BENEFICIARY_CLASS_CDE,
	TGT.BENEFICIARY_DESIGNATION_TXT,
	TGT.BENEFICIARY_NAME_FORMAT_CDE,
	TGT.beneficiary_unformatted_nm
FROM EDW_STAGING.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT_PRE_WORK SRC
JOIN edw.rel_party_agreement TGT
ON SRC.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID = TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
AND SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID = TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND SRC.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID = TGT.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID
AND SRC.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID = TGT.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID
AND TGT.CURRENT_ROW_IND
WHERE (SRC.CHECK_SUM <> TGT.CHECK_SUM);


COMMIT ;


/* WORK TABLE - UPDATE WHERE RECORD ALREADY EXISTS IN TARGET 
 *  
 * */

INSERT INTO EDW_WORK.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT
(
	DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
	DIM_PARTY_NATURAL_KEY_HASH_UUID,
	REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
	REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
	DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
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
	DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
	ADDRESS_TYPE_CDE,
	ATTENTION_LINE_TXT,
	DIM_PHONE_NATURAL_KEY_HASH_UUID,
	DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
	DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
	BENEFICIARY_SUB_CLASS_NR,
	BENEFICIARY_RELATIONSHIP_NM,
	BENEFICIARY_ALLOCATION_PCT,
	BENEFICIARY_ALLOCATION_AMT,
	BENEFICIARY_AGREEMENT_TXT,
	SOURCE_PARTY_ROLE_CDE,
	SOURCE_PARTY_SUB_ROLE_CDE,
	SOURCE_ADDRESS_TYPE_CDE,
	SOURCE_DELETE_IND,
	BENEFICIARY_ISS_PER_STIRPES_CDE,
	BUSINESS_STRT_DT,
	BUSINESS_END_DT,
	ADVISOR_FIRST_YEAR_COMMISSION,
	ADVISOR_RENEWAL_COMMISSION,
	ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	SOLICIT_WRITING_AGENT_IND,
	SERVICE_AGENT_IND,
	CAREER_CORPORATION_IND,
	SOURCE_PACKAGE_ID,
	SOURCE_STATUS_CDE,
	SOURCE_STATUS_REASON_CDE,
	AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
	ADVISOR_ORDER_NR,
	AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	ADVISOR_COMMISSION_PCT,
	BENEFICIARY_ROLE_POSITION_CDE,
	SOURCE_BENEFICIARY_ROLE_CDE,
	SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
	BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	BENEFICIARY_EFFECTIVE_DT,
	BENEFICIARY_TRUST_DT,
	BENEFICIARY_SPECIAL_DESTINATION_CDE,
	BENEFICIARY_ROW_CONTROL_CDE,
	BENEFICIARY_DESTINATION_AMT,
	DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	BENEFICIARY_CLASS_CDE,
	BENEFICIARY_DESIGNATION_TXT,
	BENEFICIARY_NAME_FORMAT_CDE,
	beneficiary_unformatted_nm
)
SELECT 
	SRC.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
	SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID,
	SRC.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
	SRC.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
	SRC.DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
	SRC.BEGIN_DT,
	SRC.BEGIN_DTM,
	CURRENT_TIMESTAMP AS ROW_PROCESS_DTM,
	SRC.AUDIT_ID,
	SRC.LOGICAL_DELETE_IND,
	SRC.CHECK_SUM,
	SRC.CURRENT_ROW_IND,
	SRC.END_DT,
	SRC.END_DTM,
	SRC.SOURCE_SYSTEM_ID,
	SRC.RESTRICTED_ROW_IND,
	SRC.UPDATE_AUDIT_ID,
	SRC.DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
	SRC.ADDRESS_TYPE_CDE,
	SRC.ATTENTION_LINE_TXT,
	SRC.DIM_PHONE_NATURAL_KEY_HASH_UUID,
	SRC.DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
	SRC.DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
	SRC.BENEFICIARY_SUB_CLASS_NR,
	SRC.BENEFICIARY_RELATIONSHIP_NM,
	SRC.BENEFICIARY_ALLOCATION_PCT,
	SRC.BENEFICIARY_ALLOCATION_AMT,
	SRC.BENEFICIARY_AGREEMENT_TXT,
	SRC.SOURCE_PARTY_ROLE_CDE,
	SRC.SOURCE_PARTY_SUB_ROLE_CDE,
	SRC.SOURCE_ADDRESS_TYPE_CDE,
	SRC.SOURCE_DELETE_IND,
	SRC.BENEFICIARY_ISS_PER_STIRPES_CDE,
	SRC.BUSINESS_STRT_DT,
	SRC.BUSINESS_END_DT,
	SRC.ADVISOR_FIRST_YEAR_COMMISSION,
	SRC.ADVISOR_RENEWAL_COMMISSION,
	SRC.ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	SRC.FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	SRC.SOLICIT_WRITING_AGENT_IND,
	SRC.SERVICE_AGENT_IND,
	SRC.CAREER_CORPORATION_IND,
	SRC.SOURCE_PACKAGE_ID,
	SRC.SOURCE_STATUS_CDE,
	SRC.SOURCE_STATUS_REASON_CDE,
	SRC.AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
	SRC.ADVISOR_ORDER_NR,
	SRC.AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	SRC.ADVISOR_COMMISSION_PCT,
	SRC.BENEFICIARY_ROLE_POSITION_CDE,
	SRC.SOURCE_BENEFICIARY_ROLE_CDE,
	SRC.SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
	SRC.BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	SRC.SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	SRC.BENEFICIARY_EFFECTIVE_DT,
	SRC.BENEFICIARY_TRUST_DT,
	SRC.BENEFICIARY_SPECIAL_DESTINATION_CDE,
	SRC.BENEFICIARY_ROW_CONTROL_CDE,
	SRC.BENEFICIARY_DESTINATION_AMT,
	SRC.DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	SRC.BENEFICIARY_CLASS_CDE,
	SRC.BENEFICIARY_DESIGNATION_TXT,
	SRC.BENEFICIARY_NAME_FORMAT_CDE,
	SRC.beneficiary_unformatted_nm
FROM EDW_STAGING.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT_PRE_WORK SRC
LEFT JOIN edw.rel_party_agreement TGT
ON SRC.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID = TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
AND SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID = TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND SRC.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID = TGT.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID
AND SRC.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID = TGT.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID
AND TGT.CURRENT_ROW_IND 
 WHERE 
 (     TGT.ROW_SID IS NULL AND
 
	       (SRC.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
			SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID,
			COALESCE(SRC.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,uuid_gen(null)::uuid),
			COALESCE(SRC.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,uuid_gen(null)::uuid),
			COALESCE(SRC.BENEFICIARY_SUB_CLASS_NR,-999)
		   ) IN 
		   (SELECT DISTINCT 
			TGT1.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
			TGT1.DIM_PARTY_NATURAL_KEY_HASH_UUID,
			COALESCE(TGT1.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,uuid_gen(null)::uuid),
			COALESCE(TGT1.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,uuid_gen(null)::uuid),
			COALESCE(TGT1.BENEFICIARY_SUB_CLASS_NR,-999)
			FROM
			edw.rel_party_agreement TGT1
			)
		) 
		OR 
		( TGT.ROW_SID IS NOT NULL AND TGT.CHECK_SUM <> SRC.CHECK_SUM);


COMMIT;

SELECT ANALYZE_STATISTICS('EDW_WORK.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT');
SELECT ANALYZE_STATISTICS('edw.rel_party_agreement');

	
