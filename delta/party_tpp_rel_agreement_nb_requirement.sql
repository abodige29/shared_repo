/*
    FileName: party_tpp_rel_agreement_nb_requirement.sql
    Author: MM01884
    Subject Area: Party
    Source: TPP
    Create Date:2021-12-28
       
    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    TERSUN-3698            Party-Tier2     01/10/2022            Initial version     
    ---------------------------------------------------------------------------------------------------------------
*/


/* UPDATING STATISTICS */
SELECT ANALYZE_STATISTICS('edw.rel_agreement_nb_requirement');
SELECT ANALYZE_STATISTICS('edw_staging.tpp_edw_tpp_requirements');
SELECT ANALYZE_STATISTICS('edw_staging.tpp_edw_tpp_requirement_history');
SELECT ANALYZE_STATISTICS('edw_staging.tpp_edw_tpp_requirement_history_snapshot');
SELECT ANALYZE_STATISTICS('edw_staging.tpp_edw_tpp_application_snapshot');

/* TRUNCATE PRE WORK AND WORK TABLES */
Truncate table edw_staging.party_tpp_rel_agreement_nb_requirement_pre_work;
Truncate table edw_work.party_tpp_rel_agreement_nb_requirement;

commit;

/*tpp rel_agreement_nb_requirement pre process script*/

update edw.rel_agreement_nb_requirement tgt
set dim_agreement_natural_key_hash_uuid=sub.dim_agreement_natural_key_hash_uuid
from(
select sub.*,rar.row_sid
from
(select distinct da.dim_agreement_natural_key_hash_uuid,src.policy_number,src.case_reqt_id,
UUID_GEN(clean_string(null),CLEAN_STRING('Appl'),null,lpad(Btrim(src.policy_number), 20, '0'),null,src.case_id)::uuid null_uuid
		from
			edw_staging.tpp_edw_tpp_requirements_snapshot src
		left outer join edw.dim_agreement da 
			on	clean_string(lpad(src.policy_number,20,'0')) = da.agreement_nr
			and src.case_id = da.application_case_id
			and da.current_row_ind
			and da.logical_delete_ind = FALSE
			and Clean_string(da.agreement_type_cde) = 'Appl'
			and da.source_system_id in ('32', '347')
			where da.dim_agreement_natural_key_hash_uuid is not null
			order by src.policy_number,src.case_reqt_id
			) sub
			inner join
			edw.rel_agreement_nb_requirement rar
			on sub.null_uuid=rar.dim_agreement_natural_key_hash_uuid
			and sub.case_reqt_id=rar.requirement_case_id			
		) sub 
	where tgt.dim_agreement_natural_key_hash_uuid = sub.null_uuid and tgt.requirement_case_id=sub.case_reqt_id and tgt.row_sid=sub.row_sid;

commit;

drop table if exists temp_carr_admin_sys_cd;
/*carr_admin_sys_cd*/
create local temporary table temp_carr_admin_sys_cd on commit preserve rows as
/* +DIRECT */
select
	distinct src.policy_number,src.case_id,src.case_reqt_id,pt.admn_sys_cde as carr_admin_sys_cd
from
	edw_staging.tpp_edw_tpp_requirements src
left join edw_staging.tpp_edw_tpp_application_snapshot appl on
	src.policy_number = appl.policy_number
left join (
	select
		distinct admn_sys_cde,
		prod_typ_cde
	from
		edw_ref.product_translator
) pt on clean_string(appl.product_code) = clean_string(pt.prod_typ_cde)
order by src.case_id,src.case_reqt_id,src.policy_number;

drop table if exists temp_tpp_edw_tpp_requirement_history_snapshot;
/*temp_tpp_edw_tpp_requirement_history_snapshot*/
create local temporary table temp_tpp_edw_tpp_requirement_history_snapshot on commit preserve rows as
/* +DIRECT */
select * from
(
select
	distinct src.policy_number,src.case_id,src.case_reqt_id,hist.requirement_status_code, hist.requirement_status_date,
	ROW_NUMBER() over(partition by src.policy_number,src.case_id,src.case_reqt_id order by sort_num desc) rnk
from
	edw_staging.tpp_edw_tpp_requirements src
inner join edw_staging.tpp_edw_tpp_requirement_history_snapshot hist
		on src.policy_number=hist.policy_number
		and src.case_id=hist.case_id 
		and src.case_reqt_id=hist.case_reqt_id
		) sub where rnk=1
order by case_id,case_reqt_id,policy_number;

/*pre work insert for not deletes */
INSERT /*+DIRECT*/ 
	into edw_staging.party_tpp_rel_agreement_nb_requirement_pre_work (
	dim_agreement_natural_key_hash_uuid ,
	ref_requirement_type_natural_key_hash_uuid ,
	requirement_case_id ,
	collection_methord_cde,
	requirement_category_cde ,
	requirement_comment_txt ,
	requirement_status_cde ,
	workbench_collection_id,
	workbench_collection_methord_cde,
	physician_full_nm ,
	requirement_status_dt ,
	source_requirement_status_cde ,
	source_requirement_cde ,
	source_requirement_category ,
	begin_dt ,
	begin_dtm ,
	row_process_dtm ,
	audit_id ,
	logical_delete_ind ,
	check_sum ,
	current_row_ind ,
	end_dt ,
	end_dtm ,
	source_system_id ,
	restricted_row_ind ,
	update_audit_id ,
	source_delete_ind ,
	operator_ind
	)
select
	dim_agreement_natural_key_hash_uuid ,
	ref_requirement_type_natural_key_hash_uuid ,
	requirement_case_id ,
	collection_methord_cde,
	requirement_category_cde ,
	requirement_comment_txt ,
	requirement_status_cde ,
	workbench_collection_id,
	workbench_collection_methord_cde,
	physician_full_nm ,
	requirement_status_dt ,
	source_requirement_status_cde ,
	source_requirement_cde ,
	source_requirement_category ,
	begin_dt ,
	begin_dtm ,
	row_process_dtm ,
	audit_id ,
	logical_delete_ind ,
	check_sum ,
	current_row_ind ,
	end_dt ,
	end_dtm ,
	source_system_id ,
	restricted_row_ind ,
	update_audit_id ,
	source_delete_ind ,
	operator_ind
from
	(
	select
		dim_agreement_natural_key_hash_uuid ,
		ref_requirement_type_natural_key_hash_uuid ,
		requirement_case_id ,
		collection_methord_cde,
		requirement_category_cde ,
		requirement_comment_txt ,
		requirement_status_cde ,
		workbench_collection_id,
		workbench_collection_methord_cde,
		physician_full_nm ,
		requirement_status_dt ,
		source_requirement_status_cde ,
		source_requirement_cde ,
		source_requirement_category ,
		begin_dt ,
		begin_dtm ,
		row_process_dtm ,
		audit_id ,
		logical_delete_ind ,
		UUID_GEN(source_delete_ind ,
		collection_methord_cde,
		requirement_category_cde ,
		requirement_comment_txt ,
		requirement_status_cde ,
		workbench_collection_id,
		workbench_collection_methord_cde,
		physician_full_nm ,
		requirement_status_dt ,
		source_requirement_status_cde ,
		source_requirement_cde ,
		source_requirement_category)::uuid as check_sum ,
		current_row_ind ,
		end_dt ,
		end_dtm ,
		source_system_id ,
		restricted_row_ind ,
		update_audit_id ,
		source_delete_ind ,
		operator_ind,
		ROW_NUMBER() OVER(PARTITION BY DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
		REF_REQUIREMENT_TYPE_NATURAL_KEY_HASH_UUID,
		REQUIREMENT_CASE_ID
	order by
		begin_dtm desc) rnk
	from
		(
		select
			coalesce(da.dim_agreement_natural_key_hash_uuid,UUID_GEN(clean_string(src1.CARR_ADMIN_SYS_CD),CLEAN_STRING('Appl'),null,lpad(src.policy_number, 20, '0'),null,src.case_id)::uuid) as dim_agreement_natural_key_hash_uuid,
			UUID_GEN(COALESCE(clean_string(sdt.TRNSLT_FLD_VAL),'Unk'))::uuid as ref_requirement_type_natural_key_hash_uuid,
			src.case_reqt_id as requirement_case_id,
			src.requirement_collection_method as collection_methord_cde,
			COALESCE(clean_string(sdt_1.TRNSLT_FLD_VAL),'Unk') as requirement_category_cde,
			src.requirement_comments as requirement_comment_txt,
			COALESCE(clean_string(sdt_2.TRNSLT_FLD_VAL),'Unk') as requirement_status_cde,
			Clean_string(hist.requirement_status_code) as source_requirement_status_cde,
			src.workbench_collection_id as workbench_collection_id,
			COALESCE(clean_string(sdt_3.TRNSLT_FLD_VAL),'Unk') as workbench_collection_methord_cde,
			src.physician_name as physician_full_nm,
			Clean_string(src.requirement_id) as source_requirement_cde,
			hist.requirement_status_date::date as requirement_status_dt,
			src.requirement_category as source_requirement_category,
			src.lst_updt_dt::date as begin_dt,
			src.lst_updt_dt::timestamp(6) as begin_dtm,
			current_timestamp(6) as ROW_PROCESS_DTM,
			:audit_id as AUDIT_ID,
			False::boolean as LOGICAL_DELETE_IND,
			True::boolean as CURRENT_ROW_IND,
			'12-31-9999'::date as end_dt ,
			'12-31-9999'::timestamp(6) as end_dtm ,
			'32' as source_system_id ,
			False::boolean as restricted_row_ind ,
			:audit_id as update_audit_id ,
			False::boolean as source_delete_ind,
			'I' as operator_ind
		from
			edw_staging.tpp_edw_tpp_requirements src
		left outer join edw.dim_agreement da 
			on	clean_string(lpad(src.policy_number,20,'0')) = da.agreement_nr
			and src.case_id = da.application_case_id
			and da.current_row_ind
			and da.logical_delete_ind = FALSE
			and Clean_string(da.agreement_type_cde) = 'Appl'
			and da.source_system_id in ('32', '347')
		LEFT JOIN (
			SELECT
				DISTINCT SRC_FLD_VAL AS SRC_FLD_VAL,
				CLEAN_STRING(TRNSLT_FLD_VAL) AS TRNSLT_FLD_VAL
			FROM
				EDW_REF.SRC_DATA_TRNSLT
			WHERE
				UPPER(SRC_CDE) = 'TPP'
					AND UPPER(SRC_FLD_NM) = 'REQT_ID'
						AND UPPER(TRNSLT_FLD_NM) = 'REQUIREMENT CODE' ) SDT ON
			CLEAN_STRING(src.requirement_id) = CLEAN_STRING(SDT.SRC_FLD_VAL)
		LEFT JOIN (
			SELECT
				DISTINCT SRC_FLD_VAL AS SRC_FLD_VAL,
				CLEAN_STRING(TRNSLT_FLD_VAL) AS TRNSLT_FLD_VAL
			FROM
				EDW_REF.SRC_DATA_TRNSLT
			WHERE
				UPPER(SRC_CDE) = 'TPP'
					AND UPPER(SRC_FLD_NM) = 'REQT_CAT_CD'
						AND UPPER(TRNSLT_FLD_NM) = 'REQUIREMENT CATEGORY' ) SDT_1 ON
			CLEAN_STRING(src.requirement_category) = CLEAN_STRING(SDT_1.SRC_FLD_VAL)
			
		LEFT JOIN temp_tpp_edw_tpp_requirement_history_snapshot hist
		on src.policy_number=hist.policy_number
		and src.case_id=hist.case_id 
		and src.case_reqt_id=hist.case_reqt_id

		left outer join temp_carr_admin_sys_cd src1
		on src.policy_number=src1.policy_number
		and src.case_id=src1.case_id 
		and src.case_reqt_id=src1.case_reqt_id
		
		LEFT JOIN (
			SELECT
				DISTINCT SRC_FLD_VAL AS SRC_FLD_VAL,
				CLEAN_STRING(TRNSLT_FLD_VAL) AS TRNSLT_FLD_VAL
			FROM
				EDW_REF.SRC_DATA_TRNSLT
			WHERE
				UPPER(SRC_CDE) = 'TPP'
					AND UPPER(SRC_FLD_NM) = 'REQT_STAT_CD'
						AND UPPER(TRNSLT_FLD_NM) = 'REQUIREMENT STATUS' ) SDT_2 ON
			CLEAN_STRING(hist.requirement_status_code) = CLEAN_STRING(SDT_2.SRC_FLD_VAL)
			
		LEFT JOIN (
			SELECT
				DISTINCT SRC_FLD_VAL AS SRC_FLD_VAL,
				CLEAN_STRING(TRNSLT_FLD_VAL) AS TRNSLT_FLD_VAL
			FROM
				EDW_REF.SRC_DATA_TRNSLT
			WHERE
				UPPER(SRC_CDE) = 'TPP'
					AND UPPER(SRC_FLD_NM) = 'WB_COLL_CD'
						AND UPPER(TRNSLT_FLD_NM) = 'WB COLLECTION METHOD DESCRIPTION' ) SDT_3 ON
			CLEAN_STRING(src.workbench_collection_id) = CLEAN_STRING(SDT_3.SRC_FLD_VAL)
		/*where
			Btrim(change_mode) <> 'DELETE' */) t1 ) t2
where
	rnk = 1;
	

commit;

SELECT ANALYZE_STATISTICS('edw_staging.party_tpp_rel_agreement_nb_requirement_pre_work');
	

/* WORK TABLE - INSERTS 
 * this script is used to load the records that don't have a record in target */
INSERT /*+DIRECT*/ 
	into edw_work.party_tpp_rel_agreement_nb_requirement (
	dim_agreement_natural_key_hash_uuid ,
	ref_requirement_type_natural_key_hash_uuid ,
	requirement_case_id ,
	collection_methord_cde,
	requirement_category_cde ,
	requirement_comment_txt ,
	requirement_status_cde ,
	workbench_collection_id,
	workbench_collection_methord_cde,
	physician_full_nm ,
	requirement_status_dt ,
	source_requirement_status_cde ,
	source_requirement_cde ,
	source_requirement_category ,
	begin_dt ,
	begin_dtm ,
	row_process_dtm ,
	audit_id ,
	logical_delete_ind ,
	check_sum ,
	current_row_ind ,
	end_dt ,
	end_dtm ,
	source_system_id ,
	restricted_row_ind ,
	update_audit_id ,
	source_delete_ind
	)
select
	src.dim_agreement_natural_key_hash_uuid ,
	src.ref_requirement_type_natural_key_hash_uuid ,
	src.requirement_case_id ,
	src.collection_methord_cde,
	src.requirement_category_cde ,
	src.requirement_comment_txt ,
	src.requirement_status_cde ,
	src.workbench_collection_id,
	src.workbench_collection_methord_cde,
	src.physician_full_nm ,
	src.requirement_status_dt ,
	src.source_requirement_status_cde ,
	src.source_requirement_cde ,
	src.source_requirement_category ,
	src.begin_dt ,
	src.begin_dtm ,
	src.row_process_dtm ,
	src.audit_id ,
	src.logical_delete_ind ,
	src.check_sum ,
	src.current_row_ind ,
	src.end_dt ,
	src.end_dtm ,
	src.source_system_id ,
	src.restricted_row_ind ,
	src.update_audit_id ,
	src.source_delete_ind
	from
		edw_staging.party_tpp_rel_agreement_nb_requirement_pre_work src
	left outer join
		edw.rel_agreement_nb_requirement tgt
		on src.dim_agreement_natural_key_hash_uuid=tgt.dim_agreement_natural_key_hash_uuid
		and src.ref_requirement_type_natural_key_hash_uuid=tgt.ref_requirement_type_natural_key_hash_uuid
		and src.requirement_case_id=tgt.requirement_case_id
	where
		tgt.dim_agreement_natural_key_hash_uuid is null ;
	
commit;
		

/* WORK TABLE - UPDATE TGT RECORD
* This script finds the records where the new record from the source has a different check_sum than the current target record or the record is being ended/deleted. 
* The current record in the target will be ended since the source record will be inserted in the next step */
	
	INSERT /*+DIRECT*/ 
	into edw_work.party_tpp_rel_agreement_nb_requirement (
	dim_agreement_natural_key_hash_uuid ,
	ref_requirement_type_natural_key_hash_uuid ,
	requirement_case_id ,
	collection_methord_cde,
	requirement_category_cde ,
	requirement_comment_txt ,
	requirement_status_cde ,
	workbench_collection_id,
	workbench_collection_methord_cde,
	physician_full_nm ,
	requirement_status_dt ,
	source_requirement_status_cde ,
	source_requirement_cde ,
	source_requirement_category ,
	begin_dt ,
	begin_dtm ,
	row_process_dtm ,
	audit_id ,
	logical_delete_ind ,
	check_sum ,
	current_row_ind ,
	end_dt ,
	end_dtm ,
	source_system_id ,
	restricted_row_ind ,
	update_audit_id ,
	source_delete_ind,
	row_sid
	)
select
	tgt.dim_agreement_natural_key_hash_uuid ,
	tgt.ref_requirement_type_natural_key_hash_uuid ,
	tgt.requirement_case_id ,
	tgt.collection_methord_cde,
	tgt.requirement_category_cde ,
	tgt.requirement_comment_txt ,
	tgt.requirement_status_cde ,
	tgt.workbench_collection_id,
	tgt.workbench_collection_methord_cde,
	tgt.physician_full_nm ,
	tgt.requirement_status_dt ,
	tgt.source_requirement_status_cde ,
	tgt.source_requirement_cde ,
	tgt.source_requirement_category ,
	tgt.begin_dt ,
	tgt.begin_dtm ,
	CURRENT_TIMESTAMP(6)                     AS row_process_dtm ,
	tgt.audit_id ,
	tgt.logical_delete_ind ,
	tgt.check_sum ,
	FALSE                                 AS current_row_ind ,
	src.begin_dt - interval '1' day       as end_dt,
	src.begin_dtm - interval '1' second   as end_dtm  ,
	tgt.source_system_id ,
	tgt.restricted_row_ind ,
	src.update_audit_id ,
	tgt.source_delete_ind,
	tgt.row_sid
	from
		edw_staging.party_tpp_rel_agreement_nb_requirement_pre_work src
	join
		edw.rel_agreement_nb_requirement tgt
		on src.dim_agreement_natural_key_hash_uuid=tgt.dim_agreement_natural_key_hash_uuid
		and src.ref_requirement_type_natural_key_hash_uuid=tgt.ref_requirement_type_natural_key_hash_uuid
		and src.requirement_case_id=tgt.requirement_case_id
		and tgt.current_row_ind=True
	WHERE
		--change in check_sum
		(TGT.CHECK_SUM <> SRC.CHECK_SUM)
		--ending of a record (delete)
		OR (SRC.OPERATOR_IND = 'D' 
		AND TGT.CHECK_SUM = SRC.CHECK_SUM);
	
commit;
		

/* WORK TABLE - UPDATE WHERE RECORD ALREADY EXISTS IN TARGET */
INSERT /*+DIRECT*/ 
	into edw_work.party_tpp_rel_agreement_nb_requirement (
	dim_agreement_natural_key_hash_uuid ,
	ref_requirement_type_natural_key_hash_uuid ,
	requirement_case_id ,
	collection_methord_cde,
	requirement_category_cde ,
	requirement_comment_txt ,
	requirement_status_cde ,
	workbench_collection_id,
	workbench_collection_methord_cde,
	physician_full_nm ,
	requirement_status_dt ,
	source_requirement_status_cde ,
	source_requirement_cde ,
	source_requirement_category ,
	begin_dt ,
	begin_dtm ,
	row_process_dtm ,
	audit_id ,
	logical_delete_ind ,
	check_sum ,
	current_row_ind ,
	end_dt ,
	end_dtm ,
	source_system_id ,
	restricted_row_ind ,
	update_audit_id ,
	source_delete_ind
	)
select distinct 
	src.dim_agreement_natural_key_hash_uuid ,
	src.ref_requirement_type_natural_key_hash_uuid ,
	src.requirement_case_id ,
	src.collection_methord_cde,
	src.requirement_category_cde ,
	src.requirement_comment_txt ,
	src.requirement_status_cde ,
	src.workbench_collection_id,
	src.workbench_collection_methord_cde,
	src.physician_full_nm ,
	src.requirement_status_dt ,
	src.source_requirement_status_cde ,
	src.source_requirement_cde ,
	src.source_requirement_category ,
	src.begin_dt ,
	src.begin_dtm ,
	current_timestamp(6) as row_process_dtm ,
	src.audit_id ,
	src.logical_delete_ind ,
	src.check_sum ,
	src.current_row_ind ,
	src.end_dt ,
	src.end_dtm ,
	src.source_system_id ,
	src.restricted_row_ind ,
	src.update_audit_id ,
	src.source_delete_ind
	from
		edw_staging.party_tpp_rel_agreement_nb_requirement_pre_work src
	left outer join
		edw.rel_agreement_nb_requirement tgt
		on src.dim_agreement_natural_key_hash_uuid=tgt.dim_agreement_natural_key_hash_uuid
		and src.ref_requirement_type_natural_key_hash_uuid=tgt.ref_requirement_type_natural_key_hash_uuid
		and src.requirement_case_id=tgt.requirement_case_id
	where
		(  --HANDLE WHEN THERE ISN'T A CURRENT RECORD IN TARGET BUT THERE ARE HISTORICAL RECORDS AND A DELTA COMING THROUGH
       TGT.ROW_SID IS NULL
       AND (src.dim_agreement_natural_key_hash_uuid,
	src.ref_requirement_type_natural_key_hash_uuid,
	src.requirement_case_id) 
						IN
          (SELECT DISTINCT dim_agreement_natural_key_hash_uuid,
	ref_requirement_type_natural_key_hash_uuid,
	requirement_case_id
	from edw.rel_agreement_nb_requirement TGT1
		   WHERE TGT1.SOURCE_SYSTEM_ID IN ('32', '347'))
     )
    --handle when there is a current target record and either the check_sum has changed or record is being logically deleted.
    OR
	 (
       TGT.ROW_SID IS NOT NULL AND (TGT.CHECK_SUM <> SRC.CHECK_SUM) --checksum changed
     );
     
commit;

 SELECT ANALYZE_STATISTICS('edw_work.party_tpp_rel_agreement_nb_requirement');
 SELECT ANALYZE_STATISTICS('edw.rel_agreement_nb_requirement');