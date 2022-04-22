CREATE TABLE edw_staging.party_ipm_rel_non_mastered_party_nb_agreement_pre_wok
(
	dim_agreement_natural_key_hash_uuid uuid NOT NULL,
    dim_non_mastered_party_natural_key_hash_uuid uuid NOT NULL,
    ref_party_role_natural_key_hash_uuid uuid NOT NULL,
    begin_dt date NOT NULL DEFAULT '0001-01-01'::date,
    begin_dtm timestamp(6) NOT NULL,
    row_process_dtm timestamp(6) NOT NULL,
    audit_id int NOT NULL DEFAULT 0,
    logical_delete_ind boolean NOT NULL DEFAULT false,
    check_sum uuid NOT NULL,
    current_row_ind boolean NOT NULL DEFAULT true,
    end_dt date NOT NULL DEFAULT '9999-12-31'::date,
    end_dtm timestamp(6) NOT NULL,
    source_system_id varchar(50) NOT NULL,
    restricted_row_ind boolean NOT NULL DEFAULT false,
    row_sid int,
    update_audit_id int NOT NULL DEFAULT 0,
    source_delete_ind boolean NOT NULL,
	operator_ind varchar(1)
); 
