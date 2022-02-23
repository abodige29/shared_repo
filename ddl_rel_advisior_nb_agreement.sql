CREATE TABLE EDW_WORK.party_rel_advisor_nb_agreement
(
dim_agreement_natural_key_hash_uuid uuid NOT NULL,
dim_party_natural_key_hash_uuid uuid NOT NULL,
ref_party_role_natural_key_hash_uuid uuid NOT NULL,
ref_party_sub_role_natural_key_hash_uuid uuid NOT NULL,
agency_dim_party_natural_key_hash_uuid uuid,
distributor_dim_party_natural_key_hash_uuid uuid,
advisor_first_year_commission_prc numeric(9,6),
advisor_renewal_commission_prc numeric(9,6),
owner_detail_cde varchar(50),
source_owner_detail_cde varchar(50),
business_partner_commission_split_id varchar(50),
source_party_role_cde varchar(50),
source_party_sub_role_cde varchar(50),
annual_commission_amt numeric(17,4),
source_bp_id varchar(50),
source_scarab_id varchar(50),
source_agency_id varchar(50),
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
row_sid IDENTITY ,
update_audit_id int NOT NULL DEFAULT 0,
source_delete_ind boolean NOT NULL,
CONSTRAINT C_PRIMARY PRIMARY KEY (row_sid) DISABLED
);