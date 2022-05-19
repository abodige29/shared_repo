
/* insert a dummy record for NULL phone */


INSERT INTO edw.dim_phone (dim_phone_natural_key_hash_uuid,phone_nr,extension_nr,country_cde,area_cde,dial_nr,
primary_phone_ind,begin_dt,begin_dtm,row_process_dtm,audit_id,logical_delete_ind,check_sum,current_row_ind,
end_dt,end_dtm,source_system_id,restricted_row_ind,update_audit_id,
exchange_nr,source_system_phone_id,source_delete_ind) 
VALUES ('2c4f5c40-866f-54f4-4ef6-cf633348309f','',NULL,NULL,NULL,NULL,NULL,'0001-01-01'::date,'0001-01-01'::timestamp(6),current_timestamp(6),-1,false,uuid_gen(false::Boolean)::uuid,true,'9999-12-31','9999-12-31'::timestamp(6),'-1',false,-1,NULL,NULL,false);

commit;

/* deactivate bad records in RCP table */

create local temporary table bad_records1 on commit preserve rows as
select distinct rcp.dim_phone_natural_key_hash_uuid as rcp_dim_phone,rcp.*
from edw.rel_contact_preference rcp
left join edw.dim_phone prd
on rcp.dim_phone_natural_key_hash_uuid = prd.dim_phone_natural_key_hash_uuid
and prd.current_row_ind = True
where prd.dim_phone_natural_key_hash_uuid is null;


update edw.rel_contact_preference p
set current_row_ind='False',logical_delete_ind='True',row_process_dtm=current_timestamp(6) 
from(
select * from bad_records1
)q
where p.dim_phone_natural_key_hash_uuid=q.rcp_dim_phone
and p.row_sid=q.row_sid;

commit;