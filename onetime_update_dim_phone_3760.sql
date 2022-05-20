/*update dummy record in rel_contact*/

update edw.rel_contact_preference
set dim_phone_natural_key_hash_uuid=uuid_gen(NULL)::uuid,
row_process_dtm=current_timestamp(6)
where dim_phone_natural_key_hash_uuid='2c4f5c40-866f-54f4-4ef6-cf633348309f';

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