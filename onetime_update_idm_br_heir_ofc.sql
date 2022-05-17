update edw_external_input_data.idm_br_hier_ofc 
set ofc_id=q1.ofc_id_new
from(
select voltageprotect(clean_string(voltageaccess(ofc_id,'sorparty')),'sorparty') as ofc_id_new,
voltageprotect(clean_string(lpad(voltageaccess(ofc_id,'sorparty'),10,'0')),'sorparty') as ofc_id_old
from EDW_STAGING.IDM_BR_HIER_OFC_SNAPSHOT
)q1
where source_system_id in ('15','356')
and  q1.ofc_id_old=ofc_id;

commit;
