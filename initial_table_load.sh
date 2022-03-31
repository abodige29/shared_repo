#!/bin/bash
##Author: 
##FileName : 

clear
echo 'Pass the parameters from Jenkins '
db_host=$1
db_port=443
db_user=$2
db_password=$3
db_name='advana'
file_path=$4
filename=$5
src_sys_id_param=$6

type_of_load='initial'
pdate=$(date +%d-%b-%Y)

echo ${pdate}

#if [[ "$#" -ne 5 ]]
#then
#  echo "Usage:: sh test.sh ENV host_name user_name password dim_or_rel_tbl_nm source_system"
#  exit 2
#fi

echo pwd
cd ../../../../..
cd ${file_path}
echo pwd

function check_file()
{
echo 'Checking File'
if test -f ${filename}
then
	echo "File exists"
else
	"Error: File doesn't exists"
        exit 999
fi
}

function get_parse_audit_id_variable()
{
    parse_script_audit_id=$(grep ":audit_id" ${filename})
    if [ $? -eq 0 ]
    then
        echo "Results : Looks Good, audit_id present in the script --> " $parse_script_audit_id
    else
        "Error: Initial Load Cannot proceed....!!!! audit_id variable not found in the initial load script"
        exit 999
    fi

}

function load_history_tables()
{ 
    echo 'Invoke Load tables'
    echo 'You are connecting to           --->' $db_host
    echo 'You are connected as user       --->' $db_user
    echo 'The DB Port                     --->' $db_port
    echo 'Script Name:                     -->' $filename
	echo 'src_sys_id_param:                -->' $src_sys_id_param
    echo 'Load Type                       --->  History'
    echo 'NOTE: This shell script will execute automation for intial HISTORY LOAD'
    echo 'Date :: ' ${pdate}
    echo 'Results: Script Execution Stated on:' $(date)
    parse_script_src_id=$(grep ":src_sys_id_param" ${filename})

    if [ $? -eq 0 ]
    then
        echo 'SRC_ID_LOAD'
        vsql -h ${db_host} -p ${db_port}  -d advana -U ${db_user} -w ${db_password}  -o ${filename}_log.log -f ${filename} -v "audit_id=-1" -v "src_sys_id_param=${src_sys_id_param}" -v ON_ERROR_STOP=1
        if [ $? -eq 0 ]
        then
            echo 'Results: '${filename} 'Execution completed on:' $(date)
        else
            echo 'Results: '${filename} ' failed' >&2
            exit 1
        fi
    else
        echo 'AUDIT_ID_LOAD'
        vsql -h ${db_host} -p ${db_port}  -d advana -U ${db_user} -w ${db_password}  -o ${filename}_log.log -f ${filename} -v "audit_id=-1" -v ON_ERROR_STOP=1
        if [ $? -eq 0 ]
        then
            echo 'Results: '${filename} 'Execution completed on:' $(date)
        else
            echo 'Results: '${filename} ' failed' >&2
            exit 1
        fi
    fi

    echo "Results:The Results are stored under the " ${filename}_initial.log
}

echo "***********STEP-1******************************"
check_file
echo "***********STEP-2******************************"
get_parse_audit_id_variable
echo "***********STEP-3******************************"
load_history_tables