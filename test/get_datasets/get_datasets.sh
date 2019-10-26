#!/bin/bash

rm -rf jsons
rm -rf results
rm -rf updated_results
rm -rf updated_jsons
rm -rf files
mkdir jsons
mkdir results
mkdir updated_jsons
mkdir updated_results
mkdir files

make_datasets_jsons.py
filter_datasets_jsons.py
select_multiple_datasets_jsons.py
write_datasets.py
make_dataset_files_jsons.py
write_dataset_files.py
convert_dataset_files_to_cl.py mc ./files ./results/cl_mc_dataset_files_info.py
convert_dataset_files_to_cl.py data ./files ./results/cl_data_dataset_files_info.py

copy_aods.py /store/mc/RunIISummer16NanoAODv5/WZTo3LNu_TuneCUETP8M1_13TeV-powheg-pythia8/NANOAODSIM/PUMoriond17_Nano1June2019_102X_mcRun2_asymptotic_v7-v1/120000/7EC09E9F-A061-6D46-83DB-E91A8479CD7A.root files/NanoAODv5/nano/2016/mc
copy_aods.py /store/data/Run2016B_ver1/MET/NANOAOD/Nano1June2019_ver1-v1/60000/538825E4-0615-6C48-BE0D-33507A68B0B2.root files/NanoAODv5/nano/2016/data

make_disk_files_jsons.py -b ./files -o updated_jsons

update_datasets_jsons.py -m updated_meta -o updated_jsons
filter_datasets_jsons.py -m updated_meta -i updated_jsons -ip updated_ -o updated_jsons
select_multiple_datasets_jsons.py -m updated_meta -i updated_jsons -o updated_jsons
write_datasets.py -m updated_meta -i updated_jsons -o updated_results
update_dataset_files_jsons.py -ir updated_results -o updated_jsons
write_dataset_files.py -m updated_meta -i updated_jsons -if updated_ -id updated_ -o updated_results
convert_dataset_files_to_cl.py mc ./files ./updated_results/cl_mc_dataset_files_info.py -m updated_meta -i updated_jsons -if updated_
convert_dataset_files_to_cl.py data ./files ./updated_results/cl_data_dataset_files_info.py -m updated_meta -i updated_jsons -if updated_
convert_dataset_files_to_cl.py zjets ./files ./updated_results/cl_zjets_dataset_files_info.py -m updated_meta -i updated_jsons -if updated_

# Compare results
echo "Comparing results"
echo "For cl_mc_dataset_files_info.py"
diff <(./results/cl_mc_dataset_files_info.py | cut -d' ' -f2 | sort) <(./gold_results/cl_mc_dataset_files_info.py | cut -d' ' -f2 |sort)
echo "For cl_data_dataset_files_info.py"
diff <(./results/cl_data_dataset_files_info.py | cut -d' ' -f2 | sort) <(./gold_results/cl_data_dataset_files_info.py | cut -d' ' -f2 |sort)

echo "For updated/cl_mc_dataset_files_info.py"
diff <(./updated_results/cl_mc_dataset_files_info.py | cut -d' ' -f2 | sort) <(./gold_results/cl_updated_mc_dataset_files_info.py | cut -d' ' -f2 |sort)
echo "For updated/cl_data_dataset_files_info.py"
diff <(./updated_results/cl_data_dataset_files_info.py | cut -d' ' -f2 | sort) <(./gold_results/cl_updated_data_dataset_files_info.py | cut -d' ' -f2 |sort)
echo "For updated/cl_zjet_dataset_files_info.py"
diff <(./updated_results/cl_zjets_dataset_files_info.py | cut -d' ' -f2 | sort) <(./gold_results/cl_zjets_dataset_files_info.py | cut -d' ' -f2 |sort)
