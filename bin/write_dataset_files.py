#!/usr/bin/env python
import datasets
import nested_dict
import filter_datasets_jsons
import argparse
import os
import sys
import argparse_helper
import ask

def write_list(target_list, out_filename):
  with open(out_filename,'w') as out_file:
    for item in target_list:
      out_file.write(item+'\n')

if __name__ == '__main__':

  #mc_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_files_prefix']+'mc_dataset_files_info.json')
  #data_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_files_prefix']+'data_dataset_files_info.json')

  mc_dataset_files_info_filename = './jsons/mc_dataset_files_info.json'
  data_dataset_files_info_filename = './jsons/data_dataset_files_info.json'
  mc_files_filename = './results/mc_dataset_files'
  data_files_filename = './results/data_dataset_files'

  do_mc = True
  do_data = False

  if do_mc:
    mc_files = []
    # datasets_files_info[dataset][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
    mc_dataset_files_info = nested_dict.load_json_file(mc_dataset_files_info_filename)
    for dataset in mc_dataset_files_info:
      for filename in mc_dataset_files_info[dataset]:
        mc_files.append(datasets.mc_filename_to_parsed(filename))
    write_list(mc_files, mc_files_filename)
    print('Wrote to '+mc_files_filename)


  # TODO for data
  if do_data:
    # datasets_files_info[dataset][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
    dataset_files_info_filename = data_dataset_files_info_filename
