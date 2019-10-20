#!/usr/bin/env python
import multiprocessing
import subprocess
import json
import datasets
import dataset_files
import argparse
import os
import sys
import argparse_helper
import ask

def are_arguments_valid(args):
  # Check for mc_data
  if not argparse_helper.is_valid(args, 'mc_data', ['mc', 'data']):
    return False, 'mc_data: '+str(args['mc_data'])+' is not valid.'
  
  # Check if files exists with in_results_prefix
  if 'mc' in args['mc_data']:
    t_path = os.path.join(args['in_results_folder'], args['in_results_prefix']+'mc_dataset_paths')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

  if 'data' in args['mc_data']:
    t_path = os.path.join(args['in_results_folder'], args['in_results_prefix']+'data_dataset_paths')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

  # Check if files exists with in_jsons_prefix
  if 'mc' in args['mc_data']:
    t_path = os.path.join(args['in_jsons_folder'], args['in_jsons_prefix']+'mc_dataset_files_info.json')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

  if 'data' in args['mc_data']:
    t_path = os.path.join(args['in_jsons_folder'], args['in_jsons_prefix']+'data_dataset_files_info.json')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

  # Check if output folder exits
  if not os.path.isdir(args['out_jsons_folder']):
    return False, 'out_jsons_folder: '+args['out_jsons_folder']+" doesn't exist."

  # Check if files exists with out_jsons_prefix 
  if 'mc' in args['mc_data']:
    t_path = os.path.join(args['out_jsons_folder'], args['out_jsons_prefix']+'mc_dataset_files_info.json')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

  if 'data' in args['mc_data']:
    t_path = os.path.join(args['out_jsons_folder'], args['out_jsons_prefix']+'data_dataset_files_info.json')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

  return True, ''

def remove_dataset_files_info(dataset_files_info, remove_list_dataset):
  for dataset in remove_list_dataset:
    del dataset_files_info[dataset]

#datasets_files_info[dataset][filename] = {'number_events':number_events}
def combine_dataset_files_info(dataset_files_info_a, dataset_files_info_b):
  combined_dataset_files_info = {}
  for dataset in dataset_files_info_a:
    combined_dataset_files_info[dataset] = {}
    for filename in dataset_files_info_a[dataset]:
      combined_dataset_files_info[dataset][filename] = dataset_files_info_a[dataset][filename]
  for dataset in dataset_files_info_b:
    for filename in dataset_files_info_b[dataset]:
      if dataset not in combined_dataset_files_info:
        combined_dataset_files_info[dataset] = {}
      if filename not in combined_dataset_files_info:
        combined_dataset_files_info[dataset][filename] = dataset_files_info_b[dataset][filename]
      else:
        if combined_dataset_files_info[dataset][filename] != dataset_files_info_b[dataset][filename]:
          print('[Error] combined_dataset_files_info and dataset_files_info_b is different.')
          print('  combined_dataset_files_info['+dataset+']['+filename+'] = '+str(combined_dataset_files_info[dataset][filename]))
          print('  dataset_files_info_b['+dataset+']['+filename+'] = '+str(dataset_files_info_b[dataset][filename]))
  return combined_dataset_files_info

def update_datasets_files_json(path_datasets_filename, in_dataset_files_info_filename, out_dataset_files_info_filename):
  list_dataset = dataset_files.get_list_dataset(path_datasets_filename)
  #dataset_files_info[dataset][filename] = {'number_events':number_events}
  in_dataset_files_info = datasets.load_json_file(in_dataset_files_info_filename)

  in_list_dataset = in_dataset_files_info.keys()
  append_list_dataset = list(set(list_dataset) - set(in_list_dataset))
  remove_list_dataset = list(set(in_list_dataset) - set(list_dataset))
  # Get files for each dataset
  append_dataset_file_commands = dataset_files.make_dataset_file_commands(append_list_dataset)
  #dataset_file_commands = [[dataset, commands]]
  append_dataset_file_results = dataset_files.run_list_command(append_dataset_file_commands)
  #datasets_files_info[dataset][filename] = {'number_events':number_events}
  append_dataset_files_info = dataset_files.parse_dataset_file_results(append_dataset_file_results)

  # Get meta for each file
  append_dataset_meta_commands = dataset_files.make_dataset_meta_commands(append_dataset_files_info)
  append_dataset_meta_results = dataset_files.run_list_command(append_dataset_meta_commands)
  dataset_files.parse_dataset_meta_results(append_dataset_meta_results, append_dataset_files_info)

  remove_dataset_files_info(in_dataset_files_info, remove_list_dataset)
  out_dataset_files_info = combine_dataset_files_info(in_dataset_files_info, append_dataset_files_info)

  print('appended list_dataset: ',str(append_list_dataset))
  print('removed list_dataset: ',str(remove_list_dataset))

  datasets.save_json_file(out_dataset_files_info, out_dataset_files_info_filename)


if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Updates dataset_files_jsons.')
  parser.add_argument('-t', '--mc_data', metavar='"mc,data"', nargs=1, default=['mc', 'data'])
  parser.add_argument('-ir', '--in_results_folder', metavar='./results', nargs=1, default=['./results'])
  parser.add_argument('-irp', '--in_results_prefix', metavar="''", nargs=1, default=[''])
  parser.add_argument('-ij', '--in_jsons_folder', metavar='./jsons', nargs=1, default=['./jsons'])
  parser.add_argument('-ijp', '--in_jsons_prefix', metavar="''", nargs=1, default=[''])
  parser.add_argument('-o', '--out_jsons_folder', metavar='./jsons', nargs=1, default=['./jsons'])
  parser.add_argument('-op', '--out_jsons_prefix', metavar="'updated_'", nargs=1, default=['updated_'])

  args = vars(parser.parse_args())
  argparse_helper.initialize_arguments(args, list_args=['mc_data'])
  valid, log = are_arguments_valid(args)
  if not valid:
    print('[Error] '+log)
    sys.exit()

  do_mc = False
  if 'mc' in args['mc_data']: do_mc = True
  do_data = False
  if 'data' in args['mc_data']: do_data = True

  path_mc_datasets_filename = os.path.join(args['in_results_folder'], args['in_results_prefix']+'mc_dataset_paths')
  path_data_datasets_filename = os.path.join(args['in_results_folder'], args['in_results_prefix']+'data_dataset_paths')
  in_mc_dataset_files_info_filename = os.path.join(args['in_jsons_folder'], args['in_jsons_prefix']+'mc_dataset_files_info.json')
  in_data_dataset_files_info_filename = os.path.join(args['in_jsons_folder'], args['in_jsons_prefix']+'data_dataset_files_info.json')

  out_mc_dataset_files_info_filename = os.path.join(args['out_jsons_folder'], args['out_jsons_prefix']+'mc_dataset_files_info.json')
  out_data_dataset_files_info_filename = os.path.join(args['out_jsons_folder'], args['out_jsons_prefix']+'data_dataset_files_info.json')

  if do_mc:
    update_datasets_files_json(path_mc_datasets_filename, in_mc_dataset_files_info_filename, out_mc_dataset_files_info_filename)

  if do_data:
    update_datasets_files_json(path_data_datasets_filename, in_data_dataset_files_info_filename, out_data_dataset_files_info_filename)

