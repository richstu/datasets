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
  
  # Check if files exists with in_json_prefix
  if 'mc' in args['mc_data']:
    t_path = os.path.join(args['in_results_folder'], args['in_results_prefix']+'mc_dataset_paths')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

  if 'data' in args['mc_data']:
    t_path = os.path.join(args['in_results_folder'], args['in_results_prefix']+'data_dataset_paths')
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

def make_dataset_files_jsons(path_datasets_filename, out_dataset_files_info_filename):
  list_dataset = dataset_files.get_list_dataset(path_datasets_filename)
  # Get files for each dataset
  dataset_file_commands = dataset_files.make_dataset_file_commands(list_dataset)
  #dataset_file_commands = [[dataset, commands]]
  dataset_file_results = dataset_files.run_list_command(dataset_file_commands)
  #datasets_files_info[dataset][filename] = {'number_events':number_events}
  dataset_files_info = dataset_files.parse_dataset_file_results(dataset_file_results)

  # Get meta for each file
  dataset_meta_commands = dataset_files.make_dataset_meta_commands(dataset_files_info)
  dataset_meta_results = dataset_files.run_list_command(dataset_meta_commands)
  dataset_files.parse_dataset_meta_results(dataset_meta_results, dataset_files_info)
  nested_dict.save_json_file(dataset_files_info, out_dataset_files_info_filename)


if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Makes dataset_files_jsons.')
  parser.add_argument('-t', '--mc_data', metavar='"mc,data"', nargs=1, default=['mc', 'data'])
  parser.add_argument('-i', '--in_results_folder', metavar='./results', nargs=1, default=['./results'])
  parser.add_argument('-ip', '--in_results_prefix', metavar="''", nargs=1, default=[''])
  parser.add_argument('-o', '--out_jsons_folder', metavar='./jsons', nargs=1, default=['./jsons'])
  parser.add_argument('-op', '--out_jsons_prefix', metavar="''", nargs=1, default=[''])

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
  out_mc_dataset_files_info_filename = os.path.join(args['out_jsons_folder'], args['out_jsons_prefix']+'mc_dataset_files_info.json')
  out_data_dataset_files_info_filename = os.path.join(args['out_jsons_folder'], args['out_jsons_prefix']+'data_dataset_files_info.json')

  if do_mc:
    make_dataset_files_jsons(path_mc_datasets_filename, out_mc_dataset_files_info_filename)

  if do_data:
    make_dataset_files_jsons(path_data_datasets_filename, out_data_dataset_files_info_filename)

