#!/usr/bin/env python
import nested_dict
import os
import argparse
import os
import sys
import argparse_helper
import ask

def are_arguments_valid(args):
  # Check if files exists with in_json_prefix
  if not os.path.isfile(args['in_dataset_files_info']):
    return False, args['in_dataset_files_info']+' does not exists.'

  if not os.path.isdir(args['target_directory']):
    return False, args['target_directory']+' does not exists.'

  # Check if files exists with out_jsons_prefix 
  if os.path.isfile(args['out_command_lines']):
    overwrite = ask.ask_key(args['out_command_lines']+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
    if overwrite == 'n':
      return False, args['out_command_lines']+' already exists.'

  return True, ''

if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Makes command lines from mc_dataset_files_info.')
  parser.add_argument('in_dataset_files_info', metavar='./jsons/DATASET_FILES_INFO.json')
  parser.add_argument('target_directory', metavar='/mnt/hadoop/jbkim/Download')
  parser.add_argument('out_command_lines', metavar='./results/CL_DATASET_FILES_INFO.py')

  args = vars(parser.parse_args())
  argparse_helper.initialize_arguments(args, list_args=['data_tiers','mc_data'])
  valid, log = are_arguments_valid(args)
  if not valid:
    print('[Error] '+log)
    sys.exit()

  dataset_files_info_filename = args['in_dataset_files_info']
  command_list_filename = args['out_command_lines']

  command_list_string = ''
  command_list_string = ''
  command_list_string += '#!/bin/env python\n'
  #command_list_string += "base_command = './run_scripts/copy_aods.py'\n"
  command_list_string += "base_command = '"+os.environ['JB_DATASETS_DIR']+"/bin/copy_aods.py'\n"
  command_list_string += "base_folder = '"+args['target_directory']+"'\n"
  command_list_string += "mid_folder = ''\n"
  command_list_string += "print('# [global_key] dataset_files_info_filename : "+dataset_files_info_filename+"')\n"

  #datasets_files_info[dataset][filename] = {'number_events':number_events}
  dataset_files_info = nested_dict.load_json_file(dataset_files_info_filename)

  for dataset in dataset_files_info:
    for filename in dataset_files_info[dataset]:
      command_list_string += "print(base_command+' "+filename+" '+base_folder+mid_folder)\n"

  with open(command_list_filename,'w') as command_list_file:
    command_list_file.write(command_list_string)
    print('Wrote to '+command_list_filename)
    os.system('chmod +x '+command_list_filename)

