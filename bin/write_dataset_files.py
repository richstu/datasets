#!/usr/bin/env python
import datasets
import nested_dict
import filter_datasets_jsons
import argparse
import os
import sys
import argparse_helper
import ask

def are_arguments_valid(args):
  # Check for data_tiers
  if not argparse_helper.is_valid(args, 'data_tiers', ['nanoaod', 'miniaod']):
    return False, 'data_tier: '+str(args['data_tiers'])+' is not valid.'
  
  # Check for mc_data
  if not argparse_helper.is_valid(args, 'mc_data', ['mc', 'data']):
    return False, 'mc_data: '+str(args['mc_data'])+' is not valid.'

  # Check if files exists with in_json_prefix
  if 'mc' in args['mc_data']:
    t_path = os.path.join(args['in_json_folder'], args['in_json_prefix']+'mc_dataset_files_info.json')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

  if 'data' in args['mc_data']:
    t_path = os.path.join(args['in_json_folder'], args['in_json_prefix']+'data_dataset_files_info.json')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

  # Check if output folder exits
  if not os.path.isdir(args['out_results_folder']):
    return False, 'out_results_folder: '+args['out_results_folder']+" doesn't exist."


  # Check if files exists with out_results_prefix 
  if 'mc' in args['mc_data']:
    t_path = os.path.join(args['out_results_folder'], args['out_results_prefix']+'mc_dataset_files')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

  if 'data' in args['mc_data']:
    t_path = os.path.join(args['out_results_folder'], args['out_results_prefix']+'data_dataset_files')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

  return True, ''

def write_list(target_list, out_filename):
  with open(out_filename,'w') as out_file:
    for item in target_list:
      out_file.write(item+'\n')

# datasets_files_info[dataset][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
# files_filename = [parsed_filename]
def write_dataset_files(dataset_files_info_filename, files_filename):
    files = []
    # datasets_files_info[dataset][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
    dataset_files_info = nested_dict.load_json_file(dataset_files_info_filename)
    for dataset in dataset_files_info:
      for filename in dataset_files_info[dataset]:
        files.append(datasets.filename_to_parsed(filename))
    write_list(files, files_filename)
    print('Wrote to '+files_filename)

if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Selects datasets_jsons.')
  parser.add_argument('-t', '--mc_data', metavar='"mc,data"', nargs=1, default=['mc,data'])
  parser.add_argument('-d', '--data_tiers', metavar='"nanoaod,miniaod"', nargs=1, default=['nanoaod', 'miniaod'])
  parser.add_argument('-i', '--in_json_folder', metavar='./jsons', nargs=1, default=['./jsons'])
  parser.add_argument('-ip', '--in_json_prefix', metavar='', nargs=1, default=[''])
  parser.add_argument('-o', '--out_results_folder', metavar='./results', nargs=1, default=['./results'])
  parser.add_argument('-op', '--out_results_prefix', metavar="''", nargs=1, default=[''])

  args = vars(parser.parse_args())
  argparse_helper.initialize_arguments(args, list_args=['data_tiers','mc_data'])
  valid, log = are_arguments_valid(args)
  if not valid:
    print('[Error] '+log)
    sys.exit()

  #mc_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_files_prefix']+'mc_dataset_files_info.json')
  #data_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_files_prefix']+'data_dataset_files_info.json')

  mc_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_json_prefix']+'mc_dataset_files_info.json')
  data_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_json_prefix']+'data_dataset_files_info.json')
  mc_files_filename = os.path.join(args['out_results_folder'], args['out_results_prefix']+'mc_dataset_files')
  data_files_filename = os.path.join(args['out_results_folder'], args['out_results_prefix']+'data_dataset_files')

  do_mc = False
  if 'mc' in args['mc_data']: do_mc = True
  do_data = False
  if 'data' in args['mc_data']: do_data = True

  if do_mc:
    write_dataset_files(mc_dataset_files_info_filename, mc_files_filename)
    #mc_files = []
    ## datasets_files_info[dataset][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
    #mc_dataset_files_info = nested_dict.load_json_file(mc_dataset_files_info_filename)
    #for dataset in mc_dataset_files_info:
    #  for filename in mc_dataset_files_info[dataset]:
    #    mc_files.append(datasets.filename_to_parsed(filename))
    #write_list(mc_files, mc_files_filename)
    #print('Wrote to '+mc_files_filename)


  if do_data:
    write_dataset_files(data_dataset_files_info_filename, data_files_filename)
    ## datasets_files_info[dataset][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
    #dataset_files_info_filename = data_dataset_files_info_filename
    #data_dataset_files_info = nested_dict.load_json_file(data_dataset_files_info_filename)
    #for dataset in data_dataset_files_info:
    #  for filename in data_dataset_files_info[dataset]:
    #    data_files.append(datasets.data_filename_to_parsed(filename))
    #write_list(data_files, data_files_filename)
    #print('Wrote to '+data_files_filename)
