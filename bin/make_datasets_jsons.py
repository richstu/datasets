#!/usr/bin/env python
import datasets
import nested_dict
import json
import datetime
import argparse
import os
import sys
import argparse_helper
import ask

def are_arguments_valid(args, eras):
  # Check for data_tiers
  if not argparse_helper.is_valid(args, 'data_tiers', ['nanoaod', 'miniaod']):
    return False, 'data_tier: '+str(args['data_tiers'])+' is not valid.'
  
  # Check for mc_data
  if not argparse_helper.is_valid(args, 'mc_data', ['mc', 'data']):
    return False, 'mc_data: '+str(args['mc_data'])+' is not valid.'

  # Check for meta files
  if not os.path.isdir(args['meta_folder']):
    return False, 'meta_folder: '+args['meta_folder']+" doesn't exist."

  for era in eras:
    t_path = os.path.join(args['meta_folder'],'mc_dataset_'+era+'_names')
    if not os.path.isfile(os.path.join(t_path)):
      return False, 'meta_mc_dataset_'+era+'_names: '+t_path+" doesn't exist."

  t_path = os.path.join(args['meta_folder'],'mc_dataset_run2common_names')
  if not os.path.isfile(os.path.join(t_path)):
    return False, 'meta_mc_run2dataset_common: '+t_path+" doesn't exist."

  t_path = os.path.join(args['meta_folder'],'mc_dataset_run3common_names')
  if not os.path.isfile(os.path.join(t_path)):
    return False, 'meta_mc_run3dataset_common: '+t_path+" doesn't exist."

  if 'mc' in args['mc_data']:
    t_path = os.path.join(args['meta_folder'],'mc_tag_meta')
    if not os.path.isfile(os.path.join(t_path)):
      return False, 'meta_mc_tag_meta: '+t_path+" doesn't exist."

  if 'data' in args['mc_data']:
    t_path = os.path.join(args['meta_folder'],'data_tag_meta')
    if not os.path.isfile(os.path.join(t_path)):
      return False, 'meta_data_tag_meta: '+t_path+" doesn't exist."
 
  # Check if output folder exits
  if not os.path.isdir(args['out_json_folder']):
    return False, 'out_json_folder: '+args['out_json_folder']+" doesn't exist."

  # Check if files exists with out_json_prefix 
  if 'mc' in args['mc_data']:
    t_path = os.path.join(args['out_json_folder'], args['out_json_prefix']+'mc_datasets.json')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

  if 'data' in args['mc_data']:
    t_path = os.path.join(args['out_json_folder'], args['out_json_prefix']+'data_datasets.json')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

  return True, ''

if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Makes datasets_jsons using dasgoclient.')
  parser.add_argument('-m', '--meta_folder', metavar='./meta', nargs=1, default=['./meta'])
  parser.add_argument('-d', '--data_tiers', metavar='"nanoaod"', nargs=1, default=['nanoaod'])
  parser.add_argument('-t', '--mc_data', metavar='"mc,data"', nargs=1, default=['mc', 'data'])
  parser.add_argument('-o', '--out_json_folder', metavar='./jsons', nargs=1, default=['./jsons'])
  parser.add_argument('-p', '--out_json_prefix', metavar="''", nargs=1, default=[''])

  eras = ['2016', '2016APV', '2017', '2018', '2022', '2022EE', '2023', '2023BPix']

  args = vars(parser.parse_args())
  argparse_helper.initialize_arguments(args, list_args=['data_tiers','mc_data'])
  valid, log = are_arguments_valid(args, eras=eras)
  if not valid:
    print('[Error] '+log)
    sys.exit()

  make_mc_datasets = False
  if 'mc' in args['mc_data']: make_mc_datasets = True
  make_data_datasets = False
  if 'data' in args['mc_data']: make_data_datasets = True
  meta_folder = args['meta_folder']
  data_tiers = args['data_tiers']

  mc_tag_meta_filename = meta_folder+'/mc_tag_meta'
  data_tag_meta_filename = meta_folder+'/data_tag_meta'
  out_mc_datasets_filename = os.path.join(args['out_json_folder'],args['out_json_prefix']+'mc_datasets.json')
  out_data_datasets_filename = os.path.join(args['out_json_folder'],args['out_json_prefix']+'data_datasets.json')
 
  if make_mc_datasets:
    datasets_era = [
      [meta_folder+'/mc_dataset_run2common_names', ['2016', '2016APV', '2017', '2018']],
      [meta_folder+'/mc_dataset_run3common_names', ['2022', '2022EE', '2023', '2023BPix']],
      ]
    for era in eras:
      datasets_era.append([meta_folder+'/mc_dataset_'+era+'_names', [era]])
    # mc_dataset_names[year] = [(mc_dataset_name, path)]
    mc_dataset_names = datasets.parse_multiple_mc_dataset_names(datasets_era)

    #print ('dataset_names:', mc_dataset_names)
    # Ex) tag_meta[2016] = RunIISummer16, MiniAODv3, NanoAODv5
    mc_tag_meta = datasets.parse_mc_tag_meta(mc_tag_meta_filename)

  if make_data_datasets:
    # data_tag_meta[year][run_group][streams][data_tier] = reco_tag
    data_tag_meta = datasets.parse_data_tag_meta(data_tag_meta_filename)


  if make_mc_datasets:
    # mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
    mc_datasets = datasets.make_mc_datasets(mc_dataset_names, mc_tag_meta, data_tiers)
    #datasets.print_path_mc_datasets(mc_datasets)
    nested_dict.save_json_file(mc_datasets, out_mc_datasets_filename)

  if make_data_datasets:
    # data_dataset[stream][year][run_group][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
    data_datasets = datasets.make_data_datasets(data_tag_meta, data_tiers)
    #datasets.print_path_data_datasets(data_datasets)
    nested_dict.save_json_file(data_datasets, out_data_datasets_filename)
