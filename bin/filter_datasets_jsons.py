#!/usr/bin/env python
import datasets
import nested_dict
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

  # Check for meta files
  if not os.path.isdir(args['meta_folder']):
    return False, 'meta_folder: '+args['meta_folder']+" doesn't exist."

  t_path = os.path.join(args['meta_folder'],'mc_dataset_common_names')
  if not os.path.isfile(os.path.join(t_path)):
    return False, 'meta_mc_dataset_common: '+t_path+" doesn't exist."

  t_path = os.path.join(args['meta_folder'],'mc_dataset_2016_names')
  if not os.path.isfile(os.path.join(t_path)):
    return False, 'meta_mc_dataset_2016_names: '+t_path+" doesn't exist."

  t_path = os.path.join(args['meta_folder'],'mc_dataset_2017_names')
  if not os.path.isfile(os.path.join(t_path)):
    return False, 'meta_mc_dataset_2017_names: '+t_path+" doesn't exist."

  t_path = os.path.join(args['meta_folder'],'mc_dataset_2018_names')
  if not os.path.isfile(os.path.join(t_path)):
    return False, 'meta_mc_dataset_2018_names: '+t_path+" doesn't exist."

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

  # Check if files exists with in_json_prefix
  if 'mc' in args['mc_data']:
    t_path = os.path.join(args['in_json_folder'], args['in_json_prefix']+'mc_datasets.json')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

  if 'data' in args['mc_data']:
    t_path = os.path.join(args['in_json_folder'], args['in_json_prefix']+'data_datasets.json')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

  # Check if files exists with out_json_prefix 
  if 'mc' in args['mc_data']:
    t_path = os.path.join(args['out_json_folder'], args['out_json_prefix']+'mc_datasets.json')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

    t_path = os.path.join(args['out_json_folder'], args['out_json_prefix']+'bad_pu_mc_datasets.json')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

    t_path = os.path.join(args['out_json_folder'], args['out_json_prefix']+'bad_ps_weight_mc_datasets.json')
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

def reject_string_ignore_case_mc_datasets(string, mc_datasets, mc_dataset_name, year, data_tier, path):
  #print(string, path, mc_dataset_name, year, data_tier, mc_datasets[mc_dataset_name][year][data_tier])
  return string.lower() in path.lower() 

def reject_string_ignore_case_data_datasets(string, data_datasets, stream, year, run_group, data_tier, path):
  path_info = data_datasets[stream][year][run_group][data_tier][path]
  return string.lower() in path.lower() 

# reject_data_function should have input as (reject_input, data_datasets, stream, year, run_group, data_tier, path)
def reject_string_ignore_case_path_parent_data_datasets(string, data_datasets, stream, year, run_group, data_tier, path):
  path_info = data_datasets[stream][year][run_group][data_tier][path]
  reject = False
  if string.lower() in path_info['parent_chain'][0].lower(): reject = True
  if string.lower() in path.lower(): reject = True
  return reject

## reject_function should have input as (reject_input, mc_datasets, mc_dataset_name, year, data_tier, path)
#def reject_bad_pu_2017_mc_datasets(dummy_input, mc_datasets, mc_dataset_name, year, data_tier, path):
#  path_info = mc_datasets[mc_dataset_name][year][data_tier][path]
#  if year == '2017':
#    if data_tier == 'miniaod':
#      if len(path_info['parent_chain']) < 1: return True
#      parent = path_info['parent_chain'][0]
#      if not 'PU2017' in parent: return True
#      return False
#    elif data_tier == 'nanoaod':
#      if len(path_info['parent_chain']) < 2: return True
#      parent_parent = path_info['parent_chain'][1]
#      if not 'PU2017' in parent_parent: return True
#      return False
#  return False

# reject_function should have input as (reject_input, mc_datasets, mc_dataset_name, year, data_tier, path)
def get_rejected_mc_datasets(mc_datasets, reject_mc_function, reject_input):
  rejected_mc_datasets = {}
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        for path in mc_datasets[mc_dataset_name][year][data_tier]:
          if reject_mc_function(reject_input, mc_datasets, mc_dataset_name, year, data_tier, path):
            path_info = mc_datasets[mc_dataset_name][year][data_tier][path]
            nested_dict.fill_nested_dict(rejected_mc_datasets, [mc_dataset_name, year, data_tier, path], path_info)
  return rejected_mc_datasets

# reject_data_function should have input as (reject_input, data_datasets, stream, year, run_group, data_tier, path)
def get_rejected_data_datasets(data_datasets, reject_data_function, reject_input):
  rejected_data_datasets = {}
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        for data_tier in data_datasets[stream][year][run_group]:
          for path in data_datasets[stream][year][run_group][data_tier]:
            if reject_data_function(reject_input, data_datasets, stream, year, run_group, data_tier, path):
              path_info = data_datasets[stream][year][run_group][data_tier][path]
              nested_dict.fill_nested_dict(rejected_data_datasets, [stream, year, run_group, data_tier, path], path_info)
  return rejected_data_datasets

# reject_mc_function should have input as (reject_input, mc_datasets, mc_dataset_name, year, data_tier, path)
# mc_dataset[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
def filter_mc_datasets(mc_datasets, reject_mc_function, reject_input=None):
  rejected_mc_datasets = get_rejected_mc_datasets(mc_datasets, reject_mc_function, reject_input)
  return datasets.subtract_mc_datasets(mc_datasets, rejected_mc_datasets)

# reject_data_function should have input as (reject_input, data_datasets, stream, year, run_group, data_tier, path)
# data_dataset[stream][year][run_group][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events":int, "runs":[]}
def filter_data_datasets(data_datasets, reject_data_function, reject_input=None):
  rejected_data_datasets = get_rejected_data_datasets(data_datasets, reject_data_function, reject_input)
  return datasets.subtract_data_datasets(data_datasets, rejected_data_datasets)

def filter_if_possible_mc_datasets(mc_datasets, reject_mc_function, reject_input=None, verbose=False):
  rejected_if_possible_mc_datasets = get_rejected_if_possible_mc_datasets(mc_datasets, reject_mc_function, reject_input, verbose)
  if verbose: print('rejected datasets', rejected_if_possible_mc_datasets)
  return datasets.subtract_mc_datasets(mc_datasets, rejected_if_possible_mc_datasets)

# reject_mc_function should have input as (reject_input, mc_datasets, mc_dataset_name, year, data_tier, path)
def get_unrejected_if_possible_mc_datasets(mc_datasets, reject_mc_function, reject_input=None):
  unrejected_mc_datasets = {}
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        found_mc_dataset = False
        list_rejected_mc_dataset = []
        for path in mc_datasets[mc_dataset_name][year][data_tier]:
          path_info = mc_datasets[mc_dataset_name][year][data_tier][path]
          if reject_mc_function(reject_input, mc_datasets, mc_dataset_name, year, data_tier, path):
            list_rejected_mc_dataset.append([path, path_info])
            continue
          found_mc_dataset = True
        if not found_mc_dataset:
          for rejected_mc_dataset in list_rejected_mc_dataset:
            nested_dict.fill_nested_dict(unrejected_mc_datasets, [mc_dataset_name, year, data_tier, rejected_mc_dataset[0]], rejected_mc_dataset[1])
  return unrejected_mc_datasets

# reject_mc_function should have input as (reject_input, mc_datasets, mc_dataset_name, year, data_tier, path)
def get_rejected_if_possible_mc_datasets(mc_datasets, reject_mc_function, reject_input, verbose=False):
  rejected_mc_datasets = {}
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        found_mc_dataset = False
        list_rejected_mc_dataset = []
        for path in mc_datasets[mc_dataset_name][year][data_tier]:
          path_info = mc_datasets[mc_dataset_name][year][data_tier][path]
          if reject_mc_function(reject_input, mc_datasets, mc_dataset_name, year, data_tier, path):
            if verbose: print('Possibly rejected '+path)
            list_rejected_mc_dataset.append([path, path_info])
            continue
          if verbose: print('Found '+path)
          found_mc_dataset = True
        if found_mc_dataset:
          if verbose: print('Rejected '+path)
          for rejected_mc_dataset in list_rejected_mc_dataset:
            nested_dict.fill_nested_dict(rejected_mc_datasets, [mc_dataset_name, year, data_tier, rejected_mc_dataset[0]], rejected_mc_dataset[1])
  return rejected_mc_datasets

# runs[stream][year][data_tier][run]
def add_all_runs(all_runs, stream, year, data_tier, run):
  if stream not in all_runs:
    all_runs[stream] = {}
  if year not in all_runs[stream]:
    all_runs[stream][year] = {}
  if data_tier not in all_runs[stream][year]:
    all_runs[stream][year][data_tier] = set()
  if run not in all_runs[stream][year][data_tier]:
    all_runs[stream][year][data_tier].add(run)

# runs[stream][year][data_tier][run]
def is_all_runs(all_runs, stream, year, data_tier, run):
  exist = False
  if stream in all_runs:
    if year in all_runs[stream]:
      if data_tier in all_runs[stream][year]:
        if run in all_runs[stream][year][data_tier]:
          exist = True
  return exist

# data_datasets[stream][year][run_group][data_tier] = [path, parent, runs]
def check_common_runs_data_datasets(data_datasets):
  # runs[stream][year][run]
  all_runs = {}
  all_runs['miniaod'] = {}
  all_runs['nanoaod'] = {}
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        for data_tier in data_datasets[stream][year][run_group]:
          for path in data_datasets[stream][year][run_group][data_tier]:
            path_info = data_datasets[stream][year][run_group][data_tier][path]
            runs = path_info['runs']
            for run in runs:
              if not is_all_runs(all_runs, stream, year, data_tier, run):
                add_all_runs(all_runs, stream, year, data_tier, run)
              else:
                print('Run overlaps. stream: '+stream+' year: '+year+' data_tier: '+data_tier+' run: '+run+' in data_datasets['+stream+']['+year+']['+run_group+']['+data_tier+']')

if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Filters datasets_jsons.')
  parser.add_argument('-m', '--meta_folder', metavar='./meta', nargs=1, default=['./meta'])
  parser.add_argument('-d', '--data_tiers', metavar='"nanoaod,miniaod"', nargs=1, default=['nanoaod', 'miniaod'])
  parser.add_argument('-t', '--mc_data', metavar='"mc,data"', nargs=1, default=['mc', 'data'])
  parser.add_argument('-i', '--in_json_folder', metavar='./jsons', nargs=1, default=['./jsons'])
  parser.add_argument('-ip', '--in_json_prefix', metavar='', nargs=1, default=[''])
  parser.add_argument('-o', '--out_json_folder', metavar='./jsons', nargs=1, default=['./jsons'])
  parser.add_argument('-op', '--out_json_prefix', metavar='filtered_', nargs=1, default=['filtered_'])

  args = vars(parser.parse_args())
  argparse_helper.initialize_arguments(args, list_args=['data_tiers','mc_data'])
  valid, log = are_arguments_valid(args)
  if not valid:
    print('[Error] '+log)
    sys.exit()

  make_mc_datasets = False
  if 'mc' in args['mc_data']: make_mc_datasets = True
  make_data_datasets = False
  if 'data' in args['mc_data']: make_data_datasets = True
  meta_folder = args['meta_folder']
  data_tiers = args['data_tiers']

  mc_dataset_common_names_filename = meta_folder+'/mc_dataset_common_names'
  mc_dataset_2016_names_filename = meta_folder+'/mc_dataset_2016_names'
  mc_dataset_2017_names_filename = meta_folder+'/mc_dataset_2017_names'
  mc_dataset_2018_names_filename = meta_folder+'/mc_dataset_2018_names'
  mc_tag_meta_filename = meta_folder+'/mc_tag_meta'
  data_tag_meta_filename = meta_folder+'/data_tag_meta'

  mc_datasets_filename = os.path.join(args['in_json_folder'],args['in_json_prefix']+'mc_datasets.json')
  data_datasets_filename = os.path.join(args['in_json_folder'],args['in_json_prefix']+'data_datasets.json')
  out_filtered_mc_datasets_filename = os.path.join(args['out_json_folder'], args['out_json_prefix']+'mc_datasets.json')
  out_filtered_data_datasets_filename = os.path.join(args['out_json_folder'], args['out_json_prefix']+'data_datasets.json')

  out_bad_pu_mc_datasets_filename = os.path.join(args['out_json_folder'], args['out_json_prefix']+'bad_pu_mc_datasets.json')
  out_ps_weight_mc_datasets_filename = os.path.join(args['out_json_folder'], args['out_json_prefix']+'bad_ps_weight_mc_datasets.json')

  if make_mc_datasets:
    # mc_dataset_names[year] = [(mc_dataset_name, mc_dir)]
    mc_dataset_names = datasets.parse_multiple_mc_dataset_names([
      [mc_dataset_common_names_filename, ['2016', '2017', '2018']],
      [mc_dataset_2016_names_filename, ['2016']],
      [mc_dataset_2017_names_filename, ['2017']],
      [mc_dataset_2018_names_filename, ['2018']],
      ])
    #print ('dataset_names:', mc_dataset_names)
    # Ex) tag_meta[2016] = RunIISummer16, MiniAODv3, NanoAODv5
    mc_tag_meta = datasets.parse_mc_tag_meta(mc_tag_meta_filename)

  if make_data_datasets:
    # Ex) data_tag_meta[2016][B][MET][miniaod] = 17Jul2018
    data_tag_meta = datasets.parse_data_tag_meta(data_tag_meta_filename)

  if make_mc_datasets:
    # mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
    mc_datasets = datasets.load_json_file(mc_datasets_filename)
    datasets.check_overlapping_paths_mc_datasets(mc_datasets)

    #print(nested_dict.get_from_nested_dict(mc_datasets, '/DYJetsToLL_M-50_HT-70to100_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/MINIAODSIM'))
    #print(nested_dict.get_nested_dict(mc_datasets, ['ZJetsToNuNu_HT-600To800', '2017', 'miniaod']))

    # keys_mc_datasets = [ [mc_dataset_name, year, data_tier, search_string] ]
    keys_mc_datasets = datasets.get_keys_mc_datasets(mc_dataset_names, mc_tag_meta, data_tiers)
    #datasets.print_missing_mc_datasets(keys_mc_datasets, mc_datasets)
    ##nested_dict.fill_empty_nested_dict(mc_datasets, ['TTJets_SingleLeptFromT_Tune', '2016', 'miniaod'])
    ##datasets.check_false_none_mc_datasets(mc_datasets)
    #datasets.print_same_parent_mc_datasets(mc_datasets)
    #datasets.check_mini_nano_consistentcy_mc_datasets(mc_tag_meta, mc_datasets)

    filtered_mc_datasets = mc_datasets
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, '_mtop1')
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'TuneCP5Down')
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'TuneCP5Up')
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'CUETP8M1Up')
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'CUETP8M1Down')
    # Reject signal tune
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'TuneCP2')
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'DoubleScattering')
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, '14TeV')
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'FlatPU')
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'BS2016')
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'percentMaterial')
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'herwigpp')
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'CUETP8M2')
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'DownPS')
    filtered_mc_datasets = filter_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'UpPS')
    #datasets.print_path_mc_datasets(datasets.subtract_mc_datasets(mc_datasets, filtered_mc_datasets))
    #datasets.print_missing_mc_datasets(keys_mc_datasets, mc_datasets)

    filtered_mc_datasets = filter_if_possible_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'PSweights')
    bad_ps_weights_mc_datasets = get_unrejected_if_possible_mc_datasets(filtered_mc_datasets, reject_string_ignore_case_mc_datasets, 'PSweights')
    print('Using ps_weights for below, because no other datasets')
    datasets.print_path_mc_datasets(bad_ps_weights_mc_datasets)

    pu_filtered_mc_datasets = filter_if_possible_mc_datasets(filtered_mc_datasets, datasets.reject_bad_pu_2017_mc_datasets)
    #datasets.print_path_mc_datasets(pu_filtered_mc_datasets)
    #datasets.print_multiple_mc_datasets(pu_filtered_mc_datasets)
    #datasets.print_incomplete_parent_mc_datasets(pu_filtered_mc_datasets)
    datasets.print_missing_mc_datasets(keys_mc_datasets, pu_filtered_mc_datasets)

    bad_pu_mc_datasets = get_unrejected_if_possible_mc_datasets(filtered_mc_datasets, datasets.reject_bad_pu_2017_mc_datasets)
    #datasets.print_path_parent_mc_datasets(bad_pu_mc_datasets)
    print('Using bad pileup for below, because no other datasets')
    datasets.print_path_mc_datasets(bad_pu_mc_datasets)

    datasets.save_json_file(pu_filtered_mc_datasets, out_filtered_mc_datasets_filename)
    datasets.save_json_file(bad_pu_mc_datasets, out_bad_pu_mc_datasets_filename)
    datasets.save_json_file(bad_ps_weights_mc_datasets, out_ps_weight_mc_datasets_filename)

  if make_data_datasets:
    # data_dataset[stream][year][run_group][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events":int, "runs":[]}
    data_datasets = datasets.load_json_file(data_datasets_filename)
    datasets.check_overlapping_paths_data_datasets(data_datasets)

    #print_multiple_data_datasets(data_datasets)
    keys_data_datasets = datasets.get_keys_data_datasets(data_tag_meta, data_tiers)
    #nested_dict.remove_key_nested_dict(data_datasets, '/SingleElectron/Run2017C-31Mar2018-v1/MINIAOD')
    datasets.print_missing_data_datasets(keys_data_datasets, data_datasets)

    filtered_data_datasets = filter_data_datasets(data_datasets, reject_string_ignore_case_path_parent_data_datasets, 'pilot')
    #datasets.print_path_data_datasets(filtered_data_datasets)
    check_common_runs_data_datasets(filtered_data_datasets)

    datasets.save_json_file(filtered_data_datasets, out_filtered_data_datasets_filename)
