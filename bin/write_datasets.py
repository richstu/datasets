#!/usr/bin/env python
import datasets
import nested_dict
import filter_datasets_jsons
import argparse
import os
import sys
import argparse_helper
import ask

def is_sig(args):
  sig = False
  for data_type in args['mc_data_sig']:
    if data_type == 'mc': continue
    if data_type == 'data': continue
    sig = True
  return sig

def are_arguments_valid(args):
  # Check for data_tiers
  if not argparse_helper.is_valid(args, 'data_tiers', ['nanoaod', 'miniaod']):
    return False, 'data_tier: '+str(args['data_tiers'])+' is not valid.'

  # Check for years
  if not argparse_helper.is_valid(args, 'years', ['2016', '2016APV', '2017', '2018', '2022', '2022EE']):
    return False, 'years: '+str(args['years'])+' is not valid.'
  
  # Will not check for mc_data_sig. Can't know what nanoaod_tag will be.
  ## Check for mc_data
  #if not argparse_helper.is_valid(args, 'mc_data', ['mc', 'data']):
  #  return False, 'mc_data: '+str(args['mc_data'])+' is not valid.'

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

  if 'mc' in args['mc_data_sig'] or is_sig(args) or args['mc_data_sig'][0] == '':
    t_path = os.path.join(args['meta_folder'],'mc_tag_meta')
    if not os.path.isfile(os.path.join(t_path)):
      return False, 'meta_mc_tag_meta: '+t_path+" doesn't exist."

  if 'data' in args['mc_data_sig'] or args['mc_data_sig'][0] == '':
    t_path = os.path.join(args['meta_folder'],'data_tag_meta')
    if not os.path.isfile(os.path.join(t_path)):
      return False, 'meta_data_tag_meta: '+t_path+" doesn't exist."
 
  # Check if output folder exits
  if not os.path.isdir(args['out_results_folder']):
    return False, 'out_results_folder: '+args['out_results_folder']+" doesn't exist."

  # Check if files exists with in_json_prefix
  if 'mc' in args['mc_data_sig'] or is_sig(args) or args['mc_data_sig'][0] == '':
    t_path = os.path.join(args['in_json_folder'], args['in_json_prefix']+'mc_datasets.json')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

    #t_path = os.path.join(args['in_json_folder'], args['in_json_prefix']+'bad_pu_mc_datasets.json')
    #if not os.path.isfile(t_path):
    #  return False, t_path+' does not exists.'

    #t_path = os.path.join(args['in_json_folder'], args['in_json_prefix']+'bad_ps_weight_mc_datasets.json')
    #if not os.path.isfile(t_path):
    #  return False, t_path+' does not exists.'

  if 'data' in args['mc_data_sig'] or args['mc_data_sig'][0] == '':
    t_path = os.path.join(args['in_json_folder'], args['in_json_prefix']+'data_datasets.json')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

  # Check if files exists with out_results_prefix 
  if 'mc' in args['mc_data_sig'] or is_sig(args) or args['mc_data_sig'][0] == '':
    t_path = os.path.join(args['out_results_folder'], args['out_results_prefix']+'mc_dataset_paths')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

    t_path = os.path.join(args['out_results_folder'], args['out_results_prefix']+'mc_dataset_empties')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

    t_path = os.path.join(args['out_results_folder'], args['out_results_prefix']+'bad_pu_mc_dataset_paths')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

    t_path = os.path.join(args['out_results_folder'], args['out_results_prefix']+'bad_ps_weight_mc_dataset_paths')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

  if 'data' in args['mc_data_sig'] or args['mc_data_sig'][0] == '':
    t_path = os.path.join(args['out_results_folder'], args['out_results_prefix']+'data_dataset_paths')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

    t_path = os.path.join(args['out_results_folder'], args['out_results_prefix']+'data_dataset_empties')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

  return True, ''

## multiple_selection[search_string]= {'paths':[paths], 'selected_paths':[path_selection], 'reason':reason}
#def select_paths_from_multiple(mc_tag_meta, mc_datasets, multiple_selection):
#  mc_datasets_selected = {}
#  mc_datasets_non_selected = {}
#  for mc_dataset_name in mc_datasets:
#    for year in mc_datasets[mc_dataset_name]:
#      for data_tier in mc_datasets[mc_dataset_name][year]:
#        search_string = datasets.get_mc_dataset_search_string(mc_tag_meta, mc_dataset_name, year, data_tier)
#        multiple_selected_paths = None
#        if search_string in multiple_selection:
#          multiple_selected_paths = multiple_selection[search_string]['selected_paths']
#        if len(mc_datasets[mc_dataset_name][year][data_tier]) == 1: 
#          path = next(iter(mc_datasets[mc_dataset_name][year][data_tier]))
#          nested_dict.fill_nested_dict(mc_datasets_selected, [mc_dataset_name, year, data_tier, path], mc_datasets[mc_dataset_name][year][data_tier][path])
#        else:
#          for path in mc_datasets[mc_dataset_name][year][data_tier]:
#            if multiple_selected_paths != None:
#              if path in multiple_selected_paths:
#                nested_dict.fill_nested_dict(mc_datasets_selected, [mc_dataset_name, year, data_tier, path], mc_datasets[mc_dataset_name][year][data_tier][path])
#            else:
#              print('[Warning] '+path+' is not in multiple_selection. Will not select any dataset.')
#              nested_dict.fill_nested_dict(mc_datasets_non_selected, [mc_dataset_name, year, data_tier, path], mc_datasets[mc_dataset_name][year][data_tier][path])
#  return mc_datasets_selected, mc_datasets_non_selected

def write_list(target_list, out_filename):
  with open(out_filename,'w') as out_file:
    for item in target_list:
      out_file.write(item+'\n')

def write_string(string, out_filename):
  print('Writing to '+out_filename)
  with open(out_filename, 'w') as out_file:
    out_file.write(string)

def write_path_each_key_mc_datasets(mc_tag_meta, mc_datasets, out_path_each_key_mc_datasets_filename):
  path_each_key_mc_datasets_string = datasets.get_path_each_key_mc_datasets_string(mc_tag_meta, mc_datasets)
  write_string(path_each_key_mc_datasets_string, out_path_each_key_mc_datasets_filename)

def write_path_each_key_data_datasets(data_tag_meta, data_datasets, out_path_each_key_data_datasets_filename):
  path_each_key_data_datasets_string = datasets.get_path_each_key_data_datasets_string(data_tag_meta, data_datasets)
  write_string(path_each_key_data_datasets_string, out_path_each_key_data_datasets_filename)

def write_path_mc_datasets(mc_datasets, out_path_mc_datasets_filename):
  path_mc_datasets_string = datasets.get_mc_datasets_string(mc_datasets, datasets.get_path_mc_datasets_string)
  write_string(path_mc_datasets_string, out_path_mc_datasets_filename)

def write_path_data_datasets(data_datasets, out_path_data_datasets_filename):
  path_data_datasets_string = datasets.get_data_datasets_string(data_datasets, datasets.get_path_data_datasets_string)
  write_string(path_data_datasets_string, out_path_data_datasets_filename)

def write_path_mc_datasets_with_data_tier(mc_datasets, data_tier, out_path_mc_datasets_filename):
  path_mc_datasets_with_data_tier_string = datasets.get_mc_datasets_with_data_tier_string(mc_datasets, datasets.get_path_mc_datasets_string, data_tier)
  write_string(path_mc_datasets_with_data_tier_string, out_path_mc_datasets_filename)

def write_path_data_datasets_with_data_tier(data_datasets, data_tier, out_path_data_datasets_filename):
  path_data_datasets_with_data_tier_string = datasets.get_data_datasets_with_data_tier_string(data_datasets, datasets.get_path_data_datasets_string, data_tier)
  write_string(path_data_datasets_with_data_tier_string, out_path_data_datasets_filename)

def write_missing_mc_datasets(keys_mc_dataset, mc_datasets, out_empty_mc_datasets_filename):
  missing_mc_datasets = datasets.get_missing_mc_datasets(keys_mc_datasets, mc_datasets)
  missing_mc_datasets_string = datasets.get_mc_datasets_string(missing_mc_datasets, datasets.get_missing_mc_datasets_string)
  write_string(missing_mc_datasets_string, out_empty_mc_datasets_filename)

def write_missing_data_datasets(keys_data_dataset, data_datasets, out_empty_data_datasets_filename):
  missing_data_datasets = datasets.get_missing_data_datasets(keys_data_datasets, data_datasets)
  missing_data_datasets_string = datasets.get_data_datasets_string(missing_data_datasets, datasets.get_missing_data_datasets_string)
  write_string(missing_data_datasets_string, out_empty_data_datasets_filename)

def write_missing_mc_datasets_with_data_tier(keys_mc_dataset, mc_datasets, data_tier, out_empty_mc_datasets_filename):
  missing_mc_datasets = datasets.get_missing_mc_datasets_with_data_tier(keys_mc_datasets, mc_datasets, data_tier)
  missing_mc_datasets_string = datasets.get_mc_datasets_string(missing_mc_datasets, datasets.get_missing_mc_datasets_string)
  write_string(missing_mc_datasets_string, out_empty_mc_datasets_filename)

def write_missing_data_datasets_with_data_tier(keys_data_dataset, data_datasets, data_tier, out_empty_data_datasets_filename):
  missing_data_datasets = datasets.get_missing_data_datasets_with_data_tier(keys_data_datasets, data_datasets, data_tier)
  missing_data_datasets_string = datasets.get_data_datasets_string(missing_data_datasets, datasets.get_missing_data_datasets_string)
  write_string(missing_data_datasets_string, out_empty_data_datasets_filename)

def write_multiple_mc_datasets(mc_datasets, multiple_mc_datasets_filename):
  multiple_mc_datase_string = datasets.get_mc_datasets_string(mc_datasets, datasets.get_multiple_mc_datasets_string)
  write_string(multiple_mc_datase_string, multiple_mc_datasets_filename)

def write_multiple_data_datasets(data_datasets, multiple_data_datasets_filename):
  multiple_data_datase_string = datasets.get_data_datasets_string(data_datasets, datasets.get_multiple_data_datasets_string)
  write_string(multiple_data_datase_string, multiple_data_datasets_filename)

def write_same_parent_mc_datasets(mc_datasets, same_parent_mc_datasets_filename):
  same_parent_mc_datasets_string = datasets.get_same_parent_mc_datasets_string(mc_datasets)
  write_string(same_parent_mc_datasets_string, same_parent_mc_datasets_filename)

# no_nano_for_mini_mc_dataset 
# Find case where mini doesn't have nano
def get_no_nano_in_mini_mc_datasets(mc_tag_meta, full_mc_datasets):
  no_nano_in_mini_mc_datasets = {}
  mini_to_nano_from_nano = {}
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      if 'nanoaod' not in mc_datasets[mc_dataset_name][year]: continue
      for path in mc_datasets[mc_dataset_name][year]['nanoaod']:
        parent = mc_datasets[mc_dataset_name][year]['nanoaod'][path]['parent_chain'][0]
        mini_to_nano_from_nano[parent] = path
  # Check if nano is in mini
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      if 'miniaod' not in mc_datasets[mc_dataset_name][year]: continue
      for path in mc_datasets[mc_dataset_name][year]['miniaod']:
        children = mc_datasets[mc_dataset_name][year]['miniaod'][path]['children']
        if path not in mini_to_nano_from_nano:
          found_nano = False
          for nano in children:
            if mc_tag_meta[year][2] in nano:
              found_nano = True
              break
          if not found_nano:
            nested_dict.fill_nested_dict(no_nano_in_mini_mc_datasets, [mc_dataset_name, year, 'miniaod', path], mc_datasets[mc_dataset_name][year]['miniaod'][path]['children'])
  return no_nano_in_mini_mc_datasets

def check_if_other_mc_datasets(mc_tag_meta, problematic_mc_datasets, full_mc_datasets):
  for mc_dataset_name in problematic_mc_datasets:
    for year in problematic_mc_datasets[mc_dataset_name]:
      for data_tier in problematic_mc_datasets[mc_dataset_name][year]:
        problematic_mc_dataset_list = []
        for path in problematic_mc_datasets[mc_dataset_name][year][data_tier]:
          problematic_mc_dataset_list.append(path)
        found_other_mc_dataset = False
        for full_path in full_mc_datasets[mc_dataset_name][year][data_tier]:
          if full_path not in problematic_mc_dataset_list: found_other_mc_dataset = True
        if not found_other_mc_dataset:
          print(datasets.get_mc_dataset_search_string(mc_tag_meta, mc_dataset_name, year, data_tier))
          print('  mc_dataset_name: '+mc_dataset_name+' year: '+year+' data_tier: '+data_tier+' '+path+" doesn't have alternative.")

if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Writes datasets_jsons.')
  parser.add_argument('-m', '--meta_folder', metavar='./meta', nargs=1, default=['./meta'])
  parser.add_argument('-d', '--data_tiers', metavar='"nanoaod"', nargs=1, default=['nanoaod'])
  parser.add_argument('-t', '--mc_data_sig', metavar='""', nargs=1, default=[''])
  parser.add_argument('-y', '--years', metavar='"2016,2016APV,2017,2018,2022,2022EE"', nargs=1, default=['2016,2016APV,2017,2018,2022,2022EE'])
  parser.add_argument('-i', '--in_json_folder', metavar='./jsons', nargs=1, default=['./jsons'])
  parser.add_argument('-ip', '--in_json_prefix', metavar='selected_', nargs=1, default=['selected_'])
  #parser.add_argument('-is', '--in_mc_multiple_selection_json', metavar='mc_multiple_selection.json', nargs=1, default=['mc_multiple_selection.json'])
  parser.add_argument('-o', '--out_results_folder', metavar='./results', nargs=1, default=['./results'])
  parser.add_argument('-op', '--out_results_prefix', metavar="''", nargs=1, default=[''])

  args = vars(parser.parse_args())
  argparse_helper.initialize_arguments(args, list_args=['data_tiers','mc_data_sig', 'years'])
  valid, log = are_arguments_valid(args)
  if not valid:
    print('[Error] '+log)
    sys.exit()

  make_mc_datasets = False
  if 'mc' in args['mc_data_sig'] or is_sig(args) or args['mc_data_sig'][0] == '': make_mc_datasets = True
  make_data_datasets = False
  if 'data' in args['mc_data_sig'] or args['mc_data_sig'][0] == '': make_data_datasets = True
  meta_folder = args['meta_folder']
  data_tiers = args['data_tiers']

  mc_dataset_common_names_filename = meta_folder+'/mc_dataset_common_names'
  mc_dataset_2016_names_filename = meta_folder+'/mc_dataset_2016_names'
  mc_dataset_2016APV_names_filename = meta_folder+'/mc_dataset_2016APV_names'
  mc_dataset_2017_names_filename = meta_folder+'/mc_dataset_2017_names'
  mc_dataset_2018_names_filename = meta_folder+'/mc_dataset_2018_names'
  mc_dataset_2022_names_filename = meta_folder+'/mc_dataset_2022_names'
  mc_dataset_2022EE_names_filename = meta_folder+'/mc_dataset_2022EE_names'
  mc_tag_meta_filename = meta_folder+'/mc_tag_meta'
  data_tag_meta_filename = meta_folder+'/data_tag_meta'

  #mc_multiple_selection_filename = os.path.join(args['in_json_folder'], args['in_mc_multiple_selection_json'])

  mc_datasets_filename = os.path.join(args['in_json_folder'],args['in_json_prefix']+'mc_datasets.json')
  #bad_pu_mc_datasets_filename = os.path.join(args['in_json_folder'], args['in_json_prefix']+'bad_pu_mc_datasets.json')
  #ps_weight_mc_datasets_filename = os.path.join(args['in_json_folder'], args['in_json_prefix']+'bad_ps_weight_datasets.json')

  data_datasets_filename = os.path.join(args['in_json_folder'],args['in_json_prefix']+'data_datasets.json')

  out_folder = args['out_results_folder']
  out_path_mc_datasets_filename = os.path.join(args['out_results_folder'], args['out_results_prefix']+'mc_dataset_paths')
  out_empty_mc_datasets_filename = os.path.join(args['out_results_folder'], args['out_results_prefix']+'mc_dataset_empties')
  out_path_bad_pu_mc_datasets_filename = os.path.join(args['out_results_folder'], args['out_results_prefix']+'bad_pu_mc_dataset_paths')
  out_path_ps_weight_mc_datasets_filename = out_folder+'/'+args['out_results_prefix']+'bad_ps_weight_mc_dataset_paths'
  #out_no_nano_for_mini_filename = out_folder+'/no_nano_for_mini_mc_dataset'
  #out_path_each_key_mc_datasets_filename = out_folder+'/mc_dataset_path_each_key'
  #out_same_parent_mc_datasets_filename = out_folder+'/same_parent_mc_dataset'


  out_path_data_datasets_filename = os.path.join(args['out_results_folder'], args['out_results_prefix']+'data_dataset_paths')
  out_empty_data_datasets_filename = os.path.join(args['out_results_folder'], args['out_results_prefix']+'data_dataset_empties')
  #out_path_each_key_data_datasets_filename = out_folder+'/data_dataset_path_each_key'

  if make_mc_datasets:
    # mc_dataset_names[year] = [(mc_dataset_name, mc_dir)]
    mc_dataset_names = datasets.parse_multiple_mc_dataset_names([
      [mc_dataset_common_names_filename, ['2016', '2016APV', '2017', '2018']],
      [mc_dataset_2016_names_filename, ['2016']],
      [mc_dataset_2016APV_names_filename, ['2016APV']],
      [mc_dataset_2017_names_filename, ['2017']],
      [mc_dataset_2018_names_filename, ['2018']],
      [mc_dataset_2022_names_filename, ['2022']],
      [mc_dataset_2022EE_names_filename, ['2022EE']],
      ])
    #print ('dataset_names:', mc_dataset_names)
    # Ex) tag_meta[2016] = RunIISummer16, MiniAODv3, NanoAODv5
    mc_tag_meta = datasets.parse_mc_tag_meta(mc_tag_meta_filename)

  if make_data_datasets:
    # Ex) data_tag_meta[2016][B][MET] = 17Jul2018
    data_tag_meta = datasets.parse_data_tag_meta(data_tag_meta_filename)
    # keys_data_datasets = [ [stream, year, run_group, data_tier, search_string] ]
    keys_data_datasets = datasets.get_keys_data_datasets(data_tag_meta, data_tiers)

  if make_mc_datasets:
    # Make meta
    # keys_mc_datasets = [ [mc_dataset_name, year, data_tier, search_string] ]
    keys_mc_datasets = datasets.get_keys_mc_datasets(mc_dataset_names, mc_tag_meta, data_tiers)

    # mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
    mc_datasets_selected = nested_dict.load_json_file(mc_datasets_filename)
    ## Do simple checks
    datasets.check_false_none_mc_datasets(mc_datasets_selected)
    datasets.print_same_parent_mc_datasets(mc_datasets_selected)
    datasets.print_missing_mc_datasets(keys_mc_datasets, mc_datasets_selected)

    # Filter mc_datasets according to years
    mc_datasets_year = {}
    for mc_dataset in mc_datasets_selected:
      for year in mc_datasets_selected[mc_dataset]:
        if year in args['years']:
          for data_tier in mc_datasets_selected[mc_dataset][year]:
            for path in mc_datasets_selected[mc_dataset][year][data_tier]:
              path_info = mc_datasets_selected[mc_dataset][year][data_tier][path]
              nested_dict.fill_nested_dict(mc_datasets_year, [mc_dataset, year, data_tier, path], path_info)
    mc_datasets_selected = mc_datasets_year
    # keys_mc_datasets = [ [mc_dataset_name, year, data_tier, search_string] ]
    keys_mc_datasets_year = []
    for keys_mc  in keys_mc_datasets:
      year = keys_mc[1]
      if year in args['years']:
        keys_mc_datasets_year.append(keys_mc)
    keys_mc_datasets = keys_mc_datasets_year


    # Filter mc_datasets according to mc_dir
    if args['mc_data_sig'][0] != '':
      # mc_dataset_names_mc_dir[year][mc_dir][mc_dataset_name] = None
      mc_dataset_names_mc_dir = {}
      for year in mc_dataset_names:
        for mc_dataset_names_info in mc_dataset_names[year]:
          mc_dataset_name = mc_dataset_names_info[0]
          mc_dir = mc_dataset_names_info[1]
          nested_dict.fill_nested_dict(mc_dataset_names_mc_dir, [year, mc_dir, mc_dataset_name], None)
        
      mc_datasets_mc_dir = {}
      for mc_dataset in mc_datasets_selected:
        for year in mc_datasets_selected[mc_dataset]:
          for mc_dir in args['mc_data_sig']:
            if mc_dir not in mc_dataset_names_mc_dir[year]: continue
            if mc_dataset in mc_dataset_names_mc_dir[year][mc_dir]:
              for data_tier in mc_datasets_selected[mc_dataset][year]:
                for path in mc_datasets_selected[mc_dataset][year][data_tier]:
                  path_info = mc_datasets_selected[mc_dataset][year][data_tier][path]
                  nested_dict.fill_nested_dict(mc_datasets_mc_dir, [mc_dataset, year, data_tier, path], path_info)

      # keys_mc_datasets = [ [mc_dataset_name, year, data_tier, search_string] ]
      keys_mc_datasets_mc_dir = []
      for keys_mc  in keys_mc_datasets:
        mc_dataset_name = keys_mc[0]
        year = keys_mc[1]
        for mc_dir in args['mc_data_sig']:
          if mc_dir not in mc_dataset_names_mc_dir[year]: continue
          if mc_dataset_name in mc_dataset_names_mc_dir[year][mc_dir]:
            keys_mc_datasets_mc_dir.append(keys_mc)
      keys_mc_datasets = keys_mc_datasets_mc_dir

      mc_datasets_selected = mc_datasets_mc_dir


    #if os.path.isfile(mc_multiple_selection_filename):
    #  print('[Info] Using '+mc_multiple_selection_filename+' to select files.')
    #  # multiple_selection[search_string]= {'paths':[paths], 'selected_paths':[path_selection], 'reason':reason}
    #  multiple_selection = nested_dict.load_json_file(mc_multiple_selection_filename)
    #  # select paths according to multiple selection
    #  mc_datasets_selected, mc_datasets_non_selected = select_paths_from_multiple(mc_tag_meta, mc_datasets, multiple_selection)
    #else:
    #  print('[Info] No multiple selection file.')
    #  mc_datasets_selected = mc_datasets 

    ## Do simple checks
    #datasets.print_same_parent_mc_datasets(mc_datasets_selected)
    #datasets.print_missing_mc_datasets(keys_mc_datasets, mc_datasets_selected)

    # Write to file
    if len(args['data_tiers']) == 2:
      write_path_mc_datasets(mc_datasets_selected, out_path_mc_datasets_filename)
      write_missing_mc_datasets(keys_mc_datasets, mc_datasets_selected, out_empty_mc_datasets_filename)
      bad_pu_mc_datasets = filter_datasets_jsons.get_unrejected_if_possible_mc_datasets(mc_datasets_selected, filter_datasets_jsons.reject_bad_pu_2017_mc_datasets)
      write_path_mc_datasets(bad_pu_mc_datasets, out_path_bad_pu_mc_datasets_filename)
      bad_ps_weights_mc_datasets = filter_datasets_jsons.get_unrejected_if_possible_mc_datasets(mc_datasets_selected, filter_datasets_jsons.reject_string_ignore_case_mc_datasets, 'PSweights')
      write_path_mc_datasets(bad_ps_weights_mc_datasets, out_path_ps_weight_mc_datasets_filename)
    else:
      write_path_mc_datasets_with_data_tier(mc_datasets_selected, args['data_tiers'][0], out_path_mc_datasets_filename)
      # Write extra files
      write_missing_mc_datasets_with_data_tier(keys_mc_datasets, mc_datasets_selected,  args['data_tiers'][0], out_empty_mc_datasets_filename)
      # Write bad pu
      bad_pu_mc_datasets = filter_datasets_jsons.get_unrejected_if_possible_mc_datasets(mc_datasets_selected, filter_datasets_jsons.reject_bad_pu_2017_mc_datasets)
      write_path_mc_datasets_with_data_tier(bad_pu_mc_datasets, args['data_tiers'][0], out_path_bad_pu_mc_datasets_filename)
      # Write bad ps_weight
      bad_ps_weights_mc_datasets = filter_datasets_jsons.get_unrejected_if_possible_mc_datasets(mc_datasets_selected, filter_datasets_jsons.reject_string_ignore_case_mc_datasets, 'PSweights')
      write_path_mc_datasets_with_data_tier(bad_ps_weights_mc_datasets, args['data_tiers'][0], out_path_ps_weight_mc_datasets_filename)

  if make_data_datasets:
    # data_datasets[stream][year][run][data_tier] = [path, parent, runs]
    data_datasets = nested_dict.load_json_file(data_datasets_filename)
    datasets.check_false_none_data_datasets(data_datasets)

    datasets.print_missing_data_datasets(keys_data_datasets, data_datasets)

    # Filter data_datasets according to years
    data_datasets_year = {}
    for stream in data_datasets:
      for year in data_datasets[stream]:
        if year in args['years']:
          for run_group in data_datasets[stream][year]:
            for data_tier in data_datasets[stream][year][run_group]:
              for path in data_datasets[stream][year][run_group][data_tier]:
                path_info = data_datasets[stream][year][run_group][data_tier][path]
                nested_dict.fill_nested_dict(data_datasets_year, [stream, year, run_group,  data_tier, path], path_info)
    data_datasets = data_datasets_year

    # keys_data_datasets = [ [stream, year, run_group, data_tier, search_string] ]
    keys_data_datasets_year = []
    for keys_data in keys_data_datasets:
      year = keys_data[1]
      if year in args['years']:
        keys_data_datasets_year.append(keys_data)
    keys_data_datasets = keys_data_datasets_year

    if len(args['data_tiers']) == 2:
      write_path_data_datasets(data_datasets, out_path_data_datasets_filename)
      write_missing_data_datasets(keys_data_datasets, data_datasets, out_empty_data_datasets_filename)
    else:
      # Write to file
      write_path_data_datasets_with_data_tier(data_datasets,args['data_tiers'][0], out_path_data_datasets_filename)

      # Write extra files
      write_missing_data_datasets_with_data_tier(keys_data_datasets, data_datasets, args['data_tiers'][0], out_empty_data_datasets_filename)


  # ETC
  #nested_dict.remove_key_nested_dict(data_datasets, '/SingleElectron/Run2017C-31Mar2018-v1/MINIAOD')

  #bad_pu_mc_datasets = nested_dict.load_json_file(bad_pu_mc_datasets_filename)
  #ps_weights_mc_datasets = nested_dict.load_json_file(ps_weight_mc_datasets_filename)

  ##full_mc_datasets = datasets.add_mc_datasets(mc_datasets, bad_pu_mc_datasets)
  ##datasets.print_same_parent_mc_datasets(mc_datasets)
  ### Find case where mini doesn't have nano
  #no_nano_in_mini_mc_datasets = get_no_nano_in_mini_mc_datasets(mc_tag_meta, mc_datasets)
  ##datasets.print_path_mc_datasets(no_nano_in_mini_mc_datasets)
  ##check_if_other_mc_datasets(mc_tag_meta, no_nano_in_mini_mc_datasets, mc_datasets)
  ##datasets.check_mini_nano_consistentcy_mc_datasets(mc_tag_meta, full_mc_datasets, datasets.get_path_list_mc_datasets(no_nano_in_mini_mc_datasets))
  ##datasets.check_mini_nano_consistentcy_mc_datasets(mc_tag_meta, mc_datasets)
  #write_list(datasets.get_path_list_mc_datasets(no_nano_in_mini_mc_datasets), out_no_nano_for_mini_filename)

  #write_multiple_mc_datasets(mc_datasets, out_multiple_mc_datasets_filename)
  #write_path_mc_datasets(mc_datasets, out_path_mc_datasets_filename)

  #keys_mc_datasets = datasets.get_keys_mc_datasets(mc_dataset_names, mc_tag_meta, data_tiers)
  #write_missing_mc_datasets(keys_mc_datasets, mc_datasets, out_empty_mc_datasets_filename)

  #write_path_mc_datasets(bad_pu_mc_datasets, out_path_bad_pu_mc_datasets_filename)
  #write_path_mc_datasets(ps_weights_mc_datasets, out_path_ps_weight_mc_datasets_filename)

  #write_path_each_key_mc_datasets(mc_tag_meta, mc_datasets, out_path_each_key_mc_datasets_filename)

  #write_same_parent_mc_datasets(mc_datasets, out_same_parent_mc_datasets_filename)

  #write_multiple_data_datasets(data_datasets, out_multiple_data_datasets_filename)
  #write_path_data_datasets(data_datasets, out_path_data_datasets_filename)

  #write_path_each_key_data_datasets(data_tag_meta, data_datasets, out_path_each_key_data_datasets_filename)
