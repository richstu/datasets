#!/usr/bin/env python
import datasets
import filter_datasets_jsons
import nested_dict
import difflib
import os
import readline
import termcolor
import argparse
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
    t_path = os.path.join(args['out_json_folder'], args['out_json_prefix']+'mc_multiple_selection.json')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

    t_path = os.path.join(args['out_json_folder'], args['out_json_prefix']+'mc_datasets.json')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

    t_path = os.path.join(args['out_json_folder'], args['out_json_prefix']+'data_datasets.json')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

  return True, ''


def count_number_selections_left(multiple_selection, multiple_selection_from_file):
  number_selections_left = 0
  for search_string in multiple_selection:
    if search_string in multiple_selection_from_file: continue
    number_selections_left += 1
  return number_selections_left
  

# multiple_selection[search_string]= {'paths':[paths], 'selected_paths':[path_selection], 'reason':reason}
def auto_select_extension(multiple_selection, multiple_selection_from_file, multiple_selection_filename):
  for search_string in multiple_selection:

    info = multiple_selection[search_string]
    if len(info['paths']) !=  2: continue
    if len(info['paths'][0]) > len(info['paths'][1]):
      shorter_path = info['paths'][1]
      longer_path = info['paths'][0]
    else:
      shorter_path = info['paths'][0]
      longer_path = info['paths'][1]

    add_string = ''
    remove_string = ''
    for index, key in enumerate(difflib.ndiff(shorter_path, longer_path)):
      if key[0] == '  ': continue
      elif key[0]=='-':
          #print(u'Delete "{}" from position {}'.format(key[-1],index))
          remove_string += key[-1]
      elif key[0]=='+':
          #print(u'Add "{}" to position {}'.format(key[-1],index))
          add_string += key[-1]

    # Found extension
    if remove_string == '' and add_string == '_ext1':
      if search_string in multiple_selection_from_file:
        print('[Info] '+multiple_selection_filename+' already has '+search_string)
        continue

      common_name = get_common_name(info['paths'])
      print('Auto-selecting below paths with COMMON_PATH: '+common_name)
      print('  '+shorter_path.replace(common_name, '|COMMON_PATH|'))
      print('  '+longer_path.replace(common_name, '|COMMON_PATH|'))
      #multiple_selection[search_string] = {'paths': info['paths'], 'selected_paths': info['paths'], 'reason': 'Auto-selected for extension file.'}
      update_selection(search_string, multiple_selection[search_string]['paths'], [0,1], 'Auto-selected for extension file.', multiple_selection_from_file)
      nested_dict.save_json_file(multiple_selection_from_file, multiple_selection_filename)

def get_info_path_string_mc_datasets(path, mc_datasets, prefix):
  out_info_path = ''
  # Make meta 
  path_to_keys_mc_datasets = datasets.get_path_to_keys_mc_datasets(mc_datasets)
  mini_to_nanos_from_nanoaod = datasets.get_mini_to_nanos_from_nanoaod_mc_datasets(mc_datasets)
  nano_to_mini_from_miniaod = datasets.get_nano_to_mini_from_miniaod_mc_datasets(mc_datasets)

  keys = path_to_keys_mc_datasets[path]
  keys.append(path)
  path_info = nested_dict.get_item_nested_dict(mc_datasets, keys)
  bad_pu = filter_datasets_jsons.reject_bad_pu_2017_mc_datasets('', mc_datasets, keys[0], keys[1], keys[2], keys[3])

  # Print extra info
  if bad_pu:
    out_info_path += '  BAD PU\n'
  same_parent_paths = datasets.get_same_parent_paths(mc_datasets)
  if has_same_parent(same_parent_paths, path):
    out_info_path += '  Has same parent\n'
  if 'miniaod' in keys:
    if path in mini_to_nanos_from_nanoaod:
      out_info_path += prefix+'  Matching nanoaod: '+get_list_string(mini_to_nanos_from_nanoaod[path])+'\n'
    else:
      out_info_path += prefix+'  No matching nanoaod.\n'
  if 'nanoaod' in keys:
    if path in nano_to_mini_from_miniaod:
      out_info_path += prefix+'  Matching miniaod: '+nano_to_mini_from_miniaod[path]+'\n'
    else:
      out_info_path += prefix+'  No matching miniaod.'

  out_info_path += prefix + '  creation_time: '+str(datasets.convert_timestamp(path_info['creation_time']))+'\n'
  out_info_path += prefix + '  parent_chain:\n'
  parent_prefix = '    '
  for parent in path_info['parent_chain']:
    out_info_path += prefix + parent_prefix + parent +'\n'
    parent_prefix += '  '
  #if len(path_info['children']) != 0:
  #  out_info_path += prefix + '  children:\n'
  #  for child in path_info['children']:
  #    out_info_path += prefix + '  ' + child+'\n'
  #out_info_path = out_info_path.rstrip()
  return out_info_path

# mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
def print_mc_datasets_simple(mc_datasets, keys):
  if nested_dict.is_nested_dict(mc_datasets, keys):
    paths = nested_dict.get_item_nested_dict(mc_datasets, keys)
    for path in paths:
      print(get_info_path_string_mc_datasets(path, mc_datasets, '  '))
  else:
    print(str(keys)+' do not exist in mc_datasets')


def print_multiple_select_simple(multiple_selection, search_string):
  if search_string not in multiple_selection: return
  print(search_string)
  if 'paths' not in multiple_selection[search_string]: return
  print('  paths:')
  for path in multiple_selection[search_string]['paths']:
    print('    '+path)
  if 'selected_paths' not in multiple_selection[search_string]: return
  print('  selected_paths:')
  for path in multiple_selection[search_string]['selected_paths']:
    print('    '+path)
  if 'reason' not in multiple_selection[search_string]: return
  print('  reason')
  print('    '+multiple_selection[search_string]['reason'])

def need_to_select(multiple_selection, multiple_selection_from_file, search_string):
  if search_string not in multiple_selection_from_file: return True
  if 'selected_paths' not in multiple_selection_from_file[search_string]: return True
  #if sorted(multiple_selection[search_string]['paths']) != sorted(multiple_selection_from_file[search_string]['paths']): return True
  if multiple_selection[search_string]['paths'] != multiple_selection_from_file[search_string]['paths']: return True
  return False

# multiple_selection[search_string]= {'paths':[paths], 'selected_paths':[path_selection], 'reason':reason}
def update_selection(search_string, paths, selected_dataset_indices, reason, multiple_selection_from_file):
  selected_paths = []
  for selected_dataset_index in selected_dataset_indices:
    selected_paths.append(paths[selected_dataset_index])
  multiple_selection_from_file[search_string] = {}
  multiple_selection_from_file[search_string]['paths']= paths
  multiple_selection_from_file[search_string]['selected_paths'] = selected_paths
  multiple_selection_from_file[search_string]['reason'] = reason

def get_list_string(a_list):
  list_string = ''
  for item in a_list:
    list_string += str(item)+','
  return list_string[:-1]


def print_multiple_dataset_info(search_string, multiple_selection, search_string_to_keys_mc_datasets, path_to_keys_mc_datasets, same_parent_paths, multiple_mc_datasets, mini_to_nanos_from_nanoaod, nano_to_mini_from_miniaod):
  print('search keys: ['+ get_list_string(search_string_to_keys_mc_datasets[search_string]) +'] : '+search_string)
  print('')
  paths = multiple_selection[search_string]['paths']
  common_name = get_common_name(paths)
  if len(common_name) != 0:
    print('  COMMON_PATH: '+common_name)
  print('')
  for ipath, path in enumerate(paths):
    print('')
    #if len(common_name) != 0: print termcolor.colored('['+str(ipath)+'] ' + path.replace(common_name,'|COMMON_PATH|'), 'red')
    if len(common_name) != 0: print termcolor.colored('['+str(ipath)+'] ' + path, 'red')
    else: print('  ['+str(ipath)+'] '+path)
    print(get_info_path_string(path, path_to_keys_mc_datasets, same_parent_paths, multiple_mc_datasets, mini_to_nanos_from_nanoaod, nano_to_mini_from_miniaod, '  '))

def is_number_string_good(number_string, min_num, max_num):
  if not unicode(number_string,'utf-8').isnumeric():
    print(number_string+' is not a number.')
    return False
  if int(number_string) < min_num:
    print(number_string+' is too low.')
    return False
  if int(number_string) > max_num:
    print(number_string+' is too high.')
    return False
  return True

# Returns: value, exception_value
def select_multiple_numbers(string, min_num, max_num):
  selected_string = raw_input(string)
  if selected_string == 'q': return None, 'q'
  if selected_string == 's': return None, 's'
  if selected_string == 'p': return None, 'p'
  split_selected_string = selected_string.split(',')
  if len(split_selected_string) == 0:
    if is_number_string_good(selected_string, min_num, max_num): return [int(selected_string)]
    else: return select_multiple_numbers(string, min_num, max_num)
  else:
    number_is_good = False
    multiple_numbers = []
    for number in split_selected_string:
      if not is_number_string_good(number, min_num, max_num):
        return select_multiple_numbers(string, min_num, max_num)
      else:
        multiple_numbers.append(int(number))
    return multiple_numbers, None

def select_yn():
  yn = raw_input('Are you sure? [(y)es/(n)o] ')
  if 'y' == yn or 'n' == yn: return yn
  print('Please type y or n.')
  return select_yn()

def make_example(paths):
  out_string = ''
  for ipath in range(len(paths)):
    out_string += str(ipath)+','
  return out_string[:-1]

# Returns: value, exception_value
def select_dataset_indices_and_reason(mc_tag_meta, mc_datasets, paths):
  selected_dataset_indices, exception_value = select_multiple_numbers('[Example] '+make_example(paths)+'\nSelect datasets by index or (q)uit or (s)kip or (p)rint: ', 0, len(paths)-1)
  if selected_dataset_indices == None: 
    if exception_value == 'p':
      keys_string = raw_input('Type in keys for datasets to print: ')
      keys = keys_string.split(',')
      #search_string = datasets.get_mc_dataset_search_string_keys(mc_tag_meta, keys)
      print_mc_datasets_simple(mc_datasets, keys)
      return select_dataset_indices_and_reason(mc_tag_meta, mc_datasets, paths)
    else: return None, exception_value
  reason = raw_input('Type in a reason of selection:\n')
  print('')
  for selected_dataset_index in selected_dataset_indices:
    print('Selected ['+str(selected_dataset_index)+'] : '+paths[selected_dataset_index]+'.')
  print('Reason:')
  print(reason)
  print('')
  yn = select_yn()
  if yn == 'n':
    return select_dataset_indices_and_reason(mc_tag_meta, mc_datasets, paths)
  return [selected_dataset_indices, reason], None

# multiple_selection[search_string]= {'paths':[paths], 'selected_paths':[path_selection], 'reason':reason}
def get_multiple_selection(multiple_mc_datasets, data_tiers):
  multiple_selection = {}
  for data_tier in data_tiers:
    for mc_dataset_name in multiple_mc_datasets:
      for year in multiple_mc_datasets[mc_dataset_name]:
        if data_tier in multiple_mc_datasets[mc_dataset_name][year]:
          mc_dataset_search_string = datasets.get_mc_dataset_search_string(mc_tag_meta, mc_dataset_name, year, data_tier)
          paths = sorted(multiple_mc_datasets[mc_dataset_name][year][data_tier].keys())
          if mc_dataset_search_string not in multiple_selection:
            multiple_selection[mc_dataset_search_string] = {}
          else:
            print('[Error] There are multiple '+mc_dataset_search_string+' in multiple_selection.')
          multiple_selection[mc_dataset_search_string]['paths'] = paths
  return multiple_selection

def get_common_name(names):
  if len(names) == 0: return ''
  common_name = names[0]
  for name in names:
    match = difflib.SequenceMatcher(None, common_name, name).find_longest_match(0, len(common_name), 0, len(name))
    t_common_name = common_name[match.a: match.a + match.size]
    #print('find', common_name, name, '->', t_common_name)
    common_name = t_common_name
  return common_name

def has_same_parent(same_parent_paths, path):
  is_same_parent = False
  for same_parent in same_parent_paths:
    if path in same_parent_paths[same_parent]:
      return True
  return is_same_parent

def get_info_path_string(path, path_to_keys_mc_datasets, same_parent_paths, mc_datasets, mini_to_nanos_from_nanoaod, nano_to_mini_from_miniaod, prefix):

  out_info_path = ''
  keys = path_to_keys_mc_datasets[path]
  keys.append(path)
  path_info = nested_dict.get_item_nested_dict(mc_datasets, keys)
  bad_pu = filter_datasets_jsons.reject_bad_pu_2017_mc_datasets('', mc_datasets, keys[0], keys[1], keys[2], keys[3])
  
  # Print extra info
  if bad_pu:
    out_info_path += prefix+'  BAD PU\n'
  if has_same_parent(same_parent_paths, path):
    out_info_path += prefix+'  Has same parent\n'
  if 'miniaod' in keys:
    if path in mini_to_nanos_from_nanoaod:
      out_info_path += prefix+'  Matching nanoaod: '+get_list_string(mini_to_nanos_from_nanoaod[path])+'\n'
    else:
      out_info_path += prefix+'  No matching nanoaod.\n'
  if 'nanoaod' in keys:
    if path in nano_to_mini_from_miniaod:
      out_info_path += prefix+'  Matching miniaod: '+nano_to_mini_from_miniaod[path]+'\n'
    else:
      out_info_path += prefix+'  No matching miniaod.'

  out_info_path += prefix + '  creation_time: '+str(datasets.convert_timestamp(path_info['creation_time']))+'\n'
  out_info_path += prefix + '  number_events: '+"{:,}".format(path_info['number_events'])+'\n'
  out_info_path += prefix + '  parent_chain:\n'
  parent_prefix = '    '
  for parent in path_info['parent_chain']:
    out_info_path += prefix + parent_prefix + parent +'\n'
    parent_prefix += '  '
  if len(path_info['children']) != 0:
    out_info_path += prefix + '  children:\n'
    for child in path_info['children']:
      out_info_path += prefix + '    ' + child+'\n'
  out_info_path = out_info_path.rstrip()
  return out_info_path

# multiple_selection[search_string]= {'paths':[paths], 'selected_paths':[path_selection], 'reason':reason}
def select_paths_from_multiple(mc_tag_meta, mc_datasets, multiple_selection):
  mc_datasets_selected = {}
  mc_datasets_non_selected = {}
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        search_string = datasets.get_mc_dataset_search_string(mc_tag_meta, mc_dataset_name, year, data_tier)
        multiple_selected_paths = None
        if search_string in multiple_selection:
          multiple_selected_paths = multiple_selection[search_string]['selected_paths']
        if len(mc_datasets[mc_dataset_name][year][data_tier]) == 1: 
          path = next(iter(mc_datasets[mc_dataset_name][year][data_tier]))
          nested_dict.fill_nested_dict(mc_datasets_selected, [mc_dataset_name, year, data_tier, path], mc_datasets[mc_dataset_name][year][data_tier][path])
        else:
          for path in mc_datasets[mc_dataset_name][year][data_tier]:
            if multiple_selected_paths != None:
              if path in multiple_selected_paths:
                nested_dict.fill_nested_dict(mc_datasets_selected, [mc_dataset_name, year, data_tier, path], mc_datasets[mc_dataset_name][year][data_tier][path])
            else:
              print('[Warning] '+path+' is not in multiple_selection. Will not select any dataset.')
              nested_dict.fill_nested_dict(mc_datasets_non_selected, [mc_dataset_name, year, data_tier, path], mc_datasets[mc_dataset_name][year][data_tier][path])
  return mc_datasets_selected, mc_datasets_non_selected
  
if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Selects datasets_jsons.')
  parser.add_argument('-m', '--meta_folder', metavar='./meta', nargs=1, default=['./meta'])
  parser.add_argument('-d', '--data_tiers', metavar='"nanoaod"', nargs=1, default=['nanoaod'])
  parser.add_argument('-t', '--mc_data', metavar='"mc,data"', nargs=1, default=['mc,data'])
  parser.add_argument('-i', '--in_json_folder', metavar='./jsons', nargs=1, default=['./jsons'])
  parser.add_argument('-ip', '--in_json_prefix', metavar='filtered_', nargs=1, default=['filtered_'])
  parser.add_argument('-o', '--out_json_folder', metavar='./jsons', nargs=1, default=['./jsons'])
  parser.add_argument('-op', '--out_json_prefix', metavar='selected_', nargs=1, default=['selected_'])

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

  multiple_selection_filename = os.path.join(args['out_json_folder'], args['out_json_prefix']+'mc_multiple_selection.json')
  selected_mc_datasets_filename = os.path.join(args['out_json_folder'], args['out_json_prefix']+'mc_datasets.json')
  selected_data_datasets_filename = os.path.join(args['out_json_folder'], args['out_json_prefix']+'data_datasets.json')

  if make_mc_datasets:
    #data_tiers = ['miniaod']
    ## mc_dataset_names[year] = [(mc_dataset_name, mc_dir)]
    #mc_dataset_names = datasets.parse_multiple_mc_dataset_names([
    #  [mc_dataset_common_names_filename, ['2016', '2017', '2018']],
    #  [mc_dataset_2016_names_filename, ['2016']],
    #  [mc_dataset_2017_names_filename, ['2017']],
    #  [mc_dataset_2018_names_filename, ['2018']],
    #  ])
    #print ('dataset_names:', mc_dataset_names)
    # Ex) tag_meta[2016] = RunIISummer16, MiniAODv3, NanoAODv5
    mc_tag_meta = datasets.parse_mc_tag_meta(mc_tag_meta_filename)

  if make_data_datasets:
    # Ex) data_tag_meta[2016][B][MET][miniaod] = 17Jul2018
    data_tag_meta = datasets.parse_data_tag_meta(data_tag_meta_filename)

  if make_mc_datasets:
    # mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
    mc_datasets = nested_dict.load_json_file(mc_datasets_filename)
    datasets.check_false_none_mc_datasets(mc_datasets)

    # Make meta data
    path_to_keys_mc_datasets = datasets.get_path_to_keys_mc_datasets(mc_datasets)
    search_string_to_keys_mc_datasets =  datasets.get_search_string_to_keys_mc_datasets(mc_tag_meta, mc_datasets)
    same_parent_paths = datasets.get_same_parent_paths(mc_datasets)
    multiple_mc_datasets = datasets.get_multiple_mc_datasets(mc_datasets)
    mini_to_nanos_from_nanoaod = datasets.get_mini_to_nanos_from_nanoaod_mc_datasets(mc_datasets)
    nano_to_mini_from_miniaod = datasets.get_nano_to_mini_from_miniaod_mc_datasets(mc_datasets)

  if make_data_datasets:
    data_datasets = nested_dict.load_json_file(data_datasets_filename)
    datasets.check_false_none_data_datasets(data_datasets)
    datasets.print_multiple_data_datasets(data_datasets)
    nested_dict.save_json_file(data_datasets, selected_data_datasets_filename)

  if make_mc_datasets:
    # multiple_selection[search_string]= {'paths':[paths], 'selected_paths':[path_selection], 'reason':reason}
    # Load multiple_selection from file
    multiple_selection_from_file = {}
    if os.path.exists(multiple_selection_filename):
      multiple_selection_from_file = nested_dict.load_json_file(multiple_selection_filename)
    # Load multiple_selection from json
    multiple_selection = get_multiple_selection(multiple_mc_datasets, data_tiers)

    # Should do automatic selection for ext
    auto_select_extension(multiple_selection, multiple_selection_from_file, multiple_selection_filename)

    # Show previous selections
    print('--------')
    print('Previous selections.')
    print('')
    for search_string in sorted(multiple_selection_from_file.keys()):
      print_multiple_select_simple(multiple_selection_from_file, search_string)
      print('')
    print('--------')

    for search_string in sorted(multiple_selection.keys()):
      print('--------')
      print(str(count_number_selections_left(multiple_selection, multiple_selection_from_file))+' selections left')

      # Check if need to select
      if need_to_select(multiple_selection, multiple_selection_from_file, search_string):
        if search_string in multiple_selection_from_file:
          print('[Before]')
          print_multiple_select_simple(multiple_selection_from_file, search_string)
          print('Paths are different with before. Please re-select.')
          print('')
      else:
        print(search_string +' was already selected.\nSelected below paths.')
        for selected_path in multiple_selection_from_file[search_string]['selected_paths']:
          print('  '+selected_path)
        print('')
        continue

      # Select
      print_multiple_dataset_info(search_string, multiple_selection, search_string_to_keys_mc_datasets, path_to_keys_mc_datasets, same_parent_paths, multiple_mc_datasets, mini_to_nanos_from_nanoaod, nano_to_mini_from_miniaod)
      paths = multiple_selection[search_string]['paths']
      selected_result, exception_value = select_dataset_indices_and_reason(mc_tag_meta, mc_datasets, paths)
      # Abort due to 'q' selection
      if selected_result == None and exception_value=='q': break
      # Skip due to 's' selection
      if selected_result == None and exception_value=='s': continue
      selected_dataset_indices = selected_result[0]
      reason = selected_result[1]
      update_selection(search_string, multiple_selection[search_string]['paths'], selected_dataset_indices, reason, multiple_selection_from_file)
      nested_dict.save_json_file(multiple_selection_from_file, multiple_selection_filename)
      print('Saved to '+multiple_selection_filename)
      print('')

    if multiple_selection_from_file != {}:
      # Save final result to mc_datasets file
      mc_datasets_selected, mc_datasets_non_selected = select_paths_from_multiple(mc_tag_meta, mc_datasets, multiple_selection_from_file)
      # Do simple checks
      datasets.print_same_parent_mc_datasets(mc_datasets_selected)
      # Save results
      nested_dict.save_json_file(mc_datasets_selected, selected_mc_datasets_filename)
    



  #datasets.print_same_parent_mc_datasets(mc_datasets)
  #datasets.print_multiple_mc_datasets(mc_datasets)
  #datasets.print_incomplete_parent_mc_datasets(mc_datasets)
  # print info for multiple datasets
  # select between multiple datasets
