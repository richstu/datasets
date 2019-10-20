#!/usr/bin/env python

# -------- Instructions for running on your laptop -----------
# one time pre-requisites: 
#    - intall go compiler https://golang.org/doc/install
#    - remember to set GOPATH (I just set it to the dasgoclient folder)
#    - then follow instructions at https://github.com/dmwm/dasgoclient
# every time:
#    - go to lxplus, generate proxy by running voms-proxy-init, it will be stored in some folder /tmp/x509..., remember the lxplus node
#    - copy the file x509... to the computer where you want to run the dasgoclient
#    - export X509_USER_PROXY=/tmp/x509up_u31582
#    - test it, e.g.:
#         ./dasgoclient -query="parent dataset=/TTJets_SingleLeptFromTbar_genMET-150_TuneCP5_13TeV-madgraphMLM-pythia8/RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2/MINIAODSIM"
import subprocess, sys, os
import json
import multiprocessing
import datetime
import itertools
import nested_dict

## mc_dataset_names = [mc_dataset]
## Ex) mc_dataset_names[0] = TTJets_SingleLeptFromT_Tune
#def parse_mc_dataset_names(mc_dataset_names_filename):
#  mc_dataset_names = []
#  with open(mc_dataset_names_filename) as mc_dataset_names_file:
#    for line in mc_dataset_names_file:
#      if line[0]=='#': continue
#      if '#' in line: line_clean = line.split('#')[0].rstrip()
#      else: line_clean = line.rstrip()
#      mc_dataset_names.append(line_clean)
#  return mc_dataset_names

#mc_dataset_names_list = [[mc_dataset_names, [year]]]
def parse_multiple_mc_dataset_names(mc_dataset_names_list):
  # mc_dataset_names[year] = [mc_dataset_name, path]
  mc_dataset_names = {}
  for mc_dataset_names_item in mc_dataset_names_list:
    t_mc_dataset_names = parse_mc_dataset_names(mc_dataset_names_item[0], mc_dataset_names_item[1])
    mc_dataset_names = add_mc_dataset_names(mc_dataset_names, t_mc_dataset_names)
  return mc_dataset_names

# mc_dataset_names[year] = [mc_dataset_name]
# Ex) mc_dataset_names[2016] = TTJets_SingleLeptFromT_Tune
def parse_mc_dataset_names(mc_dataset_names_filename, years):
  mc_dataset_names = {}
  with open(mc_dataset_names_filename) as mc_dataset_names_file:
    for line in mc_dataset_names_file:
      if line[0]=='#': continue
      if '#' in line: line_clean = line.split('#')[0].rstrip()
      else: line_clean = line.rstrip()
      line_split = line_clean.split(' ')
      if len(line_split) == 1:
        mc_dir = 'mc'
      elif len(line_split) == 2:
        mc_dir = line_split[1]
      for year in years:
        fill_mc_dataset_names(mc_dataset_names, year, line_split[0], mc_dir)
  return mc_dataset_names

def fill_mc_dataset_names(mc_dataset_names, year, mc_dataset_name, mc_dir):
  if year not in mc_dataset_names:
    mc_dataset_names[year] = []
  if mc_dataset_name not in mc_dataset_names[year]:
    mc_dataset_names[year].append([mc_dataset_name, mc_dir])
  else:
    print('[Warning] mc_dataset_name['+str(year)+']:'+mc_dataset_name+' is already in mc_dataset_names')

def add_mc_dataset_names(mc_dataset_names_a, mc_dataset_names_b):
  added_mc_dataset_names = {}
  for year in mc_dataset_names_a:
    for mc_dataset_name, mc_dir in mc_dataset_names_a[year]:
      fill_mc_dataset_names(added_mc_dataset_names, year, mc_dataset_name, mc_dir)
  for year in mc_dataset_names_b:
    for mc_dataset_name, mc_dir in mc_dataset_names_b[year]:
      fill_mc_dataset_names(added_mc_dataset_names, year, mc_dataset_name, mc_dir)
  return added_mc_dataset_names

# mc_tag_meta[year] = [reco_tag, miniaod_tag, nanoaod_tag]
# Ex) mc_tag_meta[2016] = [RunIISummer16, MiniAODv3, NanoAODv5]
def parse_mc_tag_meta(tag_meta_filename):
  mc_tag_meta = {}
  with open(tag_meta_filename) as tag_meta_file:
    for line in tag_meta_file:
      if line[0]=='#': continue
      if '#' in line: line_clean = line.split('#')[0].rstrip()
      else: line_clean = line.rstrip()
      year, reco_tag, miniaod_tag, nanoaod_tag = line_clean.split(' ')
      mc_tag_meta[year] = [reco_tag, miniaod_tag, nanoaod_tag]
    return mc_tag_meta

# data_tag_meta[year][run_group][streams][data_tier] = reco_tag
# Ex) data_tag_meta[2016][B][MET][miniaod] = 17Jul2018
def parse_data_tag_meta(data_tag_meta_filename):
  data_tag_meta = {}
  with open(data_tag_meta_filename) as data_tag_meta_file:
    for line in data_tag_meta_file:
      if line[0]=='#': continue
      if '#' in line: line_clean = line.split('#')[0].rstrip()
      else: line_clean = line.rstrip()
      year, miniaod_reco_tag, nanoaod_reco_tag, streams_tag, runs_tag = line_clean.split(' ')
      streams = streams_tag.split(',')
      runs = runs_tag.split(',')
      for run in runs:
        for stream in streams:
          if year not in data_tag_meta:
            data_tag_meta[year] = {}
          if run not in data_tag_meta[year]:
            data_tag_meta[year][run] = {}
          if stream not in data_tag_meta[year][run]:
            data_tag_meta[year][run][stream] = {}
          data_tag_meta[year][run][stream]['miniaod'] = miniaod_reco_tag
          data_tag_meta[year][run][stream]['nanoaod'] = nanoaod_reco_tag
    return data_tag_meta

def get_data_tier_tag(data_tier, is_data):
  if is_data:
    data_tier_tag = data_tier.upper()
  else:
    data_tier_tag = data_tier.upper()+'SIM'
  return data_tier_tag

# Ex) mc_dataset_path = /mc_dataset_name*/reco_tag+data_tier_tag+*/data_tier
# mc_tag_meta[year] = [reco_tag, miniaod_tag, nanoaod_tag]
def get_mc_dataset_search_string(mc_tag_meta, mc_dataset_name, year, data_tier):
  if year not in mc_tag_meta: return '/'+mc_dataset_name+'*/unknown_tag_for_'+str(year)+'/'+get_data_tier_tag(data_tier,False)
  reco_tag = mc_tag_meta[year][0]
  if data_tier == 'miniaod': data_tier_tag = mc_tag_meta[year][1]
  elif data_tier == 'nanoaod': data_tier_tag = mc_tag_meta[year][2]
  else: data_tier_tag = 'unknown_tag_for_data_tier'
  mc_dataset_path = '/'+mc_dataset_name+'*/'+reco_tag+data_tier_tag+'*/'+get_data_tier_tag(data_tier,False)
  return mc_dataset_path

def get_mc_dataset_search_string_keys(mc_tag_meta, keys):
  return get_mc_dataset_search_string(mc_tag_meta, keys[0], keys[1], keys[2])

# Ex) data_dataset_path = /mc_dataset_name*/reco_tag+data_tier_tag+*/data_tier
# data_tag_meta[year][run_group][streams][data_tier] = reco_tag
def get_data_dataset_search_string(data_tag_meta, stream, year, run_group, data_tier):
  if not nested_dict.is_nested_dict(data_tag_meta, [year, run_group, stream, data_tier]): return '/'+stream+'/Run'+str(year)+run_group+'*unknown_tag_for_'+str(year)+'*/'+get_data_tier_tag(data_tier,True)
  reco_tag = data_tag_meta[year][run_group][stream][data_tier]
  data_dataset_path = '/'+stream+'/Run'+str(year)+run_group+'*'+reco_tag+'*/'+get_data_tier_tag(data_tier,True)
  return data_dataset_path

def get_data_dataset_search_string_keys(data_tag_meta, keys):
  return get_data_dataset_search_string(data_tag_meta, keys[0], keys[1], keys[2], keys[3])

def query_dataset(query_type, dataset_path, verbose=True):
  if verbose: print('Query: '+query_type+' Dataset: '+dataset_path)
  result = subprocess.check_output('dasgoclient -query="'+query_type+' dataset='+dataset_path+'"', shell=True, universal_newlines=True).split()
  if verbose: print('  Result: ' + str(result))
  return result

def get_parent_chain(mc_dataset_path, parent_chain):
  parent = query_dataset('parent', mc_dataset_path)
  if len(parent) == 0: return
  elif len(parent) == 1: 
    parent_chain.append(parent[0])
    get_parent_chain(parent[0], parent_chain)
  else:
    print('[Error] There are multiple parents.')

# meta_info = {'disk_size':int, 'number_files':int, 'number_events':int, 'number_lumis':int, 'creation_time':int}
def make_meta_dataset(dataset_path, verbose=True):
  if verbose: print('Query: meta Dataset: '+dataset_path)
  result = subprocess.check_output('dasgoclient -query="dataset='+dataset_path+'" -json', shell=True)
  if verbose: print('  Result: ' + str(result))
  result_dict_list = json.loads(result)
  meta_info = {}
  for dict_item in result_dict_list:
    if 'creation_time' in dict_item['dataset'][0]:
      nested_dict.fill_dict(meta_info, 'creation_time', dict_item['dataset'][0]['creation_time'])
    if 'size' in dict_item['dataset'][0]:
      nested_dict.fill_dict(meta_info, 'data_size', dict_item['dataset'][0]['size'])
      nested_dict.fill_dict(meta_info, 'number_files', dict_item['dataset'][0]['nfiles'])
      nested_dict.fill_dict(meta_info, 'number_lumis', dict_item['dataset'][0]['nlumis'])
      nested_dict.fill_dict(meta_info, 'number_events', dict_item['dataset'][0]['nevents'])
  return meta_info

# keys_mc_datasets = [ [mc_dataset_name, year, data_tier, search_string] ]
# mc_dataset_names[year] = [mc_dataset_name, mc_dir]
def get_keys_mc_datasets(mc_dataset_names, mc_tag_meta, data_tiers):
  keys_mc_datasets = []
  for year in mc_tag_meta:
    if year not in mc_dataset_names: continue
    for mc_dataset_name, mc_dir in mc_dataset_names[year]:
      for data_tier in data_tiers:
        mc_dataset_search_string = get_mc_dataset_search_string(mc_tag_meta, mc_dataset_name, year, data_tier)
        keys_mc_datasets.append([mc_dataset_name, year, data_tier, mc_dataset_search_string])
  return keys_mc_datasets

# keys_data_datasets = [ [stream, year, run_group, data_tier, search_string] ]
def get_keys_data_datasets(data_tag_meta, data_tiers):
  keys_data_datasets = []
  for year in data_tag_meta:
    for run_group in data_tag_meta[year]:
      for stream in data_tag_meta[year][run_group]:
        for data_tier in data_tiers:
          data_dataset_search_string = get_data_dataset_search_string(data_tag_meta, stream, year, run_group, data_tier)
          keys_data_datasets.append([stream, year, run_group, data_tier, data_dataset_search_string])
  return keys_data_datasets

# mc_datasets_list = [ [ [mc_dataset_name, year, data_tier, path, path_info] ] ]
def get_mc_datasets_list(keys_mc_datasets):
  #print(keys_mc_datasets)
  pool = multiprocessing.Pool()
  mc_datasets_list = pool.map(get_item_mc_datasets_list, keys_mc_datasets)
  return mc_datasets_list

# data_datasets_list = [ [ [stream, year, run_group, data_tier, path, path_info] ] ]
# data_dataset[stream][year][run_group][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
def get_data_datasets_list(keys_data_datasets):
  pool = multiprocessing.Pool()
  data_datasets_list = pool.map(get_item_data_datasets_list, keys_data_datasets)
  return data_datasets_list

# item_mc_datasets_list = [ [mc_dataset_name, year, data_tier, path, path_info] ]
def get_item_mc_datasets_list(arguments):
  item_mc_datasets_list = []
  mc_dataset_name = arguments[0]
  year = arguments[1]
  data_tier = arguments[2]
  mc_dataset_search_string = arguments[3]
  mc_dataset_paths = query_dataset('dataset', mc_dataset_search_string)
  for mc_dataset_path in mc_dataset_paths:
    mc_dataset_path_info = {}
    mc_dataset_parent_chain = []
    get_parent_chain(mc_dataset_path, mc_dataset_parent_chain)
    mc_dataset_path_info['parent_chain'] = mc_dataset_parent_chain
    mc_dataset_children = query_dataset('child', mc_dataset_path)
    mc_dataset_path_info['children'] = mc_dataset_children
    mc_dataset_meta = make_meta_dataset(mc_dataset_path)
    mc_dataset_path_info.update(mc_dataset_meta)
    item_mc_datasets_list.append([mc_dataset_name, year, data_tier, mc_dataset_path, mc_dataset_path_info])
  return item_mc_datasets_list

# item_data_datasets_list = [ [stream, year, run_group, data_tier, path, path_info] ]
def get_item_data_datasets_list(arguments):
  item_data_datasets_list = []
  stream = arguments[0]
  year = arguments[1]
  run_group = arguments[2]
  data_tier = arguments[3]
  data_dataset_search_string = arguments[4]
  data_dataset_paths = query_dataset('dataset', data_dataset_search_string)
  for data_dataset_path in data_dataset_paths:
    data_dataset_path_info = {}
    data_dataset_parent_chain = []
    get_parent_chain(data_dataset_path, data_dataset_parent_chain)
    data_dataset_path_info['parent_chain'] = data_dataset_parent_chain
    data_dataset_children = query_dataset('child', data_dataset_path)
    data_dataset_path_info['children'] = data_dataset_children
    data_dataset_runs = query_dataset('run', data_dataset_path)
    data_dataset_path_info['runs'] = data_dataset_runs
    data_dataset_meta = make_meta_dataset(data_dataset_path)
    data_dataset_path_info.update(data_dataset_meta)
    item_data_datasets_list.append([stream, year, run_group, data_tier, data_dataset_path, data_dataset_path_info])
  return item_data_datasets_list

# mc_dataset_names[year] = [mc_dataset_name, mc_dir]
def make_mc_datasets(mc_dataset_names, mc_tag_meta, data_tiers):
  keys_mc_datasets = get_keys_mc_datasets(mc_dataset_names, mc_tag_meta, data_tiers)
  mc_datasets_list = get_mc_datasets_list(keys_mc_datasets)
  mc_datasets = convert_list_to_mc_datasets(mc_datasets_list)
  #print(mc_datasets)
  return mc_datasets

def make_data_datasets(data_tag_meta, data_tiers):
  keys_data_datasets = get_keys_data_datasets(data_tag_meta, data_tiers)
  data_datasets_list = get_data_datasets_list(keys_data_datasets)
  data_datasets = convert_list_to_data_datasets(data_datasets_list)
  #print(data_datasets)
  return data_datasets

# keys_mc_datasets = [ [mc_dataset_name, year, data_tier, search_string] ]
def get_keys_from_mc_datasets(mc_datasets, mc_tag_meta, data_tiers):
  keys_from_mc_datasets = []
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        mc_dataset_search_string = get_mc_dataset_search_string(mc_tag_meta, mc_dataset_name, year, data_tier)
        keys_from_mc_datasets.append([mc_dataset_name, year, data_tier, mc_dataset_search_string])
  return keys_from_mc_datasets

# keys_data_datasets = [ [stream, year, run_group, data_tier, search_string] ]
def get_keys_from_data_datasets(data_datasets, data_tag_meta, data_tiers):
  keys_from_data_datasets = []
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        for data_tier in data_datasets[stream][year][run_group]:
          data_dataset_search_string = get_data_dataset_search_string(data_tag_meta, stream, year, run_group, data_tier)
          keys_from_data_datasets.append([stream, year, run_group, data_tier, data_dataset_search_string])
  return keys_from_data_datasets

# keys_datasets = [ [keys] ]
def subtract_keys_datasets(keys_datasets_a, keys_datasets_b):
  left_keys_datasets = []
  for keys_a in keys_datasets_a:
    found_keys = False
    for keys_b in keys_datasets_b:
      if keys_a == keys_b: found_keys = True
    if not found_keys:
      left_keys_datasets.append(keys_a)
  return left_keys_datasets

# keys_mc_datasets = [ [mc_dataset_name, year, data_tier, search_string] ]
def update_mc_datasets(mc_dataset_names, mc_tag_meta, data_tiers, original_mc_datasets):
  keys_mc_datasets = get_keys_mc_datasets(mc_dataset_names, mc_tag_meta, data_tiers)
  keys_from_mc_datasets = get_keys_from_mc_datasets(original_mc_datasets, mc_tag_meta, data_tiers)
  #print('meta', keys_data_datasets)
  #print('json', keys_from_data_datasets)
  append_keys_mc_datasets = subtract_keys_datasets(keys_mc_datasets, keys_from_mc_datasets)
  remove_keys_mc_datasets = subtract_keys_datasets(keys_from_mc_datasets, keys_mc_datasets)

  append_mc_datasets_list = get_mc_datasets_list(append_keys_mc_datasets)
  append_mc_datasets = convert_list_to_mc_datasets(append_mc_datasets_list)
  updated_mc_datasets = add_mc_datasets(original_mc_datasets, append_mc_datasets)
  for keys in remove_keys_mc_datasets:
    nested_dict.remove_keys_nested_dict(updated_mc_datasets, keys[:-1])

  print('append_keys', append_keys_mc_datasets)
  print('')
  print('remove_keys', remove_keys_mc_datasets)
  return updated_mc_datasets

# keys_data_datasets = [ [stream, year, run_group, data_tier, search_string] ]
def update_data_datasets(data_tag_meta, data_tiers, original_data_datasets):
  keys_data_datasets = get_keys_data_datasets(data_tag_meta, data_tiers)
  keys_from_data_datasets = get_keys_from_data_datasets(original_data_datasets, data_tag_meta, data_tiers)
  #print('meta', keys_data_datasets)
  #print('json', keys_from_data_datasets)
  append_keys_data_datasets = subtract_keys_datasets(keys_data_datasets, keys_from_data_datasets)
  remove_keys_data_datasets = subtract_keys_datasets(keys_from_data_datasets, keys_data_datasets)

  append_data_datasets_list = get_data_datasets_list(append_keys_data_datasets)
  append_data_datasets = convert_list_to_data_datasets(append_data_datasets_list)
  updated_data_datasets = add_data_datasets(original_data_datasets, append_data_datasets)
  for keys in remove_keys_data_datasets:
    nested_dict.remove_keys_nested_dict(updated_data_datasets, keys[:-1])

  print('append_keys', append_keys_data_datasets)
  print('')
  print('remove_keys', remove_keys_data_datasets)
  return updated_data_datasets

# mc_datasets_list = [ [ [mc_dataset_name, year, data_tier, path, path_info] ] ]
def convert_list_to_mc_datasets(mc_datasets_list):
  mc_datasets = {}
  for item_mc_datasets_list in mc_datasets_list:
    for mc_dataset_info in item_mc_datasets_list:
      keys = mc_dataset_info[0:4]
      path_info = mc_dataset_info[4]
      #print(keys, path_info)
      nested_dict.fill_nested_dict(mc_datasets, keys, path_info)
  return mc_datasets

# data_datasets_list = [ [ [stream, year, run_group, data_tier, path, path_info] ] ]
def convert_list_to_data_datasets(data_datasets_list):
  data_datasets = {}
  for item_data_datasets_list in data_datasets_list:
    for data_dataset_info in item_data_datasets_list:
      keys = data_dataset_info[0:5]
      path_info = data_dataset_info[5]
      #print(keys, path_info)
      nested_dict.fill_nested_dict(data_datasets, keys, path_info)
  return data_datasets

#def save_json_file(dict_name, json_filename):
#  with open(json_filename,'w') as json_file:
#    json.dump(dict_name, json_file, indent=2)
#  print('Saved '+json_filename)

#def ascii_encode_dict(data):
#    ascii_encode = lambda x: x.encode('ascii') if isinstance(x, unicode) else x
#    return dict(map(ascii_encode, pair) for pair in data.items())

#def load_json_file(json_filename, no_null=True):
#  with open(json_filename) as json_file:
#    out_dict = json.load(json_file, object_hook=ascii_encode_dict)
#  #nested_dict.check_key_nested_dict(out_dict, 'null')
#  if no_null:
#    nested_dict.remove_key_nested_dict(out_dict, 'null')
#    nested_dict.check_key_nested_dict(out_dict, 'null')
#    nested_dict.check_key_nested_dict(out_dict, None)
#  return out_dict

# path_to_keys_mc_datasets[path] = [key]
def get_path_to_keys_mc_datasets(mc_datasets):
  path_to_keys_mc_datasets = {}
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        for path in mc_datasets[mc_dataset_name][year][data_tier]:
          if path in path_to_keys_mc_datasets:
            print('[Error] There is already path: '+path+' in path_to_keys_mc_datasets.')
            print('  path_to_keys_mc_datasets['+path+']: '+path_to_keys_mc_datasets[path])
          else:
            path_to_keys_mc_datasets[path] = [mc_dataset_name, year, data_tier]
  return path_to_keys_mc_datasets

# path_to_keys_data_datasets[path] = [key]
def get_path_to_keys_data_datasets(data_datasets):
  path_to_keys_data_datasets = {}
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        for data_tier in data_datasets[stream][year][run_group]:
          for path in data_datasets[stream][year][run_group][data_tier]:
            if path in path_to_keys_data_datasets:
              print('[Error] There is already path'+path+' in path_to_keys_data_datasets.')
              print('  path_to_keys_data_datasets['+path+']: '+str(path_to_keys_data_datasets[path]))
            else:
              path_to_keys_data_datasets[path] = [stream, year, run_group, data_tier]
  return path_to_keys_data_datasets

## path_to_keys_data_datasets[path] = [key]
#def get_path_to_keys_data_datasets(mc_datasets):
#  path_to_keys_data_datasets = {}
#  for stream in data_datasets:
#    for year in data_datasets[stream]:
#      for run_group in data_datasets[stream]:
#        for data_tier in data_datasets[stream][year][run_goup]:
#          for path in data_datasets[data_dataset_name][run_group][year][data_tier]:
#            if path in path_to_keys_data_datasets:
#              print('[Error] There is already path: '+path+' in path_to_keys_data_datasets.')
#              print('  path_to_keys_data_datasets['+path+']: '+path_to_keys_data_datasets[path])
#            else:
#              path_to_keys_data_datasets[path] = [stream, year, run_group, data_tier]
#  return path_to_keys_data_datasets

def get_search_string_to_keys_mc_datasets(mc_tag_meta, mc_datasets):
  search_string_to_keys_mc_datasets = {}
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        search_string = get_mc_dataset_search_string(mc_tag_meta, mc_dataset_name, year, data_tier)
        if search_string in search_string_to_keys_mc_datasets:
          print('[Error] There is already search_string '+search_string+' in search_string_to_keys_mc_datasets.')
          print('  search_string_to_keys_mc_datasets['+search_string+']: '+search_string_to_keys_mc_datasets[path])
        else:
          search_string_to_keys_mc_datasets[search_string] = [mc_dataset_name, year, data_tier]
  return search_string_to_keys_mc_datasets

def check_overlapping_paths_mc_datasets(mc_datasets):
  get_path_to_keys_mc_datasets(mc_datasets)

def check_overlapping_paths_data_datasets(mc_datasets):
  get_path_to_keys_data_datasets(mc_datasets)

def check_false_none_mc_datasets(mc_datasets):
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        for path in mc_datasets[mc_dataset_name][year][data_tier]:
          if path == None and len(mc_datasets[mc_dataset_name][year][data_tier].keys())>1:
            print('[Error]: There is None and also an entry in mc_dataset['+mc_dataset_name+']['+year+']['+data_tier+']. Keys: '+str(mc_datasets[mc_dataset_name][year][data_tier].keys()))
            break

def check_false_none_data_datasets(data_datasets):
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        for data_tier in data_datasets[stream][year][run_group]:
          for path in data_datasets[stream][year][run_group][data_tier]:
            if path == None and len(data_datasets[stream][year][run_group][data_tier])>1:
              print('[Error]: There is None and also an entry in data_dataset['+stream+']['+year+']['+run_group+']['+data_tier+']:'+str(data_datasets[stream][year][run_group][data_tier]))
              break

# same_parent_paths[same_parent] = set(paths)
def get_same_parent_paths(mc_datasets):
  same_parent_paths = {}
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        # Compare paths
        combination_paths_list = map(dict, itertools.combinations(mc_datasets[mc_dataset_name][year][data_tier].iteritems(), 2))
        for combination_paths in combination_paths_list:
          path_a = list(combination_paths.keys())[0]
          path_info_a = mc_datasets[mc_dataset_name][year][data_tier][path_a]
          path_b = list(combination_paths.keys())[1]
          path_info_b = mc_datasets[mc_dataset_name][year][data_tier][path_b]
          for iparent, parent in enumerate(path_info_a['parent_chain']):
            if iparent >= len(path_info_b['parent_chain']): continue
            if parent == path_info_b['parent_chain'][iparent]:
              if parent not in same_parent_paths:
                same_parent_paths[parent] = set()
              same_parent_paths[parent].add(path_a)
              same_parent_paths[parent].add(path_b)
  return same_parent_paths

def get_same_parent_mc_datasets_string(mc_datasets):
  out_string = ''
  same_parent_paths = get_same_parent_paths(mc_datasets)
  # Print
  for parent in same_parent_paths:
    out_string += 'Same parent: '+parent+'\n'
    for path in same_parent_paths[parent]:
        out_string += '  '+path+'\n'
  return out_string

def print_same_parent_mc_datasets(mc_datasets):
  print(get_same_parent_mc_datasets_string(mc_datasets))

def get_mini_to_nanos_from_nanoaod_mc_datasets(mc_datasets):
  mini_to_nanos_from_nanoaod = {}
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      if 'nanoaod' in mc_datasets[mc_dataset_name][year]:
        for path in mc_datasets[mc_dataset_name][year]['nanoaod']:
          path_info = mc_datasets[mc_dataset_name][year]['nanoaod'][path]
          parent = path_info['parent_chain'][0]
          if parent not in mini_to_nanos_from_nanoaod:
            mini_to_nanos_from_nanoaod[parent] = []
          mini_to_nanos_from_nanoaod[parent].append(path)
  return mini_to_nanos_from_nanoaod

def get_nano_to_mini_from_miniaod_mc_datasets(mc_datasets):
  nano_to_mini_from_miniaod = {}
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      if 'miniaod' in mc_datasets[mc_dataset_name][year]:
        for path in mc_datasets[mc_dataset_name][year]['miniaod']:
          path_info = mc_datasets[mc_dataset_name][year]['miniaod'][path]
          for child in path_info['children']:
            if child in nano_to_mini_from_miniaod:
              print('[Error] : child: '+child+' was already filled.')
              print('  nano_to_mini_from_miniaod: '+nano_to_mini_from_miniaod[child])
            nano_to_mini_from_miniaod[child] = path
  return nano_to_mini_from_miniaod

def check_mini_nano_consistentcy_mc_datasets(mc_tag_meta, mc_datasets, ignore_nano_list=[]):
  mini_set_from_nanoaod = set()
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      if 'nanoaod' not in mc_datasets[mc_dataset_name][year]: continue
      for mc_dataset in mc_datasets[mc_dataset_name][year]['nanoaod']:
        path = mc_dataset[0]
        parent = mc_dataset[1]
        mini_set_from_nanoaod.add(parent)
  mini_set_from_miniaod = set()
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      if 'miniaod' not in mc_datasets[mc_dataset_name][year]: continue
      for mc_dataset in mc_datasets[mc_dataset_name][year]['miniaod']:
        path = mc_dataset[0]
        mini_set_from_miniaod.add(path)
  # Compare mini to nano
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      if 'miniaod' not in mc_datasets[mc_dataset_name][year]: continue
      for path in mc_datasets[mc_dataset_name][year]['miniaod']:
        children = mc_datasets[mc_dataset_name][year]['miniaod'][path]['children']
        if path in ignore_nano_list: continue
        if path not in mini_set_from_nanoaod:
          print('NanoAOD does not exist for '+path+', while miniAOD exists.')
          print('  miniaod: '+get_mc_dataset_search_string(mc_tag_meta, mc_dataset_name, year, 'miniaod'))
          print('  nanoaod: '+get_mc_dataset_search_string(mc_tag_meta, mc_dataset_name, year, 'nanoaod'))
          print('  MiniAOD children:'+str(children))
  print('')
  # Compare nano to mini
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      if 'nanoaod' not in mc_datasets[mc_dataset_name][year]: continue
      for path in mc_datasets[mc_dataset_name][year]['nanoaod']:
        parent_chain = mc_datasets[mc_dataset_name][year]['nanoaod'][path]['parent_chain']
        parent = parent_chain[0]
        parent_parent = parent_chain[1]
        if parent not in mini_set_from_miniaod:
          print('MiniAOD does not exist for '+parent+', while nanoAOD: '+path+' exists.')
          print('  miniaod: '+get_mc_dataset_search_string(mc_tag_meta, mc_dataset_name, year, 'miniaod'))
          print('  nanoaod: '+get_mc_dataset_search_string(mc_tag_meta, mc_dataset_name, year, 'nanoaod'))
          print('  Parent of MiniAOD:'+parent_parent)

def subtract_mc_datasets(mc_datasets_a, mc_datasets_b):
  left_mc_datasets = {}
  for mc_dataset_name in mc_datasets_a:
    for year in mc_datasets_a[mc_dataset_name]:
      for data_tier in mc_datasets_a[mc_dataset_name][year]:
        for path in mc_datasets_a[mc_dataset_name][year][data_tier]:
          path_a_info = mc_datasets_a[mc_dataset_name][year][data_tier][path]
          if mc_dataset_name in mc_datasets_b:
            if year in mc_datasets_b[mc_dataset_name]:
              if data_tier in mc_datasets_b[mc_dataset_name][year]:
                if path in mc_datasets_b[mc_dataset_name][year][data_tier]:
                  if path_a_info != mc_datasets_b[mc_dataset_name][year][data_tier][path]:
                    print('[Error] mc_dataset_a['+mc_dataset_name+']['+year+']['+data_tier+']['+path+'] and mc_dataset_b['+mc_dataset_name+']['+year+']['+data_tier+']['+path+'] do not match')
                    print('  mc_dataset_a['+mc_dataset_name+']['+year+']['+data_tier+']['+path+']: '+mc_datasets_a[mc_dataset_name][year][data_tier][path])
                    print('  mc_dataset_b['+mc_dataset_name+']['+year+']['+data_tier+']['+path+']: '+mc_datasets_b[mc_dataset_name][year][data_tier][path])
                else:
                  nested_dict.fill_nested_dict(left_mc_datasets, [mc_dataset_name, year, data_tier, path], path_a_info)
              else:
                nested_dict.fill_nested_dict(left_mc_datasets, [mc_dataset_name, year, data_tier, path], path_a_info)
            else:
              nested_dict.fill_nested_dict(left_mc_datasets, [mc_dataset_name, year, data_tier, path], path_a_info)
          else:
            nested_dict.fill_nested_dict(left_mc_datasets, [mc_dataset_name, year, data_tier, path], path_a_info)
        ## Fill incase all were subtracted
        #if not is_mc_datasets(left_mc_datasets, mc_dataset_name,year,data_tier): 
        #  nested_dict.fill_empty_nested_dict(left_mc_datasets, [mc_dataset_name, year, data_tier])
    check_false_none_mc_datasets(left_mc_datasets)
  return left_mc_datasets

def subtract_data_datasets(data_datasets_a, data_datasets_b):
  left_data_datasets = {}
  for stream in data_datasets_a:
    for year in data_datasets_a[stream]:
      for run_group in data_datasets_a[stream][year]:
        for data_tier in data_datasets_a[stream][year][run_group]:
          for path in data_datasets_a[stream][year][run_group][data_tier]:
            path_a_info = data_datasets_a[stream][year][run_group][data_tier][path]
            if stream in data_datasets_b:
              if year in data_datasets_b[stream]:
                if run_group in data_datasets_b[stream][year]:
                  if data_tier in data_datasets_b[stream][year][run_group]:
                    if path in data_datasets_b[stream][year][run_group][data_tier]:
                      if path_a_info != data_datasets_b[stream][year][run_group][data_tier][path]:
                        print('[Error] data_dataset_a['+stream+']['+year+']['+run_group+']['+data_tier+']['+path+'] and data_dataset_b['+stream+']['+year+']['+run_group+']['+data_tier+']['+path+'] do not match')
                        print('  data_dataset_a['+stream+']['+year+']['+run_group+']['+data_tier+']['+path+']: '+data_datasets_a[stream][year][run_group][data_tier][path])
                        print('  data_dataset_b['+stream+']['+year+']['+run_group+']['+data_tier+']['+path+']: '+data_datasets_b[stream][year][run_group][data_tier][path])
                    else:
                      nested_dict.fill_nested_dict(left_data_datasets, [stream, year, run_group, data_tier, path], path_a_info)
                  else:
                    nested_dict.fill_nested_dict(left_data_datasets, [stream, year, run_group, data_tier, path], path_a_info)
                else:
                  nested_dict.fill_nested_dict(left_data_datasets, [stream, year, run_group, data_tier, path], path_a_info)
              else:
                nested_dict.fill_nested_dict(left_data_datasets, [stream, year, run_group, data_tier, path], path_a_info)
            else:
              nested_dict.fill_nested_dict(left_data_datasets, [stream, year, run_group, data_tier, path], path_a_info)
    check_false_none_data_datasets(left_data_datasets)
  return left_data_datasets

def add_mc_datasets(mc_datasets_a, mc_datasets_b):
  full_mc_datasets = {}
  # Add dataset_a
  for mc_dataset_name in mc_datasets_a:
    for year in mc_datasets_a[mc_dataset_name]:
      for data_tier in mc_datasets_a[mc_dataset_name][year]:
        for path in mc_datasets_a[mc_dataset_name][year][data_tier]:
          path_info = mc_datasets_a[mc_dataset_name][year][data_tier][path]
          nested_dict.fill_nested_dict(full_mc_datasets,[mc_dataset_name, year, data_tier, path], path_info)
  # Add dataset_b
  for mc_dataset_name in mc_datasets_b:
    for year in mc_datasets_b[mc_dataset_name]:
      for data_tier in mc_datasets_b[mc_dataset_name][year]:
        for path in mc_datasets_b[mc_dataset_name][year][data_tier]:
          path_info = mc_datasets_b[mc_dataset_name][year][data_tier][path]
          if not nested_dict.is_nested_dict(full_mc_datasets, [mc_dataset_name, year, data_tier, path]):
            nested_dict.fill_nested_dict(full_mc_datasets,[mc_dataset_name, year, data_tier, path], path_info)
          else:
            if path_info != full_mc_datasets[mc_dataset_name][year][data_tier][path]:
              print('[Error] mc_dataset_b and full_mc_dataset has different path_info')
              print('  mc_dataset_b['+mc_dataset_name+']['+year+']['+data_tier+']['+path+']: '+str(path_info))
              print('  full_mc_dataset['+mc_dataset_name+']['+year+']['+data_tier+']['+path+']: '+str(full_mc_datasets[mc_dataset_name][year][data_tier][path]))
            else:
              print('[Warning] overlapping mc_dataset['+mc_dataset_name+']['+year+']['+data_tier+']['+path+']')
  return full_mc_datasets

def add_data_datasets(data_datasets_a, data_datasets_b):
  full_data_datasets = {}
  # Add dataset_a
  for stream in data_datasets_a:
    for year in data_datasets_a[stream]:
      for run_group in data_datasets_a[stream][year]:
        for data_tier in data_datasets_a[stream][year][run_group]:
          for path in data_datasets_a[stream][year][run_group][data_tier]:
            path_info = data_datasets_a[stream][year][run_group][data_tier][path]
            nested_dict.fill_nested_dict(full_data_datasets,[stream, year, run_group, data_tier, path], path_info)
  # Add dataset_b
  for stream in data_datasets_b:
    for year in data_datasets_b[stream]:
      for run_group in data_datasets_b[stream][year]:
        for data_tier in data_datasets_b[stream][year][run_group]:
          for path in data_datasets_b[stream][year][run_group][data_tier]:
            path_info = data_datasets_b[stream][year][run_group][data_tier][path]
            if not nested_dict.is_nested_dict(full_data_datasets, [stream, year, run_group, data_tier, path]):
              nested_dict.fill_nested_dict(full_data_datasets,[stream, year, run_group, data_tier, path], path_info)
            else:
              if path_info != full_data_datasets[stream][year][run_group][data_tier][path]:
                print('[Error] data_dataset_b and full_data_dataset has different path_info')
                print('  data_dataset_b['+stream+']['+year+']['+run_group+']['+data_tier+']['+path+']: '+str(path_info))
                print('  full_data_dataset['+stream+']['+year+']['+run_group+']['+data_tier+']['+path+']: '+str(full_data_datasets[stream][year][run_group][data_tier][path]))
              else:
                print('[Warning] overlapping data_dataset['+stream+']['+year+']['+run_group+']['+data_tier+']['+path+']')
  return full_data_datasets

def get_path_mc_datasets_string(mc_datasets, mc_dataset_name, year, data_tier, path):
  return str(path)+'\n'

def get_path_data_datasets_string(data_datasets, stream, year, run_group, data_tier, path):
  return str(path)+'\n'

def get_path_parent_mc_datasets_string(mc_datasets, mc_dataset_name, year, data_tier, path):
  path_info = mc_datasets[mc_dataset_name][year][data_tier][path]
  out_string = str(path)+'\n'
  for parent in path_info['parent_chain']:
    out_string += '  '+parent+' -> '
  out_string = out_string[:-4]+'\n'
  return out_string

def get_multiple_mc_datasets_string(mc_datasets, mc_dataset_name, year, data_tier, path):
  out_string = ''
  path_info = mc_datasets[mc_dataset_name][year][data_tier][path]
  if path == next(iter(mc_datasets[mc_dataset_name][year][data_tier])):
    if len(mc_datasets[mc_dataset_name][year][data_tier]) > 1:
      out_string = 'Multiple mc_datasets['+mc_dataset_name+']['+year+']['+data_tier+']:\n'
      for path in mc_datasets[mc_dataset_name][year][data_tier]:
        out_string += ('  '+path+'\n')
  return out_string

def get_multiple_data_datasets_string(data_datasets, stream, year, run_group, data_tier, path):
  out_string = ''
  path_info = data_datasets[stream][year][run_group][data_tier][path]
  if path == next(iter(data_datasets[stream][year][run_group][data_tier])):
    if len(data_datasets[stream][year][run_group][data_tier]) > 1:
      out_string = 'Multiple data_datasets['+stream+']['+year+']['+run_group+']['+data_tier+']:\n'
      for path in data_datasets[stream][year][run_group][data_tier]:
        out_string += ('  '+path+'\n')
  return out_string

def get_missing_mc_datasets_string(mc_datasets, mc_dataset_name, year, data_tier, path):
  return 'Missing mc_dataset['+mc_dataset_name+']['+year+']['+data_tier+']: '+str(mc_datasets[mc_dataset_name][year][data_tier][path])+'\n'

def get_missing_data_datasets_string(data_datasets, stream, year, run_group, data_tier, path):
  return 'Missing data_dataset['+stream+']['+year+']['+run_group+']['+data_tier+']: '+str(data_datasets[stream][year][run_group][data_tier][path])+'\n'

def get_incomplete_parent_mc_datasets_string(mc_datasets, mc_dataset_name, year, data_tier, path):
  return 'Incomplete parent_chain mc_dataset['+mc_dataset_name+']['+year+']['+data_tier+']['+str(path)+']: '+str(mc_datasets[mc_dataset_name][year][data_tier][path])+'\n'

def get_path_each_key_mc_datasets_string(mc_tag_meta, mc_datasets):
  out_string = ''
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        out_string += mc_dataset_name+' '+year+' '+data_tier+': '+get_mc_dataset_search_string(mc_tag_meta, mc_dataset_name, year, data_tier)+'\n'
        for path in mc_datasets[mc_dataset_name][year][data_tier]:
          out_string += '  '+path+'\n'
  return out_string

def get_path_each_key_data_datasets_string(data_tag_meta, data_datasets):
  out_string = ''
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        for data_tier in data_datasets[stream][year][run_group]:
          out_string += stream+' '+year+' '+run_group+' '+data_tier+': '+get_data_dataset_search_string(data_tag_meta, stream, year, run_group, data_tier)+'\n'
          for path in data_datasets[stream][year][run_group][data_tier]:
            out_string += '  '+path+'\n'
  return out_string

def print_path_each_key_mc_datasets_string(mc_tag_meta, mc_datasets):
  print(get_path_each_key_mc_datasets_string(mc_tag_meta, mc_datasets))

def print_path_each_key_data_datasets_string(data_tag_meta, data_datasets):
  print(get_path_each_key_data_datasets_string(data_tag_meta, data_datasets))

def print_path_mc_datasets(mc_datasets):
  print(get_mc_datasets_string(mc_datasets, get_path_mc_datasets_string))

def print_path_data_datasets(data_datasets):
  print(get_data_datasets_string(data_datasets, get_path_data_datasets_string))

def print_path_mc_datasets_with_data_tier(mc_datasets, data_tier):
  print(get_mc_datasets_with_data_tier_string(mc_datasets, get_path_mc_datasets_string, data_tier))

def print_path_data_datasets_with_data_tier(data_datasets, data_tier):
  print(get_data_datasets_with_data_tier_string(data_datasets, get_path_data_datasets_string, data_tier))

def print_path_parent_mc_datasets(mc_datasets):
  print(get_mc_datasets_string(mc_datasets, get_path_parent_mc_datasets_string))

def print_multiple_mc_datasets(mc_datasets):
  print(get_mc_datasets_string(mc_datasets, get_multiple_mc_datasets_string))

def print_multiple_data_datasets(data_datasets):
  print(get_data_datasets_string(data_datasets, get_multiple_data_datasets_string))

def print_missing_mc_datasets(keys_mc_datasets, mc_datasets):
  missing_mc_datasets = get_missing_mc_datasets(keys_mc_datasets, mc_datasets)
  print(get_mc_datasets_string(missing_mc_datasets, get_missing_mc_datasets_string))

def print_missing_data_datasets(keys_data_datasets, data_datasets):
  missing_data_datasets = get_missing_data_datasets(keys_data_datasets, data_datasets)
  print(get_data_datasets_string(missing_data_datasets, get_missing_data_datasets_string))

def print_incomplete_parent_mc_datasets(mc_datasets):
  incomplete_parent_mc_datasets = get_incomplete_parent_mc_datasets(mc_datasets)
  print(get_mc_datasets_string(incomplete_parent_mc_datasets, get_incomplete_parent_mc_datasets_string))

# mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
# mc_string_function(mc_datasets, mc_dataset_name, year, data_tier, path)
def get_mc_datasets_string(mc_datasets, mc_string_function):
  result_string = ''
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        for path in mc_datasets[mc_dataset_name][year][data_tier]:
          path_info = mc_datasets[mc_dataset_name][year][data_tier][path]
          result_string += mc_string_function(mc_datasets, mc_dataset_name, year, data_tier, path)
  return result_string

# data_datasets[stream][year][run_group][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
# data_string_function(data_datasets, stream, year, run_group, data_tier, path)
def get_data_datasets_string(data_datasets, data_string_function):
  result_string = ''
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        for data_tier in data_datasets[stream][year][run_group]:
          for path in data_datasets[stream][year][run_group][data_tier]:
            path_info = data_datasets[stream][year][run_group][data_tier][path]
            result_string += data_string_function(data_datasets, stream, year, run_group, data_tier, path)
  return result_string

# data_datasets[stream][year][run_group][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
# data_string_function(data_datasets, stream, year, run_group, data_tier, path)
def get_data_datasets_with_data_tier_string(data_datasets, data_string_function, data_tier):
  result_string = ''
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        if data_tier in data_datasets[stream][year][run_group]:
          for path in data_datasets[stream][year][run_group][data_tier]:
            path_info = data_datasets[stream][year][run_group][data_tier][path]
            result_string += data_string_function(data_datasets, stream, year, run_group, data_tier, path)
  return result_string

# mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
# mc_string_function(mc_datasets, mc_dataset_name, year, data_tier, path)
def get_mc_datasets_with_data_tier_string(mc_datasets, mc_string_function, data_tier):
  result_string = ''
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      if data_tier in mc_datasets[mc_dataset_name][year]:
        for path in mc_datasets[mc_dataset_name][year][data_tier]:
          path_info = mc_datasets[mc_dataset_name][year][data_tier][path]
          result_string += mc_string_function(mc_datasets, mc_dataset_name, year, data_tier, path)
  return result_string

# keys_mc_datasets = [ [mc_dataset_name, year, data_tier, search_string] ]
def get_missing_mc_datasets(keys_mc_datasets, mc_datasets):
  missing_mc_datasets = {}
  for keys_mc_dataset in keys_mc_datasets:
    #print('checking',keys_mc_dataset[0:3])
    if not nested_dict.is_nested_dict(mc_datasets, keys_mc_dataset[0:3]):
      #print(keys_mc_dataset)
      nested_dict.fill_empty_nested_dict(missing_mc_datasets, keys_mc_dataset[0:3])
  return missing_mc_datasets

# keys_data_datasets = [ [stream, year, run_group, data_tier, search_string] ]
def get_missing_data_datasets(keys_data_datasets, data_datasets):
  missing_data_datasets = {}
  for keys_data_dataset in keys_data_datasets:
    #print('checking',keys_data_dataset[0:3])
    if not nested_dict.is_nested_dict(data_datasets, keys_data_dataset[0:4]):
      #print(keys_data_dataset)
      nested_dict.fill_empty_nested_dict(missing_data_datasets, keys_data_dataset[0:4])
  return missing_data_datasets

# keys_mc_datasets = [ [mc_dataset_name, year, data_tier, search_string] ]
def get_missing_mc_datasets_with_data_tier(keys_mc_datasets, mc_datasets, data_tier):
  missing_mc_datasets = {}
  for keys_mc_dataset in keys_mc_datasets:
    #print('checking',keys_mc_dataset[0:3])
    keys = keys_mc_dataset[0:2]
    keys.append(data_tier)
    if not nested_dict.is_nested_dict(mc_datasets, keys):
      #print(keys_mc_dataset)
      nested_dict.fill_empty_nested_dict(missing_mc_datasets, keys)
  return missing_mc_datasets

# keys_data_datasets = [ [stream, year, run_group, data_tier, search_string] ]
def get_missing_data_datasets_with_data_tier(keys_data_datasets, data_datasets, data_tier):
  missing_data_datasets = {}
  for keys_data_dataset in keys_data_datasets:
    #print('checking',keys_data_dataset[0:3])
    keys = keys_data_dataset[0:3]
    keys.append(data_tier)
    if not nested_dict.is_nested_dict(data_datasets, keys):
      #print(keys_data_dataset)
      nested_dict.fill_empty_nested_dict(missing_data_datasets, keys)
  return missing_data_datasets

def get_incomplete_parent_mc_datasets(mc_datasets):
  incomplete_parent_mc_datasets = {}
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        for path in mc_datasets[mc_dataset_name][year][data_tier]:
          if path == None: continue
          path_info = mc_datasets[mc_dataset_name][year][data_tier][path]
          print('path_info', path, path_info)
          parent_chain = path_info['parent_chain']
          if not is_parent_chain_good(parent_chain, data_tier):
            nested_dict.fill_nested_dict(incomplete_parent_mc_datasets, [mc_dataset_name, year, data_tier, path], path_info)
  return incomplete_parent_mc_datasets 

def is_parent_chain_good(parent_chain, data_tier):
  if data_tier == 'miniaod':
    return len(parent_chain) == 3 or len(parent_chain) == 2
  elif data_tier == 'nanoaod':
    return len(parent_chain) == 4 or len(parent_chain) == 3

def convert_timestamp(timestamp):
  return datetime.datetime.fromtimestamp(timestamp)

def get_path_list_mc_datasets(mc_datasets):
  path_list = []
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        for path in mc_datasets[mc_dataset_name][year][data_tier]:
          path_list.append(path)
  return path_list

def get_path_list_data_datasets(data_datasets):
  path_list = []
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        for data_tier in data_datasets[stream][year][run_group]:
          for data_dataset in data_datasets[stream][year][run_group][data_tier]:
            path = data_dataset[0]
            if path == None: continue
            path_list.append(path)
  return path_list

def get_multiple_mc_datasets(mc_datasets):
  multiple_mc_datasets = {}
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        if len(mc_datasets[mc_dataset_name][year][data_tier]) > 1:
          for path in mc_datasets[mc_dataset_name][year][data_tier]:
            path_info = mc_datasets[mc_dataset_name][year][data_tier][path]
            nested_dict.fill_nested_dict(multiple_mc_datasets, [mc_dataset_name, year, data_tier, path], path_info)
  return multiple_mc_datasets

def get_path_to_keys_dataset_files_info(dataset_files_info):
  path_to_keys_dataset_files_info = {}
  for dataset in dataset_files_info:
    for path in dataset_files_info[dataset]:
      if path in path_to_keys_dataset_files_info:
        print('[Error] There is already a path: '+path+' in path_to_keys_dataset_files_info: '+str(path_to_keys_dataset_files_info[path]))
        continue
      path_to_keys_dataset_files_info[path] = [dataset, path]
  return path_to_keys_dataset_files_info

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


if __name__ == '__main__':
  datasets = {'test':{'somthing':1}, 'none':{'hello':{},'bye':{}}}
  nested_dict.remove_empty_nested_dict(datasets)
  print(datasets)


