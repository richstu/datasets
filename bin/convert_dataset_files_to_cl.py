#!/usr/bin/env python
import datasets
import sqlite3
import nested_dict
import os
import argparse
import os
import sys
import argparse_helper
import ask
import glob
import ROOT

def are_arguments_valid(args):
  # Will not check for mc_data_sig. Can't know what nanoaod_tag will be.
  #if not argparse_helper.is_valid(args, 'mc_data', ['mc', 'data']):
  #  return False, 'mc_data: '+str(args['mc_data'])+' is not valid.'

  if len(args['mc_data_sig']) != 1:
    return False, 'mc_data: '+str(args['mc_data_sig'])+' is not valid. Must choose only one.'

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

  if 'data' not in args['mc_data_sig']:
    t_path = os.path.join(args['meta_folder'],'mc_tag_meta')
    if not os.path.isfile(os.path.join(t_path)):
      return False, 'meta_mc_tag_meta: '+t_path+" doesn't exist."

  if 'data' in args['mc_data_sig']:
    t_path = os.path.join(args['meta_folder'],'data_tag_meta')
    if not os.path.isfile(os.path.join(t_path)):
      return False, 'meta_data_tag_meta: '+t_path+" doesn't exist."
 
  # Check if files exists with in_json_prefix
  if 'data' not in args['mc_data_sig']:
    t_path = os.path.join(args['in_json_folder'], args['in_datasets_prefix']+'mc_datasets.json')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

    t_path = os.path.join(args['in_json_folder'], args['in_files_prefix']+'mc_dataset_files_info.json')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

  if 'data' in args['mc_data_sig']:
    t_path = os.path.join(args['in_json_folder'], args['in_datasets_prefix']+'data_datasets.json')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

    t_path = os.path.join(args['in_json_folder'], args['in_files_prefix']+'data_dataset_files_info.json')
    if not os.path.isfile(t_path):
      return False, t_path+' does not exists.'

  ## Check if files exists 
  #if not os.path.isfile(args['in_dataset_files_info']):
  #  return False, args['in_dataset_files_info']+' does not exists.'

  if not os.path.isdir(args['target_base']):
    return False, args['target_base']+' does not exists.'

  # Check if files exists with out_jsons_prefix 
  if os.path.isfile(args['out_command_lines']):
    overwrite = ask.ask_key(args['out_command_lines']+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
    if overwrite == 'n':
      return False, args['out_command_lines']+' already exists.'

  return True, ''

def make_mc_database_tables(cursor):
  cursor.execute('CREATE TABLE mc_files(filename text PRIMARY KEY, path text NOT NULL, file_events integer, check_sum integer, modification_date integer, file_size integer);')
  #cursor.execute('SELECT * FROM mc_files')
  cursor.execute('CREATE TABLE mc_datasets(path text PRIMARY KEY, mc_dataset_name text NOT NULL, year integer, data_tier text NOT NULL, creation_time integer, size integer, files integer, events integer, lumis integer, mc_dir text NOT NULL);')
  #cursor.execute('SELECT * FROM mc_datasets')
  cursor.execute('CREATE TABLE mc_tags(year integer PRIMARY KEY, year_tag text NOT NULL, miniaod_tag text NOT NULL, nanoaod_tag text NOT NULL);')
  #cursor.execute('SELECT * FROM mc_tags')
  cursor.execute('CREATE TABLE mc_children(child_path text PRIMARY KEY, path text NOT NULL);')
  #cursor.execute('SELECT * FROM mc_children')
  cursor.execute('CREATE TABLE mc_parent(parent_path text PRIMARY KEY, path text NOT NULL);')
  #cursor.execute('SELECT * FROM mc_parent')
  #print([description[0] for description in cursor.description])

def make_data_database_tables(cursor):
  cursor.execute('CREATE TABLE data_files(filename text PRIMARY KEY, path text NOT NULL, file_events integer, check_sum integer, modification_date integer, file_size integer);')
  #cursor.execute('SELECT * FROM data_files')
  cursor.execute('CREATE TABLE data_datasets(path text PRIMARY KEY, stream text NOT NULL, year integer, run_group text NOT NULL, data_tier text NOT NULL, creation_time integer, size integer, files integer, events integer, lumis integer);')
  #cursor.execute('SELECT * FROM data_datasets')
  cursor.execute('CREATE TABLE data_tags(id integer PRIMARY KEY, stream text NOT NULL, year integer, run_group text NOT NULL, miniaod_tag text NOT NULL, nanoaod_tag text NOT NULL, nanoaodsim_tag text NOT NULL);')
  #cursor.execute('SELECT * FROM data_tags')
  cursor.execute('CREATE TABLE data_children(child_path text PRIMARY KEY, path text NOT NULL);')
  #cursor.execute('SELECT * FROM data_children')
  cursor.execute('CREATE TABLE data_parent(id integer PRIMARY KEY, parent_path text NOT NULL, path text NOT NULL);')
  #cursor.execute('SELECT * FROM data_parent')
  #print([description[0] for description in cursor.description])

def print_mc_database(cursor):
  cursor.execute('SELECT * FROM mc_files')
  print([description[0] for description in cursor.description])
  print(cursor.fetchall())
  cursor.execute('SELECT * FROM mc_datasets')
  print([description[0] for description in cursor.description])
  print(cursor.fetchall())
  cursor.execute('SELECT * FROM mc_tags')
  print([description[0] for description in cursor.description])
  print(cursor.fetchall())
  cursor.execute('SELECT * FROM mc_children')
  print([description[0] for description in cursor.description])
  print(cursor.fetchall())
  cursor.execute('SELECT * FROM mc_parent')
  print([description[0] for description in cursor.description])
  print(cursor.fetchall())

# datasets_files_info[path][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
def fill_mc_files_database(cursor, mc_dataset_files_info):
  for path in mc_dataset_files_info:
    for filename in mc_dataset_files_info[path]:
      file_events = mc_dataset_files_info[path][filename]['number_events']
      check_sum = mc_dataset_files_info[path][filename]['check_sum']
      modification_date = mc_dataset_files_info[path][filename]['modification_date']
      file_size = mc_dataset_files_info[path][filename]['file_size']
      cursor.execute('INSERT INTO mc_files (filename, path, file_events, check_sum, modification_date, file_size) VALUES (?, ?, ?, ?, ?, ?)', (filename, path, file_events, check_sum, modification_date, file_size))

# mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
# mc_dataset_names[year] = [(mc_dataset_name, mc_dir)]
def fill_mc_datasets_database(cursor, mc_datasets, mc_dataset_names):
  mc_dataset_names_dict = {}
  for year in mc_dataset_names:
    for info in mc_dataset_names[year]:
      mc_dataset_name = info[0]
      mc_dir = info[1]
      if year not in mc_dataset_names_dict:
        mc_dataset_names_dict[year] = {}
      if mc_dataset_name not in mc_dataset_names_dict[year]:
        mc_dataset_names_dict[year][mc_dataset_name] = mc_dir

  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        for path in mc_datasets[mc_dataset_name][year][data_tier]:
          mc_dir = mc_dataset_names_dict[year][mc_dataset_name]
          creation_time = mc_datasets[mc_dataset_name][year][data_tier][path]['creation_time']
          size = mc_datasets[mc_dataset_name][year][data_tier][path]['data_size']
          files = mc_datasets[mc_dataset_name][year][data_tier][path]['number_files']
          events = mc_datasets[mc_dataset_name][year][data_tier][path]['number_events']
          lumis = mc_datasets[mc_dataset_name][year][data_tier][path]['number_lumis']
          cursor.execute('INSERT INTO mc_datasets (path, mc_dataset_name, year, data_tier, creation_time, size, files, events, lumis, mc_dir) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',(path, mc_dataset_name, year, data_tier, creation_time, size, files, events, lumis, mc_dir))

# Ex) tag_meta[2016] = RunIISummer16, MiniAODv3, NanoAODv5
def fill_mc_tags_database(cursor, mc_tag_meta):
  for year in mc_tag_meta:
    year_tag = mc_tag_meta[year][0]
    miniaod_tag = mc_tag_meta[year][1]
    nanoaod_tag = mc_tag_meta[year][2]
    cursor.execute('INSERT INTO mc_tags (year, year_tag, miniaod_tag, nanoaod_tag) VALUES (?, ?, ?, ?)',(year, year_tag, miniaod_tag, nanoaod_tag))

# mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
def fill_mc_children_database(cursor, mc_datasets):
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        for path in mc_datasets[mc_dataset_name][year][data_tier]:
          for child_path in mc_datasets[mc_dataset_name][year][data_tier][path]['children']:
            cursor.execute('INSERT INTO mc_children (child_path, path) VALUES (?, ?)',(child_path, path))

# mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
def fill_mc_parent_database(cursor, mc_datasets):
  for mc_dataset_name in mc_datasets:
    for year in mc_datasets[mc_dataset_name]:
      for data_tier in mc_datasets[mc_dataset_name][year]:
        for path in mc_datasets[mc_dataset_name][year][data_tier]:
          previous_path = path
          for parent_path in mc_datasets[mc_dataset_name][year][data_tier][path]['parent_chain']:
            #print(path, parent_path, previous_path)
            cursor.execute('INSERT INTO mc_parent (parent_path, path) VALUES (?, ?)',(parent_path, previous_path))
            previous_path = parent_path

def print_data_database(cursor):
  cursor.execute('SELECT * FROM data_files')
  print([description[0] for description in cursor.description])
  print(cursor.fetchall())
  cursor.execute('SELECT * FROM data_datasets')
  print([description[0] for description in cursor.description])
  print(cursor.fetchall())
  cursor.execute('SELECT * FROM data_tags')
  print([description[0] for description in cursor.description])
  print(cursor.fetchall())
  cursor.execute('SELECT * FROM data_children')
  print([description[0] for description in cursor.description])
  print(cursor.fetchall())
  cursor.execute('SELECT * FROM data_parent')
  print([description[0] for description in cursor.description])
  print(cursor.fetchall())

# datasets_files_info[path][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
def fill_data_files_database(cursor, data_dataset_files_info):
  for path in data_dataset_files_info:
    for filename in data_dataset_files_info[path]:
      file_events = data_dataset_files_info[path][filename]['number_events']
      check_sum = data_dataset_files_info[path][filename]['check_sum']
      modification_date = data_dataset_files_info[path][filename]['modification_date']
      file_size = data_dataset_files_info[path][filename]['file_size']
      cursor.execute('INSERT INTO data_files (filename, path, file_events, check_sum, modification_date, file_size) VALUES (?, ?, ?, ?, ?, ?)', (filename, path, file_events, check_sum, modification_date, file_size))

# data_dataset[stream][year][run_group][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
def fill_data_datasets_database(cursor, data_datasets):
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        for data_tier in data_datasets[stream][year][run_group]:
          for path in data_datasets[stream][year][run_group][data_tier]:
            creation_time = data_datasets[stream][year][run_group][data_tier][path]['creation_time']
            size = data_datasets[stream][year][run_group][data_tier][path]['data_size']
            files = data_datasets[stream][year][run_group][data_tier][path]['number_files']
            events = data_datasets[stream][year][run_group][data_tier][path]['number_events']
            lumis = data_datasets[stream][year][run_group][data_tier][path]['number_lumis']
            cursor.execute('INSERT INTO data_datasets (path, stream, year, run_group, data_tier, creation_time, size, files, events, lumis) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',(path, stream, year, run_group, data_tier, creation_time, size, files, events, lumis))

# data_tag_meta[year][run_group][streams][data_tier] = reco_tag
def fill_data_tags_database(cursor, data_tag_meta):
  index = -1
  for year in data_tag_meta:
    for run_group in data_tag_meta[year]:
      for stream in data_tag_meta[year][run_group]:
        miniaod_tag = data_tag_meta[year][run_group][stream]['miniaod']
        nanoaod_tag = data_tag_meta[year][run_group][stream]['nanoaod']
        nanoaodsim_tag = data_tag_meta[year][run_group][stream]['nanoaodsim']
        index += 1
        cursor.execute('INSERT INTO data_tags (id, stream, year, run_group, miniaod_tag, nanoaod_tag, nanoaodsim_tag) VALUES (?, ?, ?, ?, ?, ?, ?)',(index, stream, year, run_group, miniaod_tag, nanoaod_tag, nanoaodsim_tag))

# data_dataset[stream][year][run_group][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
def fill_data_children_database(cursor, data_datasets):
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        for data_tier in data_datasets[stream][year][run_group]:
          for path in data_datasets[stream][year][run_group][data_tier]:
             for child_path in data_datasets[stream][year][run_group][data_tier][path]['children']:
               cursor.execute('INSERT INTO data_children (child_path, path) VALUES (?, ?)',(child_path, path))

# data_dataset[stream][year][run_group][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
def fill_data_parent_database(cursor, data_datasets):
  index = -1
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        for data_tier in data_datasets[stream][year][run_group]:
          for path in data_datasets[stream][year][run_group][data_tier]:
            previous_path = path
            for parent_path in data_datasets[stream][year][run_group][data_tier][path]['parent_chain']:
              index += 1
              cursor.execute('INSERT INTO data_parent (parent_path, path) VALUES (?, ?)',(parent_path, previous_path))
              previous_path = parent_path

## file_mc: NanoAODv5/Nano/2016/mc/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8__RunIISummer16NanoAODv5__PUMoriond17_Nano1June2019_102X_mcRun2_asymptotic_v7-v1__250000__37FA68CC-B841-7D41-994C-645CFA4BA227.root
## parsed_mc_filename: ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8__RunIISummer16NanoAODv5__PUMoriond17_Nano1June2019_102X_mcRun2_asymptotic_v7-v1__250000__37FA68CC-B841-7D41-994C-645CFA4BA227.root
## mc_filename: /store/mc/RunIISummer16NanoAODv5/WZTo3LNu_TuneCUETP8M1_13TeV-powheg-pythia8/NANOAODSIM/PUMoriond17_Nano1June2019_102X_mcRun2_asymptotic_v7-v1/120000/222071C0-CF04-1E4B-B65E-49D18B91DE8B.root
## file_nanomc_info = (mc_filename, year, nanoaod_tag, file_events, mc_dir)
#def get_file_nanomc_info(base_dir, mc_disk_filename_with_base):
#  mc_disk_filename = mc_disk_filename_with_base[len(base_dir)+1:]
#  mc_disk_filename_split = mc_disk_filename.split('/')
#  nanoaod_tag = mc_disk_filename_split[0]
#  year = int(mc_disk_filename_split[2])
#  mc_dir = mc_disk_filename_split[3]
#  parsed_mc_filename = mc_disk_filename_split[4]
#  parsed_mc_filename_split = parsed_mc_filename.split('__')
#  mc_filename = '/store/mc/'+parsed_mc_filename_split[1]+'/'+parsed_mc_filename_split[0]+'/NANOAODSIM/'+parsed_mc_filename_split[2]+'/'+parsed_mc_filename_split[3]+'/'+parsed_mc_filename_split[4]
#  file_events = -1
#  try:
#    root_file = ROOT.TFile.Open(mc_disk_filename_with_base)
#    root_tree = root_file.Get('Events')
#    file_events = root_tree.GetEntries()
#  except:
#    print('Failed to get events for '+mc_disk_filename_with_base)
#  return mc_filename, year, nanoaod_tag, file_events, mc_dir

def make_mc_disk_database(cursor):
  cursor.execute('CREATE TABLE mc_disk (filename text PRIMARY KEY, data_tier text NO NULL, year integer, aod_tag text NO NULL, file_events integer, mc_dir text NO NULL);')

def make_data_disk_database(cursor):
  cursor.execute('CREATE TABLE data_disk (filename text PRIMARY KEY, data_tier text NO NULL, year integer, aod_tag text NO NULL, file_events integer, data_dir text NO NULL);')

## mc_disk_files[nanoaod_tag][year][mc_dir][filename] = {'file_events': int}
#def fill_mc_disk_files(mc_disk_files, filename, year, nanoaod_tag, file_events, mc_dir):
#  if nanoaod_tag not in mc_disk_files:
#    mc_disk_files[nanoaod_tag] = {}
#  if year not in mc_disk_files[nanoaod_tag]:
#    mc_disk_files[nanoaod_tag][year] = {}
#  if mc_dir not in mc_disk_files[nanoaod_tag][year]:
#    mc_disk_files[nanoaod_tag][year][mc_dir] = {}
#  if filename not in mc_disk_files[nanoaod_tag][year][mc_dir]:
#    mc_disk_files[nanoaod_tag][year][mc_dir][filename] = {'file_events': file_events}
#  else:
#    print('[Warning] mc_disk_files['+nanoaod_tag+']['+str(year)+']['+mc_dir+']['+filename+'] already exists')
#
## mc_disk_files[nanoaod_tag][year][mc_dir][filename] = {'file_events': int}
#def make_mc_disk_files(base_dir):
#  #mc_files = glob.glob(base_dir+'/*/Nano/*/[!data]*/*.root')
#  mc_disk_filenames = glob.glob(base_dir+'/*/Nano/*/mc/*.root')
#  mc_disk_files = {}
#  for mc_disk_filename in mc_disk_filenames:
#    # mc_disk_files[nanoaod_tag][year][mc_dir][filename] = {'file_events': int}
#    mc_filename, year, nanoaod_tag, file_events, mc_dir = get_file_nanomc_info(base_dir, mc_disk_filename)
#    fill_mc_disk_files(mc_disk_files, mc_filename, year, nanoaod_tag, file_events, mc_dir)
#  return mc_disk_files

#def fill_nano_mc_disk_database(base_dir, cursor):
#  #mc_files = glob.glob(base_dir+'/*/Nano/*/[!data]*/*.root')
#  mc_disk_filenames = glob.glob(base_dir+'/*/Nano/*/mc/*.root')
#  mc_disk_files = {}
#  for mc_disk_filename in mc_disk_filenames:
#    mc_filename, year, nanoaod_tag, file_events, mc_dir = get_file_nanomc_info(base_dir, mc_disk_filename)
#    cursor.execute('INSERT INTO mc_disk (filename, year, nanoaod_tag, file_events, mc_dir) VALUES (?, ?, ?, ?, ?)', (mc_filename, year, nanoaod_tag, file_events, mc_dir))

# mc_disk_files[data_tier][aod_tag][year][mc_dir][filename] = {'file_events': int}
def fill_mc_disk_database(cursor, mc_disk_files):
  #print(mc_disk_files)
  for data_tier in mc_disk_files:
    for aod_tag in mc_disk_files[data_tier]:
      for year in mc_disk_files[data_tier][aod_tag]:
       for mc_dir in mc_disk_files[data_tier][aod_tag][year]:
         for filename in mc_disk_files[data_tier][aod_tag][year][mc_dir]:
           file_events = mc_disk_files[data_tier][aod_tag][year][mc_dir][filename]['file_events']
           cursor.execute('INSERT INTO mc_disk (filename, data_tier, year, aod_tag, file_events, mc_dir) VALUES (?, ?, ?, ?, ?, ?)', (filename, data_tier, year, aod_tag, file_events, mc_dir))

# data_disk_files[data_tier][aod_tag][year][data_dir][filename] = {'file_events': int}
def fill_data_disk_database(cursor, data_disk_files):
  #print(data_disk_files)
  for data_tier in data_disk_files:
    for aod_tag in data_disk_files[data_tier]:
      for year in data_disk_files[data_tier][aod_tag]:
       for data_dir in data_disk_files[data_tier][aod_tag][year]:
         for filename in data_disk_files[data_tier][aod_tag][year][data_dir]:
           file_events = data_disk_files[data_tier][aod_tag][year][data_dir][filename]['file_events']
           cursor.execute('INSERT INTO data_disk (filename, data_tier, year, aod_tag, file_events, data_dir) VALUES (?, ?, ?, ?, ?, ?)', (filename, data_tier, year, aod_tag, file_events, data_dir))

def print_mc_disk_database(cursor):
  cursor.execute('SELECT * FROM mc_disk')
  print([description[0] for description in cursor.description])
  print(cursor.fetchall())

def print_data_disk_database(cursor):
  cursor.execute('SELECT * FROM data_disk')
  print([description[0] for description in cursor.description])
  print(cursor.fetchall())


if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Makes command lines from mc_dataset_files_info.')
  #parser.add_argument('in_dataset_files_info', metavar='./jsons/DATASET_FILES_INFO.json')
  parser.add_argument('mc_data_sig', metavar='"mc,data,sig"', nargs=1, default=['mc'])
  parser.add_argument('target_base', metavar='/mnt/hadoop/pico')
  parser.add_argument('out_command_lines', metavar='./results/CL_DATASET_FILES_INFO.py')
  parser.add_argument('-m', '--meta_folder', metavar='./meta', nargs=1, default=['./meta'])
  parser.add_argument('-i', '--in_json_folder', metavar='./jsons', nargs=1, default=['./jsons'])
  parser.add_argument('-if', '--in_files_prefix', metavar='', nargs=1, default=[''])
  parser.add_argument('-id', '--in_datasets_prefix', metavar='selected_', nargs=1, default=['selected_'])
  parser.add_argument('-ik', '--in_disk_prefix', metavar="''", nargs=1, default=[''])
  parser.add_argument('-s', '--sql_search', metavar="''", nargs=1, default=[''])

  args = vars(parser.parse_args())
  argparse_helper.initialize_arguments(args, list_args=['mc_data_sig'])
  valid, log = are_arguments_valid(args)
  if not valid:
    print('[Error] '+log)
    sys.exit()

  # Set filenames
  #data_tiers = ['nanoaod', 'miniaod']
  data_tiers = ['nanoaod']

  meta_folder = args['meta_folder']
  mc_dataset_common_names_filename = meta_folder+'/mc_dataset_common_names'
  mc_dataset_2016_names_filename = meta_folder+'/mc_dataset_2016_names'
  mc_dataset_2016APV_names_filename = meta_folder+'/mc_dataset_2016APV_names'
  mc_dataset_2017_names_filename = meta_folder+'/mc_dataset_2017_names'
  mc_dataset_2018_names_filename = meta_folder+'/mc_dataset_2018_names'
  mc_tag_meta_filename = meta_folder+'/mc_tag_meta'
  data_tag_meta_filename = meta_folder+'/data_tag_meta'

  mc_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_files_prefix']+'mc_dataset_files_info.json')
  data_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_files_prefix']+'data_dataset_files_info.json')

  mc_datasets_filename = os.path.join(args['in_json_folder'], args['in_datasets_prefix']+'mc_datasets.json')
  data_datasets_filename = os.path.join(args['in_json_folder'], args['in_datasets_prefix']+'data_datasets.json')
  #mc_datasets_filename = os.path.join(args['in_json_folder'],args['in_json_prefix']+'mc_datasets.json')

  mc_disk_files_filename = os.path.join(args['in_json_folder'], args['in_disk_prefix']+'mc_disk_files.json')
  data_disk_files_filename = os.path.join(args['in_json_folder'], args['in_disk_prefix']+'data_disk_files.json')

  #data_datasets_filename = os.path.join(args['in_json_folder'],args['in_json_prefix']+'data_datasets.json')
  do_mc = False
  if 'mc' in args['mc_data_sig']: do_mc = True
  do_data = False
  if 'data' in args['mc_data_sig']: do_data = True
  do_signal = False
  if ('data' not in args['mc_data_sig']) and ('mc' not in args['mc_data_sig']): do_signal = True

  dataset_files_info_filename = ''
  mid_folder = ''
  #search_term = ""
  #search_term = "files<10"
  search_term = args['sql_search']

  database = sqlite3.connect(':memory:')
  cursor = database.cursor()

  files_to_download = []
  files_to_remove = []

  if do_mc or do_signal:
    dataset_files_info_filename = mc_dataset_files_info_filename
    # Make database
    # Load files
    # datasets_files_info[dataset][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
    mc_dataset_files_info = nested_dict.load_json_file(mc_dataset_files_info_filename)

    # mc_dataset_names[year] = [(mc_dataset_name, mc_dir)]
    mc_dataset_names = datasets.parse_multiple_mc_dataset_names([
      [mc_dataset_common_names_filename, ['2016', '2016APV', '2017', '2018']],
      [mc_dataset_2016_names_filename, ['2016']],
      [mc_dataset_2016APV_names_filename, ['2016APV']],
      [mc_dataset_2017_names_filename, ['2017']],
      [mc_dataset_2018_names_filename, ['2018']],
      ])
    #print ('dataset_names:', mc_dataset_names)
    # Ex) tag_meta[2016] = RunIISummer16, MiniAODv3, NanoAODv5
    mc_tag_meta = datasets.parse_mc_tag_meta(mc_tag_meta_filename)

    # mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
    mc_datasets = nested_dict.load_json_file(mc_datasets_filename)

    make_mc_database_tables(cursor)
    fill_mc_files_database(cursor, mc_dataset_files_info)
    fill_mc_datasets_database(cursor, mc_datasets, mc_dataset_names)
    fill_mc_tags_database(cursor, mc_tag_meta)
    fill_mc_children_database(cursor, mc_datasets)
    fill_mc_parent_database(cursor, mc_datasets)

    ## Search database
    ##print_mc_database(cursor)
    #mc_files = []
    #try:
    #  cursor.execute('SELECT filename, mc_files.path AS path, file_events, file_size, mc_datasets.mc_dataset_name AS mc_dataset_name, mc_datasets.year AS year, mc_datasets.data_tier AS data_tier, mc_datasets.size AS size, mc_datasets.files AS files, mc_datasets.events AS events, mc_datasets.lumis AS lumis, mc_datasets.mc_dir AS mc_dir FROM mc_files INNER JOIN mc_datasets ON mc_datasets.path = mc_files.path '+sql_search_term+';')
    #  #print(cursor.fetchall())
    #  mc_files = [x[0] for x in cursor.fetchall()]
    #except sqlite3.OperationalError:
    #  pass

    # Make file_mc_database
    # mc_disk_files[data_tier][aod_tag][year][mc_dir][filename] = {'file_events': int}
    make_mc_disk_database(cursor)
    if os.path.isfile(mc_disk_files_filename):
      mc_disk_files = nested_dict.load_json_file(mc_disk_files_filename)
      fill_mc_disk_database(cursor, mc_disk_files)
      #print_mc_disk_database(cursor)

    # Find files to download
    #cursor.execute('SELECT filename, file_events FROM mc_files '+sql_search_term+';')
    sql_search_term = 'WHERE mc_dir = "'+args['mc_data_sig'][0]+'"'
    if search_term != '':
      sql_search_term += ' AND '+search_term
    #cursor.execute('SELECT filename, mc_files.path AS path, file_events, file_size, mc_datasets.mc_dataset_name AS mc_dataset_name, mc_datasets.year AS year, mc_datasets.data_tier AS data_tier, mc_datasets.size AS size, mc_datasets.files AS files, mc_datasets.events AS events, mc_datasets.lumis AS lumis, mc_datasets.mc_dir AS mc_dir, mc_tags.year_tag AS year_tag, mc_tags.miniaod_tag AS miniaod_tag, mc_tags.nanoaod_tag AS nanoaod_tag FROM mc_files INNER JOIN mc_datasets ON mc_datasets.path = mc_files.path INNER JOIN mc_tags ON mc_tags.year = mc_datasets.year  '+sql_search_term+';')
    cursor.execute('SELECT filename, mc_files.path AS dataset_path, file_events, file_size, mc_datasets.mc_dataset_name AS mc_dataset_name, mc_datasets.year AS dataset_year, mc_datasets.data_tier AS data_tier, mc_datasets.size AS size, mc_datasets.files AS files, mc_datasets.events AS events, mc_datasets.lumis AS lumis, mc_datasets.mc_dir AS mc_dir, mc_tags.year_tag AS year_tag, mc_tags.miniaod_tag AS miniaod_tag, mc_tags.nanoaod_tag AS nanoaod_tag FROM mc_files INNER JOIN mc_datasets ON mc_datasets.path = mc_files.path INNER JOIN mc_tags ON mc_tags.year = mc_datasets.year  '+sql_search_term+';')
    #cursor.execute('SELECT filename, mc_files.path, file_events, file_size, mc_datasets.mc_dataset_name, mc_datasets.year, mc_datasets.data_tier, mc_datasets.size, mc_datasets.files, mc_datasets.events, mc_datasets.lumis, mc_datasets.mc_dir, mc_tags.year_tag, mc_tags.miniaod_tag, mc_tags.nanoaod_tag FROM mc_files INNER JOIN mc_datasets ON mc_datasets.path = mc_files.path INNER JOIN mc_tags ON mc_tags.year = mc_datasets.year  '+sql_search_term+';')
    # target_file_info[filename] = (file_events, mid_folder)
    target_file_info = {}
    for file_info in cursor.fetchall():
      filename = file_info[0]
      file_events = file_info[2]
      year = file_info[5]
      nanoaod_tag = file_info[14]
      target_file_info[filename] = file_events
      if mid_folder == "": mid_folder = nanoaod_tag+'/nano/'+str(year)+'/'+args['mc_data_sig'][0]
      else: 
        if mid_folder != nanoaod_tag+'/nano/'+str(year)+'/'+args['mc_data_sig'][0]:
          print('mid_folder: '+mid_folder+' is different with '+nanoaod_tag+'/nano/'+str(year)+'/'+args['mc_data_sig'][0])
    #print(target_file_info)

    # Find existing files
    cursor.execute('SELECT filename, file_events FROM mc_disk;')
    disk_file_info = {}
    for filename, file_events in cursor.fetchall():
      disk_file_info[filename] = file_events

    ## Make list of files to download
    ##print(disk_file_info)
    #for filename in target_file_info:
    #  if filename not in disk_file_info: files_to_download.append(filename)
    #  else:
    #    if target_file_info[filename][0] != disk_file_info[filename]:
    #      print('Events for '+filename+' is different. target:'+target_file_info[filename]+ ' disk:'+disk_file_info[filename]+'. Adding to download list and remove list.')
    #      files_to_download.append(filename)
    #      files_to_remove.append(filename)
    #print('files_to_download',str(files_to_download))
    #print('files_to_remove', str(files_to_remove))

  if do_data:
    dataset_files_info_filename = data_dataset_files_info_filename
    # Make database
    data_dataset_files_info = nested_dict.load_json_file(data_dataset_files_info_filename)
    # data_tag_meta[year][run_group][streams][data_tier] = reco_tag
    data_tag_meta = datasets.parse_data_tag_meta(data_tag_meta_filename)

    # data_dataset[stream][year][run_group][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
    data_datasets = nested_dict.load_json_file(data_datasets_filename)

    make_data_database_tables(cursor)
    fill_data_files_database(cursor, data_dataset_files_info)
    fill_data_datasets_database(cursor, data_datasets)
    fill_data_tags_database(cursor, data_tag_meta)
    fill_data_children_database(cursor, data_datasets)
    fill_data_parent_database(cursor, data_datasets)

    ## Search database
    ##print_data_database(cursor)
    #data_files = []
    #try:
    #  cursor.execute('SELECT filename, data_files.path AS path, file_events, file_size, data_datasets.stream AS stream, data_datasets.year AS year, data_datasets.run_group AS run_group, data_datasets.data_tier AS data_tier, data_datasets.size AS size, data_datasets.files AS files, data_datasets.events AS events, data_datasets.lumis as lumis FROM data_files INNER JOIN data_datasets ON data_datasets.path = data_files.path '+sql_search_term+';')
    #  #print(cursor.fetchall())
    #  data_files = [x[0] for x in cursor.fetchall()]
    #except sqlite3.OperationalError:
    #  pass
    ##print(data_files)

    # Make file_data_database
    # data_disk_files[data_tier][aod_tag][year][data_dir][filename] = {'file_events': int}
    make_data_disk_database(cursor)
    if os.path.isfile(data_disk_files_filename):
      data_disk_files = nested_dict.load_json_file(data_disk_files_filename)
      fill_data_disk_database(cursor, data_disk_files)
      #print_data_disk_database(cursor)

    # Find files to download
    sql_search_term = '' if search_term == '' else 'WHERE '+search_term
    cursor.execute('SELECT filename, data_files.path AS dataset_path, file_events, file_size, data_datasets.stream AS dataset_stream, data_datasets.year AS dataset_year, data_datasets.run_group AS dataset_run_group, data_datasets.data_tier AS data_tier, data_datasets.size AS size, data_datasets.files AS files, data_datasets.events AS events, data_datasets.lumis AS lumis, data_tags.miniaod_tag AS miniaod_tag, data_tags.nanoaod_tag AS nanoaod_tag, data_tags.nanoaodsim_tag AS nanoaodsim_tag FROM data_files INNER JOIN data_datasets ON data_datasets.path = data_files.path INNER JOIN data_tags ON data_tags.year = data_datasets.year AND data_tags.run_group = data_datasets.run_group AND data_tags.stream = data_datasets.stream '+sql_search_term+';')
    #cursor.execute('SELECT filename, file_events FROM data_files;')
    target_file_info = {}
    for file_info in cursor.fetchall():
      filename = file_info[0]
      file_events = file_info[2]
      year = file_info[5]
      nanoaod_tag = file_info[14]
      target_file_info[filename] = file_events
      if mid_folder == "": mid_folder = nanoaod_tag+'/nano/'+str(year)+'/'+args['mc_data_sig'][0]
      else: 
        if mid_folder != nanoaod_tag+'/nano/'+str(year)+'/'+args['mc_data_sig'][0]:
          print('mid_folder: '+mid_folder+' is different with '+nanoaod_tag+'/nano/'+str(year)+'/'+args['mc_data_sig'][0])
    #print(target_file_info)

    # Find existing files
    cursor.execute('SELECT filename, file_events FROM data_disk;')
    disk_file_info = {}
    for filename, file_events in cursor.fetchall():
      disk_file_info[filename] = file_events

  # Make list of files to download
  for filename in target_file_info:
    if filename not in disk_file_info: files_to_download.append(filename)
    else:
      if target_file_info[filename] != disk_file_info[filename]:
        print('Events for '+filename+' is different. target:'+target_file_info[filename]+ ' disk:'+disk_file_info[filename]+'. Adding to download list and remove list.')
        files_to_download.append(filename)
        files_to_remove.append(filename)

  #print('Files to download')
  #for x in files_to_download: print('  '+x)
  if len(files_to_remove) != 0:
    print('Files_to_remove')
    for x in files_to_remove: print('  '+x)

  #for filename in target_file_info:
  #  files_to_download.append(filename)

  # Make script for downloading
  target_folder = os.path.join(args['target_base'], mid_folder)
  if not os.path.exists(target_folder):
    make_dir = ask.ask_key('Should directory:'+target_folder+' be made? (y/n) Default is y.', ['y','n'], 'n')
    if make_dir == 'y': os.makedirs(target_folder)

  command_list_filename = args['out_command_lines']

  command_list_string = ''
  command_list_string = ''
  command_list_string += '#!/bin/env python\n'
  #command_list_string += "base_command = './run_scripts/copy_aods.py'\n"
  command_list_string += "base_command = '"+os.environ['JB_DATASETS_DIR']+"/bin/copy_aods.py'\n"
  command_list_string += "base_folder = '"+args['target_base']+"/'\n"
  command_list_string += "mid_folder = '"+mid_folder+"'\n"
  command_list_string += "print('# [global_key] dataset_files_info_filename : "+dataset_files_info_filename+"')\n"


  #for dataset in dataset_files_info:
  #  for filename in dataset_files_info[dataset]:
  #    command_list_string += "print(base_command+' "+filename+" '+base_folder+mid_folder)\n"
  for filename in files_to_download:
    command_list_string += "print(base_command+' "+filename+" '+base_folder+mid_folder)\n"

  if len(files_to_remove) != 0:
    print('These files should be removed: '+str(files_to_remove))


  with open(command_list_filename,'w') as command_list_file:
    command_list_file.write(command_list_string)
    print('Wrote to '+command_list_filename)
    os.system('chmod +x '+command_list_filename)

