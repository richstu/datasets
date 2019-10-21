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
  cursor.execute('CREATE TABLE data_tags(id integer PRIMARY KEY, stream text NOT NULL, year integer, run_group text NOT NULL, miniaod_tag text NOT NULL, nanoaod_tag text NOT NULL);')
  #cursor.execute('SELECT * FROM data_tags')
  cursor.execute('CREATE TABLE data_children(child_path text PRIMARY KEY, path text NOT NULL);')
  #cursor.execute('SELECT * FROM data_children')
  cursor.execute('CREATE TABLE data_parent(parent_path text PRIMARY KEY, path text NOT NULL);')
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
        index += 1
        cursor.execute('INSERT INTO data_tags (id, stream, year, run_group, miniaod_tag, nanoaod_tag) VALUES (?, ?, ?, ?, ?, ?)',(index, stream, year, run_group, miniaod_tag, nanoaod_tag))

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
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        for data_tier in data_datasets[stream][year][run_group]:
          for path in data_datasets[stream][year][run_group][data_tier]:
            previous_path = path
            for parent_path in data_datasets[stream][year][run_group][data_tier][path]['parent_chain']:
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
  print(mc_disk_files)
  for data_tier in mc_disk_files:
    for aod_tag in mc_disk_files[data_tier]:
      for year in mc_disk_files[data_tier][aod_tag]:
       for mc_dir in mc_disk_files[data_tier][aod_tag][year]:
         for filename in mc_disk_files[data_tier][aod_tag][year][mc_dir]:
           file_events = mc_disk_files[data_tier][aod_tag][year][mc_dir][filename]['file_events']
           cursor.execute('INSERT INTO mc_disk (filename, data_tier, year, aod_tag, file_events, mc_dir) VALUES (?, ?, ?, ?, ?, ?)', (filename, data_tier, year, aod_tag, file_events, mc_dir))

def print_mc_disk_database(cursor):
  cursor.execute('SELECT * FROM mc_disk')
  print([description[0] for description in cursor.description])
  print(cursor.fetchall())

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

  # Set filenames
  #data_tiers = ['nanoaod', 'miniaod']
  data_tiers = ['nanoaod']

  mc_dataset_files_info_filename = './jsons/mc_dataset_files_info.json'
  data_dataset_files_info_filename = './jsons/data_dataset_files_info.json'

  meta_folder = './meta'
  mc_dataset_common_names_filename = meta_folder+'/mc_dataset_common_names'
  mc_dataset_2016_names_filename = meta_folder+'/mc_dataset_2016_names'
  mc_dataset_2017_names_filename = meta_folder+'/mc_dataset_2017_names'
  mc_dataset_2018_names_filename = meta_folder+'/mc_dataset_2018_names'
  mc_tag_meta_filename = meta_folder+'/mc_tag_meta'
  data_tag_meta_filename = meta_folder+'/data_tag_meta'

  mc_datasets_filename = './jsons/selected_mc_datasets.json'
  data_datasets_filename = './jsons/selected_data_datasets.json'
  #mc_datasets_filename = os.path.join(args['in_json_folder'],args['in_json_prefix']+'mc_datasets.json')

  #data_datasets_filename = os.path.join(args['in_json_folder'],args['in_json_prefix']+'data_datasets.json')

  do_mc = True
  do_data = False

  #search_term = ""
  search_term = "files<10"
  sql_search_term = '' if search_term == '' else 'WHERE '+search_term

  database = sqlite3.connect(':memory:')
  cursor = database.cursor()

  if do_mc:
    # Make database
    # Load files
    # datasets_files_info[dataset][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
    mc_dataset_files_info = nested_dict.load_json_file(mc_dataset_files_info_filename)
    data_dataset_files_info = nested_dict.load_json_file(data_dataset_files_info_filename)

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
    # keys_mc_datasets = [ [mc_dataset_name, year, data_tier, search_string] ]
    keys_mc_datasets = datasets.get_keys_mc_datasets(mc_dataset_names, mc_tag_meta, data_tiers)

    # mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
    mc_datasets = nested_dict.load_json_file(mc_datasets_filename)

    make_mc_database_tables(cursor)
    fill_mc_files_database(cursor, mc_dataset_files_info)
    fill_mc_datasets_database(cursor, mc_datasets, mc_dataset_names)
    fill_mc_tags_database(cursor, mc_tag_meta)
    fill_mc_children_database(cursor, mc_datasets)
    fill_mc_parent_database(cursor, mc_datasets)

    # Search database
    #print_mc_database(cursor)
    mc_files = []
    try:
      cursor.execute('SELECT filename, mc_files.path AS path, file_events, file_size, mc_datasets.mc_dataset_name AS mc_dataset_name, mc_datasets.year AS year, mc_datasets.data_tier AS data_tier, mc_datasets.size AS size, mc_datasets.files AS files, mc_datasets.events AS events, mc_datasets.lumis AS lumis, mc_datasets.mc_dir AS mc_dir FROM mc_files INNER JOIN mc_datasets ON mc_datasets.path = mc_files.path '+sql_search_term+';')
      #print(cursor.fetchall())
      mc_files = [x[0] for x in cursor.fetchall()]
    except sqlite3.OperationalError:
      pass
    #print(mc_files)

    ##base_dir = '/mnt/hadoop/pico'
    #base_dir = './test'
    #mc_disk_files_filename = './jsons/mc_disk_files.json'
    ## mc_disk_files[nanoaod_tag][year][mc_dir][filename] = {'file_events': int}
    #mc_disk_files = make_mc_disk_files(base_dir)
    #nested_dict.save_json_file(mc_disk_files, mc_disk_files_filename)
    ### Make file_mc_database
    ##make_mc_disk_database(cursor)
    ##base_dir = '/mnt/hadoop/pico'
    ###base_dir = './test'
    ##fill_nano_mc_disk_database(base_dir, cursor)
    ##print_mc_disk_database(cursor)

    # Make file_mc_database
    mc_disk_files_filename = './jsons/mc_disk_files.json'
    # mc_disk_files[data_tier][aod_tag][year][mc_dir][filename] = {'file_events': int}
    mc_disk_files = nested_dict.load_json_file(mc_disk_files_filename)
    make_mc_disk_database(cursor)
    fill_mc_disk_database(cursor, mc_disk_files)
    print_mc_disk_database(cursor)

  if do_data:
    # Make database
    # data_tag_meta[year][run_group][streams][data_tier] = reco_tag
    data_tag_meta = datasets.parse_data_tag_meta(data_tag_meta_filename)
    # keys_data_datasets = [ [stream, year, run_group, data_tier, search_string] ]
    keys_data_datasets = datasets.get_keys_data_datasets(data_tag_meta, data_tiers)

    # data_dataset[stream][year][run_group][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
    data_datasets = nested_dict.load_json_file(data_datasets_filename)

    make_data_database_tables(cursor)
    fill_data_files_database(cursor, data_dataset_files_info)
    fill_data_datasets_database(cursor, data_datasets)
    fill_data_tags_database(cursor, data_tag_meta)
    fill_data_children_database(cursor, data_datasets)
    fill_data_parent_database(cursor, data_datasets)

    # Search database
    #print_data_database(cursor)
    data_files = []
    try:
      cursor.execute('SELECT filename, data_files.path AS path, file_events, file_size, data_datasets.stream AS stream, data_datasets.year AS year, data_datasets.run_group AS run_group, data_datasets.data_tier AS data_tier, data_datasets.size AS size, data_datasets.files AS files, data_datasets.events AS events, data_datasets.lumis as lumis FROM data_files INNER JOIN data_datasets ON data_datasets.path = data_files.path '+sql_search_term+';')
      #print(cursor.fetchall())
      data_files = [x[0] for x in cursor.fetchall()]
    except sqlite3.OperationalError:
      pass
    print(data_files)


  sys.exit()

  command_list_filename = args['out_command_lines']

  command_list_string = ''
  command_list_string = ''
  command_list_string += '#!/bin/env python\n'
  #command_list_string += "base_command = './run_scripts/copy_aods.py'\n"
  command_list_string += "base_command = '"+os.environ['JB_DATASETS_DIR']+"/bin/copy_aods.py'\n"
  command_list_string += "base_folder = '"+args['target_directory']+"'\n"
  command_list_string += "mid_folder = ''\n"
  command_list_string += "print('# [global_key] dataset_files_info_filename : "+dataset_files_info_filename+"')\n"


  for dataset in dataset_files_info:
    for filename in dataset_files_info[dataset]:
      command_list_string += "print(base_command+' "+filename+" '+base_folder+mid_folder)\n"

  with open(command_list_filename,'w') as command_list_file:
    command_list_file.write(command_list_string)
    print('Wrote to '+command_list_filename)
    os.system('chmod +x '+command_list_filename)

