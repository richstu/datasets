#!/usr/bin/env python
import datasets
import nested_dict
import filter_datasets_jsons
import argparse
import os
import sys
import argparse_helper
import ask
import sqlite3
import dataset_database

def is_sig(args):
  sig = False
  for data_type in args['mc_data_sig']:
    if data_type == 'mc': continue
    if data_type == 'data': continue
    sig = True
  return sig

def are_arguments_valid(args):
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

  if 'mc' in args['mc_data_sig'] or is_sig(args):
    t_path = os.path.join(args['meta_folder'],'mc_tag_meta')
    if not os.path.isfile(os.path.join(t_path)):
      return False, 'meta_mc_tag_meta: '+t_path+" doesn't exist."

  if 'data' in args['mc_data_sig']:
    t_path = os.path.join(args['meta_folder'],'data_tag_meta')
    if not os.path.isfile(os.path.join(t_path)):
      return False, 'meta_data_tag_meta: '+t_path+" doesn't exist."
 

  # Check if files exists with in_json_prefix
  if 'mc' in args['mc_data_sig'] or is_sig(args):
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


  # Check if output folder exits
  if not os.path.isdir(args['out_results_folder']):
    return False, 'out_results_folder: '+args['out_results_folder']+" doesn't exist."

  # Check if files exists with out_results_prefix 
  t_path = os.path.join(args['out_results_folder'], args['out_results_prefix']+'dataset_files')
  if os.path.isfile(t_path):
    overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
    if overwrite == 'n':
      return False, t_path+' already exists.'

  t_path = os.path.join(args['out_results_folder'], args['out_results_prefix']+'dataset_files_parsed')
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
  parser.add_argument('-t', '--mc_data_sig', metavar='"mc,data"', nargs=1, default=['mc,data'])
  parser.add_argument('-m', '--meta_folder', metavar='./meta', nargs=1, default=['./meta'])
  parser.add_argument('-i', '--in_json_folder', metavar='./jsons', nargs=1, default=['./jsons'])
  parser.add_argument('-if', '--in_files_prefix', metavar='', nargs=1, default=[''])
  parser.add_argument('-id', '--in_datasets_prefix', metavar='selected_', nargs=1, default=['selected_'])
  parser.add_argument('-s', '--sql_search', metavar="''", nargs=1, default=[''])
  parser.add_argument('-o', '--out_results_folder', metavar="./results", nargs=1, default=['./results'])
  parser.add_argument('-op', '--out_results_prefix', metavar="''", nargs=1, default=[''])

  args = vars(parser.parse_args())
  argparse_helper.initialize_arguments(args, list_args=['mc_data_sig'])
  valid, log = are_arguments_valid(args)
  if not valid:
    print('[Error] '+log)
    sys.exit()


  meta_folder = args['meta_folder']
  mc_dataset_common_names_filename = meta_folder+'/mc_dataset_common_names'
  mc_dataset_2016_names_filename = meta_folder+'/mc_dataset_2016_names'
  mc_dataset_2017_names_filename = meta_folder+'/mc_dataset_2017_names'
  mc_dataset_2018_names_filename = meta_folder+'/mc_dataset_2018_names'
  mc_tag_meta_filename = meta_folder+'/mc_tag_meta'
  data_tag_meta_filename = meta_folder+'/data_tag_meta'

  mc_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_files_prefix']+'mc_dataset_files_info.json')
  data_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_files_prefix']+'data_dataset_files_info.json')

  mc_datasets_filename = os.path.join(args['in_json_folder'], args['in_datasets_prefix']+'mc_datasets.json')
  data_datasets_filename = os.path.join(args['in_json_folder'], args['in_datasets_prefix']+'data_datasets.json')

  do_mc = False
  if 'mc' in args['mc_data_sig'] or is_sig(args): do_mc = True
  do_data = False
  if 'data' in args['mc_data_sig']: do_data = True
  do_signal = False

  search_term = args['sql_search']
  database = sqlite3.connect(':memory:')
  cursor = database.cursor()

  files = []

  if do_mc:
    # datasets_files_info[dataset][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
    mc_dataset_files_info = nested_dict.load_json_file(mc_dataset_files_info_filename)
    # mc_dataset_names[year] = [(mc_dataset_name, mc_dir)]
    mc_dataset_names = datasets.parse_multiple_mc_dataset_names([
      [mc_dataset_common_names_filename, ['2016', '2017', '2018']],
      [mc_dataset_2016_names_filename, ['2016']],
      [mc_dataset_2017_names_filename, ['2017']],
      [mc_dataset_2018_names_filename, ['2018']],
      ])
    # Ex) tag_meta[2016] = RunIISummer16, MiniAODv3, NanoAODv5
    mc_tag_meta = datasets.parse_mc_tag_meta(mc_tag_meta_filename)
    # mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
    mc_datasets = nested_dict.load_json_file(mc_datasets_filename)

    dataset_database.make_mc_database_tables(cursor)
    dataset_database.fill_mc_files_database(cursor, mc_dataset_files_info)
    dataset_database.fill_mc_datasets_database(cursor, mc_datasets, mc_dataset_names)
    dataset_database.fill_mc_tags_database(cursor, mc_tag_meta)
    dataset_database.fill_mc_children_database(cursor, mc_datasets)
    dataset_database.fill_mc_parent_database(cursor, mc_datasets)

    sql_search_term = '' if search_term == '' else 'WHERE '+search_term
    #cursor.execute('SELECT filename, mc_files.path AS path, file_events, file_size, mc_datasets.mc_dataset_name AS mc_dataset_name, mc_datasets.year AS year, mc_datasets.data_tier AS data_tier, mc_datasets.size AS size, mc_datasets.files AS files, mc_datasets.events AS events, mc_datasets.lumis AS lumis, mc_datasets.mc_dir AS mc_dir, mc_tags.year_tag AS year_tag, mc_tags.miniaod_tag AS miniaod_tag, mc_tags.nanoaod_tag AS nanoaod_tag FROM mc_files INNER JOIN mc_datasets ON mc_datasets.path = mc_files.path INNER JOIN mc_tags ON mc_tags.year = mc_datasets.year  '+sql_search_term+';')
    sql_command = 'SELECT filename, mc_files.path AS dataset_path, file_events, file_size, mc_datasets.mc_dataset_name AS mc_dataset_name, mc_datasets.year AS dataset_year, mc_datasets.data_tier AS data_tier, mc_datasets.size AS size, mc_datasets.files AS files, mc_datasets.events AS events, mc_datasets.lumis AS lumis, mc_datasets.mc_dir AS mc_dir, mc_tags.year_tag AS year_tag, mc_tags.miniaod_tag AS miniaod_tag, mc_tags.nanoaod_tag AS nanoaod_tag FROM mc_files INNER JOIN mc_datasets ON mc_datasets.path = mc_files.path INNER JOIN mc_tags ON mc_tags.year = mc_datasets.year  '+sql_search_term+';'
    #print(sql_command)
    cursor.execute(sql_command)

    for file_info in cursor.fetchall():
      filename = file_info[0]
      files.append(filename)


  if do_data:
    dataset_files_info_filename = data_dataset_files_info_filename
    # Make database
    data_dataset_files_info = nested_dict.load_json_file(data_dataset_files_info_filename)
    # data_tag_meta[year][run_group][streams][data_tier] = reco_tag
    data_tag_meta = datasets.parse_data_tag_meta(data_tag_meta_filename)

    # data_dataset[stream][year][run_group][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
    data_datasets = nested_dict.load_json_file(data_datasets_filename)

    dataset_database.make_data_database_tables(cursor)
    dataset_database.fill_data_files_database(cursor, data_dataset_files_info)
    dataset_database.fill_data_datasets_database(cursor, data_datasets)
    dataset_database.fill_data_tags_database(cursor, data_tag_meta)
    dataset_database.fill_data_children_database(cursor, data_datasets)
    dataset_database.fill_data_parent_database(cursor, data_datasets)

    sql_search_term = '' if search_term == '' else 'WHERE '+search_term
    cursor.execute('SELECT filename, data_files.path AS dataset_path, file_events, file_size, data_datasets.stream AS dataset_stream, data_datasets.year AS dataset_year, data_datasets.run_group AS dataset_run_group, data_datasets.data_tier AS data_tier, data_datasets.size AS size, data_datasets.files AS files, data_datasets.events AS events, data_datasets.lumis AS lumis, data_tags.miniaod_tag AS miniaod_tag, data_tags.nanoaod_tag AS nanoaod_tag, data_tags.nanoaodsim_tag AS nanoaodsim_tag FROM data_files INNER JOIN data_datasets ON data_datasets.path = data_files.path INNER JOIN data_tags ON data_tags.year = data_datasets.year AND data_tags.run_group = data_datasets.run_group AND data_tags.stream = data_datasets.stream '+sql_search_term+';')

    for file_info in cursor.fetchall():
      filename = file_info[0]
      files.append(filename)


  files_filename = os.path.join(args['out_results_folder'], args['out_results_prefix']+'dataset_files')
  write_list(files, files_filename)
  print('Wrote to '+files_filename)

  parsed_files_filename = os.path.join(args['out_results_folder'], args['out_results_prefix']+'dataset_files_parsed')
  parsed_files = []
  for filename in files:
    parsed_files.append(datasets.filename_to_parsed(filename))
  write_list(parsed_files, parsed_files_filename)
  print('Wrote to '+parsed_files_filename)



  ###mc_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_files_prefix']+'mc_dataset_files_info.json')
  ###data_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_files_prefix']+'data_dataset_files_info.json')

  ##mc_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_json_prefix']+'mc_dataset_files_info.json')
  ##data_dataset_files_info_filename = os.path.join(args['in_json_folder'], args['in_json_prefix']+'data_dataset_files_info.json')
  ##mc_files_filename = os.path.join(args['out_results_folder'], args['out_results_prefix']+'mc_dataset_files')
  ##data_files_filename = os.path.join(args['out_results_folder'], args['out_results_prefix']+'data_dataset_files')

  ##do_mc = False
  ##if 'mc' in args['mc_data']: do_mc = True
  ##do_data = False
  ##if 'data' in args['mc_data']: do_data = True

  #if do_mc:
  #  write_dataset_files(mc_dataset_files_info_filename, mc_files_filename)
  #  #mc_files = []
  #  ## datasets_files_info[dataset][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
  #  #mc_dataset_files_info = nested_dict.load_json_file(mc_dataset_files_info_filename)
  #  #for dataset in mc_dataset_files_info:
  #  #  for filename in mc_dataset_files_info[dataset]:
  #  #    mc_files.append(datasets.filename_to_parsed(filename))
  #  #write_list(mc_files, mc_files_filename)
  #  #print('Wrote to '+mc_files_filename)


  #if do_data:
  #  write_dataset_files(data_dataset_files_info_filename, data_files_filename)
  #  ## datasets_files_info[dataset][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
  #  #dataset_files_info_filename = data_dataset_files_info_filename
  #  #data_dataset_files_info = nested_dict.load_json_file(data_dataset_files_info_filename)
  #  #for dataset in data_dataset_files_info:
  #  #  for filename in data_dataset_files_info[dataset]:
  #  #    data_files.append(datasets.data_filename_to_parsed(filename))
  #  #write_list(data_files, data_files_filename)
  #  #print('Wrote to '+data_files_filename)
