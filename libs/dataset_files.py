#!/usr/bin/env python
import multiprocessing
import subprocess
import json
import datasets
import time

#datasets_files_info[dataset][filename] = {'number_events':number_events, 'file_size': file_size, 'check_sum': check_sum, 'modification_date': modification_date}
#dataset_meta_results = [[dataset, filename], results]]
def parse_dataset_meta_results(dataset_meta_results, dataset_files_info):
  for keys_result in dataset_meta_results:
    dataset, filename = keys_result[0]
    result = json.loads(keys_result[1])
    dataset_files_info[dataset][filename]['number_events'] = result[0]['file'][0]['nevents']
    dataset_files_info[dataset][filename]['file_size'] = result[0]['file'][0]['size']
    dataset_files_info[dataset][filename]['check_sum'] = result[0]['file'][0]['check_sum']
    dataset_files_info[dataset][filename]['modification_date'] = result[0]['file'][0]['last_modification_date']

#datasets_files_info[dataset][filename] = {'number_events':number_events}
#dataset_meta_commands = [[dataset, filename], commands]]
def make_dataset_meta_commands(datasets_files_info):
  dataset_meta_commands = []
  for dataset in datasets_files_info:
    for filename in datasets_files_info[dataset]:
      dataset_meta_commands.append([[dataset, filename], json_query_file_string(filename)])
  return dataset_meta_commands

def get_list_dataset(filename):
  list_dataset = []
  with open(filename) as infile:
    for dataset in infile:
      if (dataset[0].lstrip())== '#': continue
      list_dataset.append(dataset.rstrip())
  return list_dataset

def json_query_file_string(file_path):
  return 'dasgoclient -query="file='+file_path+'" -json'

def query_dataset_string(query_type, dataset_path):
  return 'dasgoclient -query="'+query_type+' dataset='+dataset_path+'"'

def make_dataset_file_commands(list_dataset):
  #dataset_file_commands = [[dataset, commands]]
  dataset_file_commands = []
  for dataset in list_dataset:
    dataset_file_commands.append([dataset, query_dataset_string('file', dataset)])
  return dataset_file_commands

def run_command(argument):
  key = argument[0]
  for iRetry in range(10):
    try:
      print('command: '+argument[1])
      result = subprocess.check_output(argument[1], shell=True)
      break
    except subprocess.CalledProcessError as e:
      print(e.output)
      print('Trying again '+str(iRetry+1)+' command: '+argument[1])
      time.sleep(3)
  print('result: '+result)
  return [key, result]

#commands = [[key, commands]]
#key_results = [[key, commands]]
def run_list_command(commands):
  # Multi
  ncpu = multiprocessing.cpu_count()
  if ncpu > 8: ncpu = 8
  pool = multiprocessing.Pool(ncpu)
  key_results = pool.map(run_command, commands)
  ## Single
  #key_results = []
  #for key_command in commands:
  #  key_results.append(run_command(key_command))
  return key_results

#datasets_files_info[dataset][filename] = {'number_events':number_events}
# dataset_file_results = [[dataset, result]]
def parse_dataset_file_results(dataset_file_results):
  dataset_files_info = {}
  for dataset, result in dataset_file_results:
    dataset_files_info[dataset] = {}
    dataset_files = result.split()
    for dataset_file in dataset_files:
      if dataset_file in dataset_files_info[dataset]:
        print('[Error] There is already a dataset_file in dataset_files_info['+dataset+']. Ignoring')
      else:
        dataset_files_info[dataset][dataset_file] = {}
  return dataset_files_info

