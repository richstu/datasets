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
  # Check for mc_data
  if not argparse_helper.is_valid(args, 'mc_data', ['mc', 'data']):
    return False, 'mc_data: '+str(args['mc_data'])+' is not valid.'

  # Check if input folder exits
  if not os.path.isdir(args['base_folder']):
    return False, 'base_folder: '+args['base_folder']+" doesn't exist."
 
  # Check if output folder exits
  if not os.path.isdir(args['out_jsons_folder']):
    return False, 'out_jsons_folder: '+args['out_jsons_folder']+" doesn't exist."

  # Check if files exists with out_results_prefix 
  if 'mc' in args['mc_data']:
    t_path = os.path.join(args['out_jsons_folder'], args['out_jsons_prefix']+'mc_disk_files.json')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

  if 'data' in args['mc_data']:
    t_path = os.path.join(args['out_jsons_folder'], args['out_jsons_prefix']+'data_disk_files.json')
    if os.path.isfile(t_path):
      overwrite = ask.ask_key(t_path+' already exists. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
      if overwrite == 'n':
        return False, t_path+' already exists.'

  return True, ''

# file_mc: NanoAODv5/Nano/2016/mc/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8__RunIISummer16NanoAODv5__PUMoriond17_Nano1June2019_102X_mcRun2_asymptotic_v7-v1__250000__37FA68CC-B841-7D41-994C-645CFA4BA227.root
# parsed_mc_filename: ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8__RunIISummer16NanoAODv5__PUMoriond17_Nano1June2019_102X_mcRun2_asymptotic_v7-v1__250000__37FA68CC-B841-7D41-994C-645CFA4BA227.root
# mc_filename: /store/mc/RunIISummer16NanoAODv5/WZTo3LNu_TuneCUETP8M1_13TeV-powheg-pythia8/NANOAODSIM/PUMoriond17_Nano1June2019_102X_mcRun2_asymptotic_v7-v1/120000/222071C0-CF04-1E4B-B65E-49D18B91DE8B.root
# file_nanomc_info = (mc_filename, year, nanoaod_tag, file_events, mc_dir)
def get_file_nanomc_info(base_folder, mc_disk_filename_with_base):
  mc_disk_filename = mc_disk_filename_with_base[len(base_folder)+1:]
  mc_disk_filename_split = mc_disk_filename.split('/')
  nanoaod_tag = mc_disk_filename_split[0]
  year = mc_disk_filename_split[2]
  mc_dir = mc_disk_filename_split[3]
  parsed_mc_filename = mc_disk_filename_split[4]
  mc_filename = datasets.parsed_to_mc_filename(parsed_mc_filename)
  #parsed_mc_filename_split = parsed_mc_filename.split('__')
  #mc_filename = '/store/mc/'+parsed_mc_filename_split[1]+'/'+parsed_mc_filename_split[0]+'/NANOAODSIM/'+parsed_mc_filename_split[2]+'/'+parsed_mc_filename_split[3]+'/'+parsed_mc_filename_split[4]
  file_events = -1
  try:
    root_file = ROOT.TFile.Open(mc_disk_filename_with_base)
    root_tree = root_file.Get('Events')
    file_events = root_tree.GetEntries()
  except:
    print('Failed to get events for '+mc_disk_filename_with_base)
  return mc_filename, year, nanoaod_tag, file_events, mc_dir

# file_mc: NanoAODv5/Nano/2016/mc/ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8__RunIISummer16NanoAODv5__PUMoriond17_Nano1June2019_102X_mcRun2_asymptotic_v7-v1__250000__37FA68CC-B841-7D41-994C-645CFA4BA227.root
# parsed_data_filename: ZZZ_TuneCUETP8M1_13TeV-amcatnlo-pythia8__RunIISummer16NanoAODv5__PUMoriond17_Nano1June2019_102X_mcRun2_asymptotic_v7-v1__250000__37FA68CC-B841-7D41-994C-645CFA4BA227.root
# data_filename: /store/mc/RunIISummer16NanoAODv5/WZTo3LNu_TuneCUETP8M1_13TeV-powheg-pythia8/NANOAODSIM/PUMoriond17_Nano1June2019_102X_mcRun2_asymptotic_v7-v1/120000/222071C0-CF04-1E4B-B65E-49D18B91DE8B.root
# file_nanodata_info = (data_filename, year, nanoaod_tag, file_events, data_dir)
def get_file_nanodata_info(base_folder, data_disk_filename_with_base):
  data_disk_filename = data_disk_filename_with_base[len(base_folder)+1:]
  data_disk_filename_split = data_disk_filename.split('/')
  nanoaod_tag = data_disk_filename_split[0]
  year = data_disk_filename_split[2]
  data_dir = data_disk_filename_split[3]
  parsed_data_filename = data_disk_filename_split[4]
  data_filename = datasets.parsed_to_data_filename(parsed_data_filename)
  #parsed_data_filename_split = parsed_data_filename.split('__')
  #data_filename = '/store/mc/'+parsed_data_filename_split[1]+'/'+parsed_data_filename_split[0]+'/NANOAODSIM/'+parsed_data_filename_split[2]+'/'+parsed_data_filename_split[3]+'/'+parsed_data_filename_split[4]
  file_events = -1
  try:
    root_file = ROOT.TFile.Open(data_disk_filename_with_base)
    root_tree = root_file.Get('Events')
    file_events = root_tree.GetEntries()
  except:
    print('Failed to get events for '+data_disk_filename_with_base)
  return data_filename, year, nanoaod_tag, file_events, data_dir


## mc_disk_files[data_tier][aod_tag][year][mc_data_dir][filename] = {'file_events': int}
#def fill_mc_disk_files(mc_disk_files, data_tier, filename, year, nanoaod_tag, file_events, mc_dir):
#  if data_tier not in mc_disk_files:
#    mc_disk_files[data_tier] = {}
#  if nanoaod_tag not in mc_disk_files[data_tier]:
#    mc_disk_files[data_tier][nanoaod_tag] = {}
#  if year not in mc_disk_files[data_tier][nanoaod_tag]:
#    mc_disk_files[data_tier][nanoaod_tag][year] = {}
#  if mc_dir not in mc_disk_files[data_tier][nanoaod_tag][year]:
#    mc_disk_files[data_tier][nanoaod_tag][year][mc_dir] = {}
#  if filename not in mc_disk_files[data_tier][nanoaod_tag][year][mc_dir]:
#    mc_disk_files[data_tier][nanoaod_tag][year][mc_dir][filename] = {'file_events': file_events}
#  else:
#    print('[Warning] mc_disk_files['+data_tier+']['+nanoaod_tag+']['+str(year)+']['+mc_dir+']['+filename+'] already exists')

# disk_files[data_tier][aod_tag][year][disk_dir][filename] = {'file_events': int}
def fill_disk_files(disk_files, data_tier, filename, year, nanoaod_tag, file_events, disk_dir):
  if data_tier not in disk_files:
    disk_files[data_tier] = {}
  if nanoaod_tag not in disk_files[data_tier]:
    disk_files[data_tier][nanoaod_tag] = {}
  if year not in disk_files[data_tier][nanoaod_tag]:
    disk_files[data_tier][nanoaod_tag][year] = {}
  if disk_dir not in disk_files[data_tier][nanoaod_tag][year]:
    disk_files[data_tier][nanoaod_tag][year][disk_dir] = {}
  if filename not in disk_files[data_tier][nanoaod_tag][year][disk_dir]:
    disk_files[data_tier][nanoaod_tag][year][disk_dir][filename] = {'file_events': file_events}
  else:
    print('[Warning] disk_files['+data_tier+']['+nanoaod_tag+']['+str(year)+']['+disk_dir+']['+filename+'] already exists')


# mc_disk_files[data_tier][aod_tag][year][mc_dir][filename] = {'file_events': int}
def add_mc_disk_files(base_folder, nanoaod_version, mc_disk_files):
  # Fill nanoaod info
  # TODO: Need to fix zgamma
  #mc_files = glob.glob(base_folder+'/*/nano/*/[!data]*/*.root')
  print('Finding all files for nanoaod in below path')
  print(base_folder+'/'+nanoaod_version+'/nano/*/mc/*.root')
  mc_disk_filenames = glob.glob(base_folder+'/'+nanoaod_version+'/nano/*/mc/*.root')
  for mc_disk_filename in mc_disk_filenames:
    print('Processing '+mc_disk_filename)
    # mc_disk_files[nanoaod_tag][year][mc_dir][filename] = {'file_events': int}
    mc_filename, year, nanoaod_tag, file_events, mc_dir = get_file_nanomc_info(base_folder, mc_disk_filename)
    #fill_mc_disk_files(mc_disk_files, 'nanoaod', mc_filename, year, nanoaod_tag, file_events, mc_dir)
    fill_disk_files(mc_disk_files, 'nanoaod', mc_filename, year, nanoaod_tag, file_events, mc_dir)
  return mc_disk_files

# data_disk_files[data_tier][aod_tag][year][data_dir][filename] = {'file_events': int}
def add_data_disk_files(base_folder, nanoaod_version, data_disk_files):
  print('Finding all files for nanoaod in below path')
  print(base_folder+'/'+nanoaod_version+'/nano/*/data/*.root')
  # Fill nanoaod info
  data_disk_filenames = glob.glob(base_folder+'/'+nanoaod_version+'/nano/*/data/*.root')
  for data_disk_filename in data_disk_filenames:
    print('Processing '+data_disk_filename)
    # data_disk_files[nanoaod_tag][year][data_dir][filename] = {'file_events': int}
    data_filename, year, nanoaod_tag, file_events, data_dir = get_file_nanodata_info(base_folder, data_disk_filename)
    fill_disk_files(data_disk_files, 'nanoaod', data_filename, year, nanoaod_tag, file_events, data_dir)
  return data_disk_files

if __name__ == "__main__":

  parser = argparse.ArgumentParser(description='Makes disk_files.jsons.')
  parser.add_argument('-t', '--mc_data', metavar='"mc,data"', nargs=1, default=['mc,data'])
  parser.add_argument('-b', '--base_folder', metavar='/net/cms11/cms11r0/pico', nargs=1, default=['/net/cms11/cms11r0/pico'])
  parser.add_argument('-o', '--out_jsons_folder', metavar='./jsons', nargs=1, default=['./jsons'])
  parser.add_argument('-n', '--nanoaod_version', metavar='NanoAODvX', nargs=1, default=['NanoAODv9,NanoAODv12'])
  parser.add_argument('-op', '--out_jsons_prefix', metavar="''", nargs=1, default=[''])

  args = vars(parser.parse_args())
  argparse_helper.initialize_arguments(args, list_args = ['mc_data', 'nanoaod_version'])
  valid, log = are_arguments_valid(args)
  if not valid:
    print('[Error] '+log)
    sys.exit()

  do_mc = False
  if 'mc' in args['mc_data']: do_mc = True
  do_data = False
  if 'data' in args['mc_data']: do_data = True

  base_folder = args['base_folder']
  mc_disk_files_filename = os.path.join(args['out_jsons_folder'],args['out_jsons_prefix']+'mc_disk_files.json')
  data_disk_files_filename = os.path.join(args['out_jsons_folder'],args['out_jsons_prefix']+'data_disk_files.json')

  if do_mc:
    mc_disk_files = {}
    for nanoaod_version in args['nanoaod_version']:
      # mc_disk_files[data_tier][aod_tag][year][mc_dir][filename] = {'file_events': int}
      mc_disk_files = add_mc_disk_files(base_folder, nanoaod_version, mc_disk_files)
      nested_dict.save_json_file(mc_disk_files, mc_disk_files_filename)

  if do_data:
    data_disk_files = {}
    for nanoaod_version in args['nanoaod_version']:
      # data_disk_files[data_tier][aod_tag][year][data_dir][filename] = {'file_events': int}
      data_disk_files = add_data_disk_files(base_folder, nanoaod_version, data_disk_files)
      nested_dict.save_json_file(data_disk_files, data_disk_files_filename)
