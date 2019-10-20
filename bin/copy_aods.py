#!/usr/bin/env python
import argparse
import subprocess
import os

parser = argparse.ArgumentParser()
parser.add_argument('input_path')
parser.add_argument('output_path')
args = parser.parse_args()

# Set enviornment

if not os.path.exists(args.output_path):
  os.makedirs(args.output_path)
  print('[Info] copy_aods.py: Created '+args.output_path)

input_split = args.input_path.split('/')
output_path = args.output_path+'/'+input_split[4]+'__'+input_split[3]+'__'+input_split[6]+'__'+input_split[7]+'__'+input_split[8]
command = 'xrdcp root://cms-xrd-global.cern.ch/'+args.input_path+' '+output_path
print('[Info] copy_aods.py: Running command: '+command)
subprocess.check_output(command, shell=True)
