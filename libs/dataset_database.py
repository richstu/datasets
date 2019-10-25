
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
  cursor.execute('CREATE TABLE data_parent(parent_path text PRIMARY KEY, path text NOT NULL);')
  #cursor.execute('SELECT * FROM data_parent')
  #print([description[0] for description in cursor.description])


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
  for stream in data_datasets:
    for year in data_datasets[stream]:
      for run_group in data_datasets[stream][year]:
        for data_tier in data_datasets[stream][year][run_group]:
          for path in data_datasets[stream][year][run_group][data_tier]:
            previous_path = path
            for parent_path in data_datasets[stream][year][run_group][data_tier][path]['parent_chain']:
              cursor.execute('INSERT INTO data_parent (parent_path, path) VALUES (?, ?)',(parent_path, previous_path))
              previous_path = parent_path

#def setup_database():
#  database = sqlite3.connect(':memory:')
#  cursor = database.cursor()
#  return cursor
#
#def load_mc_dataset_files(mc_dataset_files_info_filename, mc_dataset_common_names_filename,
#    mc_dataset_2016_names_filename, mc_dataset_2017_names_filename,
#    mc_dataset_2018_names_filename, mc_tag_meta_filename, mc_datasets_filename):
#  
#  # datasets_files_info[dataset][filename] = {'number_events':int, 'check_sum':int, 'modification_date':int, 'file_size':int}
#  mc_dataset_files_info = nested_dict.load_json_file(mc_dataset_files_info_filename)
#
#  # mc_dataset_names[year] = [(mc_dataset_name, mc_dir)]
#  mc_dataset_names = datasets.parse_multiple_mc_dataset_names([
#    [mc_dataset_common_names_filename, ['2016', '2017', '2018']],
#    [mc_dataset_2016_names_filename, ['2016']],
#    [mc_dataset_2017_names_filename, ['2017']],
#    [mc_dataset_2018_names_filename, ['2018']],
#    ])
#  #print ('dataset_names:', mc_dataset_names)
#  # Ex) tag_meta[2016] = RunIISummer16, MiniAODv3, NanoAODv5
#  mc_tag_meta = datasets.parse_mc_tag_meta(mc_tag_meta_filename)
#
#  # mc_datasets[mc_dataset_name][year][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
#  mc_datasets = nested_dict.load_json_file(mc_datasets_filename)
#
#  return mc_dataset_files_info, mc_dataset_names, mc_tag_meta, mc_datasets
#
#def make_mc_database(mc_dataset_files_info_filename, mc_dataset_common_names_filename,
#    mc_dataset_2016_names_filename, mc_dataset_2017_names_filename,
#    mc_dataset_2018_names_filename, mc_tag_meta_filename, mc_datasets_filename):
#
#    mc_dataset_files_info, mc_dataset_names, mc_tag_meta, mc_datasets = load_mc_dataset_files(mc_dataset_files_info_filename, mc_dataset_common_names_filename,
#      mc_dataset_2016_names_filename, mc_dataset_2017_names_filename,
#      mc_dataset_2018_names_filename, mc_tag_meta_filename, mc_datasets_filename)
#
#    cursor = setup_database()
#    make_mc_database_tables(cursor)
#    fill_mc_files_database(cursor, mc_dataset_files_info)
#    fill_mc_datasets_database(cursor, mc_datasets, mc_dataset_names)
#    fill_mc_tags_database(cursor, mc_tag_meta)
#    fill_mc_children_database(cursor, mc_datasets)
#    fill_mc_parent_database(cursor, mc_datasets)
#
#    return cursor
#
#def load_data_dataset_files(data_dataset_files_info_filename, data_tag_meta_filename,
#    data_datasets_filename):
#
#  data_dataset_files_info = nested_dict.load_json_file(data_dataset_files_info_filename)
#  # data_tag_meta[year][run_group][streams][data_tier] = reco_tag
#  data_tag_meta = datasets.parse_data_tag_meta(data_tag_meta_filename)
#
#  # data_dataset[stream][year][run_group][data_tier][path] = {"parent_chain":[], "children":[], "creation time":string, "size":int, "files":int, "events:"int}
#  data_datasets = nested_dict.load_json_file(data_datasets_filename)
#
#  return data_dataset_files_info, data_tag_meta, data_datasets
#
#def make_data_database(data_dataset_files_info_filename, data_tag_meta_filename,
#    data_datasets_filename):
#
#  data_dataset_files_info, data_tag_meta, data_datasets = load_data_dataset_files(data_dataset_files_info_filename, data_tag_meta_filename,
#    data_datasets_filename)
#
#  cursor = setup_database()
#  make_data_database_tables(cursor)
#  fill_data_files_database(cursor, data_dataset_files_info)
#  fill_data_datasets_database(cursor, data_datasets)
#  fill_data_tags_database(cursor, data_tag_meta)
#  fill_data_children_database(cursor, data_datasets)
#  fill_data_parent_database(cursor, data_datasets)
#
#  return cursor

# Search terms:
#   filename, path, file_events, file_size, mc_dataset_name, year, data_tier, size, 
#   files, events, lumis, mc_dir, year_tag, miniaod_tag, nanoaod_tag
# Returns ntuple with above sequence
def search_mc_database(mc_data_sig, search_term, cursor):
  sql_search_term = 'WHERE mc_dir = "'+mc_data_sig+'"'
  sql_search_term += ' '+search_term
  cursor.execute('SELECT filename, mc_files.path AS path, file_events, file_size, mc_datasets.mc_dataset_name AS mc_dataset_name, mc_datasets.year AS year, mc_datasets.data_tier AS data_tier, mc_datasets.size AS size, mc_datasets.files AS files, mc_datasets.events AS events, mc_datasets.lumis AS lumis, mc_datasets.mc_dir AS mc_dir, mc_tags.year_tag AS year_tag, mc_tags.miniaod_tag AS miniaod_tag, mc_tags.nanoaod_tag AS nanoaod_tag FROM mc_files INNER JOIN mc_datasets ON mc_datasets.path = mc_files.path INNER JOIN mc_tags ON mc_tags.year = mc_datasets.year  '+sql_search_term+';')
  return cursor.fetchall()

# Search terms
#   filename, path, file_events, file_size, stream, year, run_group, 
#   data_tier, size, files, events, lumis, miniaod_tag, nanoaod_tag, nanoaodsim_tag 
# Returns ntuple with above sequence
def search_data_database(search_term,cursor):
  sql_search_term = '' if search_term == '' else 'WHERE '+search_term
  cursor.execute('SELECT filename, data_files.path AS path, file_events, file_size, data_datasets.stream AS stream, data_datasets.year AS year, data_datasets.run_group AS run_group, data_datasets.data_tier AS data_tier, data_datasets.size AS size, data_datasets.files AS files, data_datasets.events AS events, data_datasets.lumis AS lumis, data_tags.miniaod_tag AS miniaod_tag, data_tags.nanoaod_tag AS nanoaod_tag, data_tags.nanoaodsim_tag AS nanoaodsim_tag FROM data_files INNER JOIN data_datasets ON data_datasets.path = data_files.path INNER JOIN data_tags ON data_tags.year = data_datasets.year AND data_tags.run_group = data_datasets.run_group AND data_tags.stream = data_datasets.stream '+sql_search_term+';')
  return cursor.fetchall()
