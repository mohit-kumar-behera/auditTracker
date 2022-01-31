from fastavro import writer, reader, parse_schema
import pandas as pd
import pyrebase
import os, datetime, ast, urllib

class Tracker:
  VALUE_CREATED = '@inserted'
  VALUE_UPDATED = '@updated'
  VALUE_DELETED = '@deleted'
  SEPARATOR = '.'
  AVRO_EXTENSION = 'avro'
  JSON_EXTENSION = 'json'
  OLD_SNAP_CALLED = 'snap'
  FROM_KEY_NAME = '-from'
  TO_KEY_NAME = '+to'
  INSERTED_KEY_NAME = '+inserted_data'
  DELETED_KEY_NAME = '-data_was'
  pk_name = 'id'
 
  def __init__(self, BASE_DIR, audit_filename, table_pk_name):
    self.BASE_DIR = BASE_DIR
    self.table_pk_name = table_pk_name
    self.audit_filename = f"{''.join(audit_filename.split('.')[0])}"
    self.AUDIT_FILE_PATH = os.path.join(self.BASE_DIR, f'{self.audit_filename}.{self.AVRO_EXTENSION}')
    self.__create_empty_audit_file(self.AUDIT_FILE_PATH)
    self.__create_avro_schema()

  @staticmethod
  def initialize_firebase_storage(firebaseConfig):
    Tracker.firebase = pyrebase.initialize_app(firebaseConfig)
    Tracker.storage = Tracker.firebase.storage()
  
  def __create_empty_audit_file(self, audit_file):
    if not os.path.isfile(audit_file):
      with open(audit_file, 'w') as f:
        pass

    path = Tracker.storage.child(audit_file).get_url(None)
    try:
      urllib.request.urlopen(path)
    except:
      self.__push_to_cloud(audit_file)
  
  def __remove_from_local(self, file):
    os.remove(file)
  
  def __create_avro_schema(self):
    self.avro_schema = {
      'doc': f"{self.audit_filename} db",
      'name': f"{self.audit_filename}",
      'namespace': f'{self.audit_filename}',
      'type': 'record',
      'fields':[
        {'name': 'updated_on', 'type': 'string'},
        {'name': 'timestamp', 'type': 'string'},
        {'name': f'{self.pk_name}', 'type': 'string'},
        {'name': f'{self.VALUE_UPDATED}', 'type': 'string'},
        {'name': f'{self.VALUE_DELETED}', 'type': 'string'},
        {'name': f'{self.VALUE_CREATED}', 'type': 'string'},
        {'name': f'{self.OLD_SNAP_CALLED}', 'type': 'string'}
      ]
    }
    self.parsed_schema = parse_schema(self.avro_schema)
    
  def __create_or_return_obj(self, obj, key):
    if key not in obj:
      obj[key] = {}
    return obj[key]
  
  def __flatten(self, dictionary, parent_key = '', sep = SEPARATOR):
    items = []
    for key, val in dictionary.items():
      new_key = parent_key + sep + key if parent_key else key
      if isinstance(val, dict):
        items.extend(self.__flatten(val, new_key, sep).items())
      else:
        items.append((new_key, val))
    return dict(items)

  def __deflatten(self, dictionary, sep = SEPARATOR):
    obj = {}
    for k, v in dictionary.items():
      if sep not in k:
        obj[k] = v
      else:
        key_splitted = k.split('.')
        def reconstruct_flat_keys(obj, key, val):
          if len(key) == 1:
            obj[key[0]] = val
            return
          reconstruct_flat_keys(self.__create_or_return_obj(obj, key[0]), key[1:], val)
        reconstruct_flat_keys(self.__create_or_return_obj(obj, key_splitted[0]), key_splitted[1:], v)
    return obj

  def __push_to_cloud(self, file):
    cloudfilename = file
    Tracker.storage.child(cloudfilename).put(file)

  def __download_from_cloud(self, path):
    cloudfilename = path
    Tracker.storage.child(cloudfilename).download(cloudfilename, cloudfilename)

  def __dump_into_avro(self, delta_changes):
    self.__download_from_cloud(self.AUDIT_FILE_PATH)
    
    df = pd.DataFrame(delta_changes)
    records = df.to_dict('records')
    
    with open(self.AUDIT_FILE_PATH, 'a+b') as outfile:
      writer(outfile, self.parsed_schema, records)
    
    self.__push_to_cloud(self.AUDIT_FILE_PATH)
    self.__remove_from_local(self.AUDIT_FILE_PATH)


  def __generate_delta_obj(self, old_obj, new_obj):
    now = datetime.datetime.now()
    delta_obj = {
      'updated_on': now.strftime('%c'),
      'timestamp': now.timestamp()
    }
    delta_obj[self.pk_name] = old_obj.get(self.table_pk_name, None)
    delta_obj[self.VALUE_UPDATED] = dict()
    delta_obj[self.VALUE_DELETED] = dict()
    delta_obj[self.VALUE_CREATED] = dict()

    attrs_of_old_obj = old_obj.keys()
    attrs_of_new_obj = new_obj.keys()

    old_obj_keys = set(attrs_of_old_obj)
    new_obj_keys = set(attrs_of_new_obj)
    inserted_keys = new_obj_keys.difference(old_obj_keys)
    deleted_keys = old_obj_keys.difference(new_obj_keys)
    updated_keys = old_obj_keys.intersection(new_obj_keys)

    """ UPDATED """
    for keys in updated_keys:
      if old_obj[keys]!=new_obj[keys]:
        delta_obj[self.VALUE_UPDATED][keys] = {'-from': old_obj[keys], '+to': new_obj[keys]}

    """ INSERTED """
    for key in inserted_keys:
      delta_obj[self.VALUE_CREATED][key] = {'+inserted_data': new_obj[key]}

    """ DELETED """
    for key in deleted_keys:
      delta_obj[self.VALUE_DELETED][key] = {'-data_was': old_obj[key]}

    return delta_obj
  

  def __extract_from_dataframe(self, value):
    evaluated_value = ast.literal_eval(value)
    return self.__deflatten(evaluated_value) if bool(value) else evaluated_value
  

  def __fetch_audit_as_json(self, old_snap = False):
    avro_records = []
    self.__download_from_cloud(self.AUDIT_FILE_PATH)
    
    with open(self.AUDIT_FILE_PATH, 'rb') as file:
      avro_reader = reader(file)
      avro_records.extend(avro_reader)
    df_avro = pd.DataFrame(avro_records)
    
    field_name = f'{self.audit_filename}_audit'
    delta_obj_json = {field_name: []}
    
    for entry in df_avro.iloc[:,:].values:
      delta_obj = dict()
      delta_obj['updated_on'] = entry[0]
      delta_obj['timestamp'] = entry[1]
      delta_obj[self.pk_name] = entry[2]

      if old_snap:
        delta_obj[self.OLD_SNAP_CALLED] = ast.literal_eval(entry[6])
      
      delta_obj[self.VALUE_UPDATED] = self.__extract_from_dataframe(entry[3])
      delta_obj[self.VALUE_DELETED] = self.__extract_from_dataframe(entry[4])
      delta_obj[self.VALUE_CREATED] = self.__extract_from_dataframe(entry[5])

      delta_obj_json[field_name].append(delta_obj)

    self.__remove_from_local(self.AUDIT_FILE_PATH)
    return delta_obj_json
  

  def __filter_by_date_range(self, records, sd, sm, sy, ed, em, ey):
    start_date = datetime.datetime(sy, sm, sd)
    start_date_timestamp = start_date.timestamp()
    end_date = datetime.datetime(ey, em, ed)
    end_date_timestamp = end_date.timestamp()
    
    filtered_records = list(filter(lambda audit: float(audit['timestamp']) >= start_date_timestamp and float(audit['timestamp']) <= end_date_timestamp, records[f'{self.audit_filename}_audit']))
    return filtered_records


  def __remove_keys(self, obj, keys):
    for key in keys:
      if key in obj: 
        del obj[key]


  def __construct_obj_from_delta(self, obj, delta):
    update_delta_flat = self.__flatten(delta[self.VALUE_UPDATED])
    insert_delta_flat = self.__flatten(delta[self.VALUE_CREATED])
    delete_delta_flat = self.__flatten(delta[self.VALUE_UPDATED])

    seperator_len = len(self.SEPARATOR)

    """ CONSTRUCT FROM UPDATE """
    for k, v in update_delta_flat.items():
      key_len = len(self.TO_KEY_NAME)
      if self.TO_KEY_NAME in k:
        obj[k[:-(key_len + seperator_len)]] = v
    
    """ CONSTRUCT FROM INSERT """
    for k, v in insert_delta_flat.items():
      key_len = len(self.INSERTED_KEY_NAME)
      if self.INSERTED_KEY_NAME in k:
        obj[k[:-(key_len + seperator_len)]] = v

    """ CONSTRUCT FROM DELETE """
    for k in delete_delta_flat.keys():
      key_len = len(self.DELETED_KEY_NAME)
      if self.DELETED_KEY_NAME in k:
        del obj[k[:-(key_len + seperator_len)]]

    return obj


  def __calc_endpoints_delta(self, records):
    _list = []
    buffer = dict()
    
    for record in records:
      record_id = record[self.pk_name]
      
      if record_id in buffer.keys():
        buffer[record_id]["end"] = record[self.OLD_SNAP_CALLED]
        buffer[record_id]["delta"] = record
        buffer[record_id]["has_atleast_one_change"] = 1
      else:
        buffer[record_id] = {"start": record[self.OLD_SNAP_CALLED], "end": None, "delta": record, "has_atleast_one_change": 0}

    for key in buffer.keys():
      current_obj = buffer[key]
      delta = None    
      if current_obj["has_atleast_one_change"]:
        new_obj = self.__construct_obj_from_delta(current_obj["end"], current_obj["delta"])
        delta = self.__generate_delta_obj(current_obj["start"], new_obj)

        """ OPTIONAL """
        # delta = self.__deflatten(self.__flatten(delta))
      else:
        delta = current_obj["delta"]
      
      self.__remove_keys(delta, ['snap', 'updated_on', 'timestamp'])
      _list.append(delta) 
    return _list
    

  def track(self, old_obj, new_obj):
    flattened_old_obj = self.__flatten(old_obj)
    flattened_new_obj = self.__flatten(new_obj)
    delta_change_obj = self.__generate_delta_obj(flattened_old_obj, flattened_new_obj)

    """ ATTACH OLD SNAPSHOT """
    delta_change_obj[self.OLD_SNAP_CALLED] = flattened_old_obj

    # converting the values of the dictionary(json) to string so as to store in avro
    for change in delta_change_obj.keys():
      delta_change_obj[change] = [str(delta_change_obj[change])]
    
    self.__dump_into_avro(delta_change_obj)


  def get_all_audits(self):
    return self.__fetch_audit_as_json(old_snap = False)


  def audit_of_today(self):
    today = datetime.datetime.today()
    d, m, y = int(today.strftime('%d')), int(today.strftime('%m')), int(today.strftime('%Y'))
    return self.audit_of_date(d, m, y)
  

  def audit_of_date(self, d, m, y):
    start_date = datetime.datetime(y, m, d)
    end_date = start_date + datetime.timedelta(days = 1)
    ed, em, ey = int(end_date.strftime('%d')), int(end_date.strftime('%m')), int(end_date.strftime('%Y')) 
    return self.audit_between_date(d, m, y, ed, em, ey)


  def audit_from_date(self, d, m, y):
    end_date = datetime.date.today()
    end_date = end_date + datetime.timedelta(days = 1)
    ed, em, ey = int(end_date.strftime('%d')), int(end_date.strftime('%m')), int(end_date.strftime('%Y'))
    return self.audit_between_date(d, m, y, ed, em, ey, endpoints = False)


  def audit_between_date(self, sd, sm, sy, ed, em, ey, endpoints = False):
    record_field = f'{self.audit_filename}-{sd}-{sm}-{sy}__{ed}-{em}-{ey}'
    records = {record_field: []}
    data_audits = self.__fetch_audit_as_json(old_snap = endpoints)

    filtered_records = self.__filter_by_date_range(data_audits, sd, sm, sy, ed, em, ey)
    delta_of_filtered_records = filtered_records if not endpoints else self.__calc_endpoints_delta(filtered_records)

    records[record_field].extend(delta_of_filtered_records)
    return records
  
  
  def audit_by_id(self, id, sd = None, sm = None, sy = None, ed = None, em = None, ey = None, endpoints = False):
    dates = [sd, sm, sy, ed, em, ey]

    record_field = f'{self.audit_filename}-id-{id}'
    records = {record_field: []}
    data_audits = self.__fetch_audit_as_json(old_snap = endpoints)

    if None not in dates:
      filtered_records = self.__filter_by_date_range(data_audits, sd, sm, sy, ed, em, ey)
      data_audits[f'{self.audit_filename}_audit'] = filtered_records

    filtered_records = list(filter(lambda audit: audit[self.pk_name] == id, data_audits[f'{self.audit_filename}_audit']))
    delta_of_filtered_records = filtered_records if not endpoints else self.__calc_endpoints_delta(filtered_records)

    records[record_field].extend(delta_of_filtered_records)
    return records
  

  def audit_by_operation(self, operation, sd = None, sm = None, sy = None, ed = None, em = None, ey = None):
    dates = [sd, sm, sy, ed, em, ey]
    operations_available = ['inserted', 'updated', 'deleted']

    if operation not in operations_available:
      return
    operation = f'@{operation}'
    
    def fetch_operation_obj(audit, _list):
      if audit.get(operation, None):
        _list.append({
          'updated_on': audit['updated_on'],
          'timestamp': audit['timestamp'],
          'id': audit[self.pk_name],
          operation: audit[operation]
        })
    
    record_field = f'{self.audit_filename}--{operation}'
    records = {record_field: []}
    data_audits = self.__fetch_audit_as_json(old_snap = False)

    if None not in dates:
      filtered_records = self.__filter_by_date_range(data_audits, sd, sm, sy, ed, em, ey)
      data_audits[f'{self.audit_filename}_audit'] = filtered_records

    list(map(lambda audit: fetch_operation_obj(audit, records[record_field]), data_audits[f'{self.audit_filename}_audit']))
    return records
