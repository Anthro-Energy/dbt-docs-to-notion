import hashlib
import json
import os
import sys
import time
import traceback

import requests


DATABASE_PARENT_ID = os.environ['DATABASE_PARENT_ID']
DATABASE_NAME = os.environ['DATABASE_NAME']
NOTION_TOKEN = os.environ['NOTION_TOKEN']

NOTION_VERSION = '2022-06-28'
MAX_ATTEMPTS = 5
RETRYABLE_STATUS_CODES = (409, 429, 500, 502, 503, 504)
# Notion caps append/create payloads at 100 blocks per request; nested table
# rows count toward the cap, so batching weighs them (see batch_children).
MAX_BLOCKS_PER_REQUEST = 100
MAX_ROWS_PER_TABLE = 98
MAX_RICH_TEXT_LENGTH = 2000
SYNC_HASH_PROPERTY = 'Sync Hash'
RETIRED_PROPERTIES = ('Approx Rows', 'Approx GB')

DATABASE_PROPERTIES_SCHEMA = {
  "Name": {"title": {}},
  "Description": {"rich_text": {}},
  "Owner": {"rich_text": {}},
  "Relation": {"rich_text": {}},
  "Depends On": {"rich_text": {}},
  "Tags": {"rich_text": {}},
  SYNC_HASH_PROPERTY: {"rich_text": {}},
}


def make_request(endpoint, querystring='', method='GET', **request_kwargs):
  headers = {
    'Authorization': (
      NOTION_TOKEN if NOTION_TOKEN.startswith('Bearer ')
      else f'Bearer {NOTION_TOKEN}'
    ),
    'Content-Type': 'application/json',
    'Notion-Version': NOTION_VERSION
  }
  url = f'https://api.notion.com/v1/{endpoint}{querystring}'

  for attempt in range(1, MAX_ATTEMPTS + 1):
    time.sleep(0.34) # notion api limit is 3 requests per second
    resp = requests.request(method, url, headers=headers, **request_kwargs)
    if resp.status_code == 200:
      return resp.json()
    if resp.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_ATTEMPTS:
      backoff = float(resp.headers.get('Retry-After') or 2 ** attempt)
      print(f'{method} {endpoint}{querystring} returned {resp.status_code}, '
            f'retrying in {backoff:.0f}s (attempt {attempt}/{MAX_ATTEMPTS})')
      time.sleep(backoff)
      continue
    raise Exception(
      f"Request returned status code {resp.status_code}\nResponse text: {resp.text}"
    )


def make_paginated_request(endpoint, querystring='', method='GET', json=None):
  """Collect results across every page of a paginated endpoint."""
  results = []
  start_cursor = None
  while True:
    if method == 'GET':
      if start_cursor:
        separator = '&' if '?' in querystring else '?'
        resp = make_request(
          endpoint, f'{querystring}{separator}start_cursor={start_cursor}', method
        )
      else:
        resp = make_request(endpoint, querystring, method)
    else:
      body = dict(json or {})
      if start_cursor:
        body['start_cursor'] = start_cursor
      resp = make_request(endpoint, querystring, method, json=body)
    results.extend(resp['results'])
    if not resp.get('has_more'):
      return results
    start_cursor = resp['next_cursor']


def get_paths_or_empty(parent_object, paths_array, zero_value=''):
  """Used for catalog_nodes accesses, since structure is variable"""
  for path in paths_array:
    obj = parent_object
    for el in path:
      if el not in obj:
        obj = zero_value
        break
      obj = obj[el]
    if obj != zero_value:
      return obj

  return zero_value


def get_owner(data, catalog_nodes, model_name):
  """
  Check for an owner field explicitly named in the DBT Config
  If none present, fall back to database table owner
  """
  owner = get_paths_or_empty(data, [['config', 'meta', 'owner']], None)
  if owner is not None:
    return owner

  return get_paths_or_empty(catalog_nodes, [[model_name, 'metadata', 'owner']], '')


def split_text_into_chunks(text, chunk_size=MAX_RICH_TEXT_LENGTH):
    """
    Split text into chunks of specified size, preserving line breaks.

    Parameters
    ----------
    text : str
        The text to split into chunks
    chunk_size : int, default=2000
        Maximum size of each chunk

    Returns
    -------
    list
        List of text chunks
    """
    if not text:
        return [""]

    lines = text.splitlines(True)  # Keep the newline characters
    chunks = []
    current_chunk = ""

    for line in lines:
        # If adding this line would exceed the chunk size and we already have content,
        # start a new chunk
        if len(current_chunk) + len(line) > chunk_size and current_chunk:
            chunks.append(current_chunk)
            current_chunk = line
        else:
            current_chunk += line

    # Add the last chunk if it has content
    if current_chunk:
        chunks.append(current_chunk)

    # Handle the case where a single line is longer than chunk_size
    final_chunks = []
    for chunk in chunks:
        if len(chunk) <= chunk_size:
            final_chunks.append(chunk)
        else:
            # Split by character if a single line is too long
            for i in range(0, len(chunk), chunk_size):
                final_chunks.append(chunk[i:i + chunk_size])

    return final_chunks


def rich_text_chunks(text):
  return [{"text": {"content": chunk}} for chunk in split_text_into_chunks(text)]


def heading_block(text):
  return {
    "object": "block",
    "type": "heading_1",
    "heading_1": {
      "rich_text": [{"type": "text", "text": {"content": text}}]
    }
  }


def table_row(cell_texts):
  return {
    "type": "table_row",
    "table_row": {
      "cells": [
        [{
          "type": "text",
          "text": {"content": text[:MAX_RICH_TEXT_LENGTH]},
          "plain_text": text[:MAX_RICH_TEXT_LENGTH],
        }]
        for text in cell_texts
      ]
    }
  }


def build_column_tables(col_names_and_data, column_descriptions):
  """One or more 3-wide tables of (column, type, description), each capped at
  MAX_ROWS_PER_TABLE rows plus its own header row."""
  header = table_row(['Column', 'Type', 'Description'])
  data_rows = [
    table_row([
      col_name,
      col_data['type'],
      column_descriptions.get(col_name.lower(), ''),
    ])
    for (col_name, col_data) in col_names_and_data
  ]

  tables = []
  for i in range(0, max(len(data_rows), 1), MAX_ROWS_PER_TABLE):
    tables.append({
      "object": "block",
      "type": "table",
      "table": {
        "table_width": 3,
        "has_column_header": True,
        "has_row_header": False,
        "children": [header] + data_rows[i:i + MAX_ROWS_PER_TABLE]
      }
    })
  return tables


def code_blocks(title, code):
  blocks = [heading_block(title)]
  for chunk in split_text_into_chunks(code):
    blocks.append({
      "object": "block",
      "type": "code",
      "code": {
        "rich_text": [{"type": "text", "text": {"content": chunk}}],
        "language": "sql"
      }
    })
  return blocks


def block_weight(block):
  """Blocks-per-request cost of a block: itself plus any nested table rows."""
  if block.get('type') == 'table':
    return 1 + len(block['table']['children'])
  return 1


def batch_children(children):
  """Split a children list into append-request batches that respect Notion's
  100-blocks-per-request cap, counting nested table rows."""
  batches = []
  current, weight = [], 0
  for child in children:
    child_weight = block_weight(child)
    if current and weight + child_weight > MAX_BLOCKS_PER_REQUEST:
      batches.append(current)
      current, weight = [], 0
    current.append(child)
    weight += child_weight
  if current:
    batches.append(current)
  return batches


def stable_json(value):
  """dbt list fields like depends_on.nodes come back in a different order on
  every full parse; sort them so the rendered text and sync hash are
  parse-order independent."""
  if isinstance(value, dict):
    return {key: stable_json(val) for key, val in sorted(value.items())}
  if isinstance(value, list):
    canonical = [stable_json(val) for val in value]
    try:
      return sorted(canonical)
    except TypeError:
      return canonical
  return value


def compute_sync_hash(properties, children):
  payload = json.dumps(
    {'properties': properties, 'children': children}, sort_keys=True
  )
  return hashlib.md5(payload.encode('utf-8')).hexdigest()


def get_existing_sync_hash(record):
  prop = record.get('properties', {}).get(SYNC_HASH_PROPERTY, {})
  return ''.join(rt.get('plain_text', '') for rt in prop.get('rich_text', []))


def find_or_create_database():
  parent_children = make_paginated_request(
    endpoint='blocks/',
    querystring=f'{DATABASE_PARENT_ID}/children',
    method='GET'
  )
  for child in parent_children:
    if ('child_database' in child
        and child['child_database'] == {'title': DATABASE_NAME}):
      database_id = child['id']
      print(f'database {database_id} already exists, proceeding to update records!')
      ensure_database_schema(database_id)
      return database_id

  database_obj = {
    "title": [
      {"type": "text", "text": {"content": DATABASE_NAME, "link": None}}
    ],
    "parent": {"type": "page_id", "page_id": DATABASE_PARENT_ID},
    "properties": DATABASE_PROPERTIES_SCHEMA,
  }
  print('creating database')
  database_creation_resp = make_request(
    endpoint='databases/',
    querystring='',
    method='POST',
    json=database_obj
  )
  database_id = database_creation_resp['id']
  print(f'\ncreated database {database_id}, proceeding to create records!')
  return database_id


def ensure_database_schema(database_id):
  """Add missing properties and drop retired ones on an existing database."""
  database = make_request(
    endpoint='databases/',
    querystring=database_id,
    method='GET'
  )
  properties_patch = {}
  for name in RETIRED_PROPERTIES:
    if name in database.get('properties', {}):
      properties_patch[name] = None
  if SYNC_HASH_PROPERTY not in database.get('properties', {}):
    properties_patch[SYNC_HASH_PROPERTY] = {'rich_text': {}}
  if properties_patch:
    print(f'updating database schema: {sorted(properties_patch)}')
    make_request(
      endpoint='databases/',
      querystring=database_id,
      method='PATCH',
      json={'properties': properties_patch}
    )


def build_record(model_name, data, catalog_nodes, database_id):
  """Returns (record_obj, children, sync_hash) for one model."""
  column_descriptions = {name.lower(): metadata['description']
                         for name, metadata in data['columns'].items()}
  col_names_and_data = list(get_paths_or_empty(
    catalog_nodes, [[model_name, 'columns']], {}
  ).items())

  raw_code = data['raw_code'] if 'raw_code' in data else data['raw_sql']
  compiled_code = (data['compiled_code'] if 'compiled_code' in data
                   else data['compiled_sql'])

  children = [
    {
      "object": "block",
      "type": "table_of_contents",
      "table_of_contents": {"color": "default"}
    },
    heading_block('Columns'),
    *build_column_tables(col_names_and_data, column_descriptions),
    *code_blocks('Raw Code', raw_code),
    *code_blocks('Compiled Code', compiled_code),
  ]

  properties = {
    "Name": {
      "title": [{"text": {"content": data['name']}}]
    },
    "Description": {"rich_text": rich_text_chunks(data['description'])},
    "Owner": {
      "rich_text": rich_text_chunks(str(get_owner(data, catalog_nodes, model_name)))
    },
    "Relation": {"rich_text": rich_text_chunks(data['relation_name'])},
    "Depends On": {
      "rich_text": rich_text_chunks(json.dumps(stable_json(data['depends_on'])))
    },
    "Tags": {"rich_text": rich_text_chunks(json.dumps(stable_json(data['tags'])))},
  }
  sync_hash = compute_sync_hash(properties, children)

  record_obj = {
    "parent": {"database_id": database_id},
    "properties": properties,
  }
  return record_obj, children, sync_hash


def write_record(model_name, record_obj, children, sync_hash, database_id):
  """Create or update one model's record. Returns 'created', 'updated' or
  'skipped'.

  The sync hash is written last, only after the children rewrite succeeded:
  a record whose content write failed keeps a stale hash and is retried on
  the next run instead of being skipped forever."""
  record_query_resp = make_request(
    endpoint='databases/',
    querystring=f'{database_id}/query',
    method='POST',
    json={
      "filter": {
        "property": "Name",
        "title": {
          "equals": record_obj['properties']['Name']['title'][0]['text']['content']
        }
      }
    }
  )

  if record_query_resp['results']:
    record = record_query_resp['results'][0]
    if get_existing_sync_hash(record) == sync_hash:
      print(f'{model_name} unchanged, skipping')
      return 'skipped'
    print(f'\nupdating {model_name} record')
    outcome = 'updated'
    record_id = record['id']

    # children can't be updated via record update, so we'll delete and re-add
    existing_children = make_paginated_request(
      endpoint='blocks/',
      querystring=f'{record_id}/children',
      method='GET'
    )
    for record_child in existing_children:
      make_request(
        endpoint='blocks/',
        querystring=record_child['id'],
        method='DELETE'
      )
  else:
    print(f'\ncreating {model_name} record')
    outcome = 'created'
    record_creation_resp = make_request(
      endpoint='pages/',
      querystring='',
      method='POST',
      json=record_obj
    )
    record_id = record_creation_resp['id']

  for batch in batch_children(children):
    make_request(
      endpoint='blocks/',
      querystring=f'{record_id}/children',
      method='PATCH',
      json={"children": batch}
    )

  properties_with_hash = dict(record_obj['properties'])
  properties_with_hash[SYNC_HASH_PROPERTY] = {
    'rich_text': [{'text': {'content': sync_hash}}]
  }
  make_request(
    endpoint=f'pages/{record_id}',
    querystring='',
    method='PATCH',
    json={'properties': properties_with_hash}
  )
  return outcome


def main(argv=None):
  if argv is None:
    argv = sys.argv
  dbt_project_dir = argv[1]
  model_records_to_write = argv[2:] # 'all' or list of model names
  print(f'Model records to write: {model_records_to_write}')

  ###### load nodes from dbt docs ######
  with open(f'{dbt_project_dir}/target/manifest.json', encoding='utf-8') as f:
    manifest = json.load(f)
    manifest_nodes = manifest['nodes']

  with open(f'{dbt_project_dir}/target/catalog.json', encoding='utf-8') as f:
    catalog = json.load(f)
    catalog_nodes = catalog['nodes']

  models = {node_name: data
            for (node_name, data)
            in manifest_nodes.items() if data['resource_type'] == 'model'}

  database_id = find_or_create_database()

  ##### create / update database records #####
  outcomes = {'created': 0, 'updated': 0, 'skipped': 0}
  failed_models = []
  for model_name, data in sorted(models.items(), reverse=True):
    if model_records_to_write != ['all'] and model_name not in model_records_to_write:
      continue
    try:
      record_obj, children, sync_hash = build_record(
        model_name, data, catalog_nodes, database_id
      )
      outcome = write_record(
        model_name, record_obj, children, sync_hash, database_id
      )
      outcomes[outcome] += 1
    except Exception:
      traceback.print_exc()
      failed_models.append(model_name)

  print(f"\ndone: {outcomes['created']} created, {outcomes['updated']} updated, "
        f"{outcomes['skipped']} skipped, {len(failed_models)} failed")
  if failed_models:
    print('failed models:')
    for model_name in failed_models:
      print(f'  {model_name}')
    return 1
  return 0


if __name__ == '__main__':
  sys.exit(main())
