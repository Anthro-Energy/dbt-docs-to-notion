import json
import os
import sys
import time

import requests


DATABASE_PARENT_ID = os.environ['DATABASE_PARENT_ID']
DATABASE_NAME = os.environ['DATABASE_NAME']
NOTION_TOKEN = os.environ['NOTION_TOKEN']
NUMERIC_ZERO_VALUE = -1


def make_request(endpoint, querystring='', method='GET', **request_kwargs):
  time.sleep(0.34) # notion api limit is 3 requests per second

  headers = {
    'Authorization': NOTION_TOKEN,
    'Content-Type': 'application/json',
    'Notion-Version': '2022-02-22'
  }
  url = f'https://api.notion.com/v1/{endpoint}{querystring}'
  resp = requests.request(method, url, headers=headers, **request_kwargs)

  if not resp.status_code == 200:
    raise Exception(
      f"Request returned status code {resp.status_code}\nResponse text: {resp.text}"
    )

  return resp.json()


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


def split_text_into_chunks(text, chunk_size=2000):
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

  ###### create database if not exists ######
  children_query_resp = make_request(
    endpoint='blocks/',
    querystring=f'{DATABASE_PARENT_ID}/children',
    method='GET'
  )

  database_id = ''
  for child in children_query_resp['results']:
    if('child_database' in child
        and child['child_database'] == {'title': DATABASE_NAME}):
      database_id = child['id']
      break

  if database_id:
    print(f'database {database_id} already exists, proceeding to update records!')
  else:
    database_obj = {
      "title": [
        {
          "type": "text",
          "text": {
            "content": DATABASE_NAME,
            "link": None
          }
        }
      ],
      "parent": {
        "type": "page_id",
        "page_id": DATABASE_PARENT_ID
      },
      "properties": {
        "Name": {
          "title": {}
        },
        "Description": {
          "rich_text": {}
        },
        "Owner": {
          "rich_text": {}
        },
        "Relation": {
          "rich_text": {}
        },
        "Approx Rows": {
          "number": {
            "format": "number_with_commas"
          }
        },
        "Approx GB": {
          "number": {
            "format": "number_with_commas"
          }
        },
        "Depends On": {
          "rich_text": {}
        },
        "Tags": {
          "rich_text": {}
        }
      }
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

  ##### create / update database records #####
  for model_name, data in sorted(list(models.items()), reverse=True):
    try:
      if model_records_to_write == ['all'] or model_name in model_records_to_write:
        # form record object
        column_descriptions = {name.lower(): metadata['description']
                              for name, metadata
                              in data['columns'].items()}
  
        columns_table_children_obj = [
          {
            "type": "table_row",
            "table_row": {
              "cells": [
                [
                  {
                    "type": "text",
                    "text": {
                      "content": "Column"
                    },
                    "plain_text": "Column"
                  }
                ],
                [
                  {
                    "type": "text",
                    "text": {
                      "content": "Type"
                    },
                    "plain_text": "Type"
                  }
                ],
                [
                  {
                    "type": "text",
                    "text": {
                      "content": "Description"
                    },
                    "plain_text": "Description"
                  }
                ]
              ]
            }
          }
        ]
        col_names_and_data = list(get_paths_or_empty(
          catalog_nodes,
          [[model_name, 'columns']],
          {}
        ).items())
        for (col_name, col_data) in col_names_and_data: # notion api limit is 100 table rows
          columns_table_children_obj.append(
            {
              "type": "table_row",
              "table_row": {
                "cells": [
                  [
                    {
                      "type": "text",
                      "text": {
                        "content": col_name
                      },
                      "plain_text": col_name
                    }
                  ],
                  [
                    {
                      "type": "text",
                      "text": {
                        "content": col_data['type']
                      },
                      "plain_text": col_data['type']
                    }
                  ],
                  [
                    {
                      "type": "text",
                      "text": {
                        "content": (
                          column_descriptions[col_name.lower()]
                          if col_name.lower() in column_descriptions
                          else ''
                        )
                      },
                      "plain_text": (
                        column_descriptions[col_name.lower()]
                        if col_name.lower() in column_descriptions
                        else ''
                      )
                    }
                  ]
                ]
              }
            }
          )
        if len(col_names_and_data) > 98:
          # make that columns have been truncated
          columns_table_children_obj.append(
            {
              "type": "table_row",
              "table_row": {
                "cells": [
                  [
                    {
                      "type": "text",
                      "text": {
                        "content": "..."
                      },
                      "plain_text": "..."
                    }
                  ],
                  [
                    {
                      "type": "text",
                      "text": {
                        "content": "..."
                      },
                      "plain_text": "..."
                    }
                  ],
                  [
                    {
                      "type": "text",
                      "text": {
                        "content": "..."
                      },
                      "plain_text": "..."
                    }
                  ]
                ]
              }
            }
          )
  
        # Split raw code into chunks
        raw_code = data['raw_code'] if 'raw_code' in data else data['raw_sql']
        raw_code_chunks = split_text_into_chunks(raw_code)
        raw_code_blocks = [
            # Add the heading only once
            {
                "object": "block",
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": { "content": "Raw Code" }
                        }
                    ]
                }
            }
        ]

        # Add each chunk as a separate code block without continuation headers
        for chunk in raw_code_chunks:
            raw_code_blocks.append({
                "object": "block",
                "type": "code",
                "code": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": chunk
                            }
                        }
                    ],
                    "language": "sql"
                }
            })

        # Split compiled code into chunks
        compiled_code = data['compiled_code'] if 'compiled_code' in data else data['compiled_sql']
        compiled_code_chunks = split_text_into_chunks(compiled_code)
        compiled_code_blocks = [
            # Add the heading only once
            {
                "object": "block",
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": { "content": "Compiled Code" }
                        }
                    ]
                }
            }
        ]

        # Add each chunk as a separate code block without continuation headers
        for chunk in compiled_code_chunks:
            compiled_code_blocks.append({
                "object": "block",
                "type": "code",
                "code": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": chunk
                            }
                        }
                    ],
                    "language": "sql"
                }
            })

        # Update record_children_obj to use the chunked code blocks
        record_children_obj = [
            # Table of contents
            {
                "object": "block",
                "type": "table_of_contents",
                "table_of_contents": {
                    "color": "default"
                }
            },
            # Columns
            {
                "object": "block",
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": { "content": "Columns" }
                        }
                    ]
                }
            },
            {
                "object": "block",
                "type": "table",
                "table": {
                    "table_width": 3,
                    "has_column_header": True,
                    "has_row_header": False,
                    "children": columns_table_children_obj
                }
            }
        ]

        # Add all raw code blocks
        record_children_obj.extend(raw_code_blocks)

        # Add all compiled code blocks
        record_children_obj.extend(compiled_code_blocks)
  
        # Split description into chunks
        description_chunks = split_text_into_chunks(data['description'])
        description_rich_text = [{"text": {"content": chunk}} for chunk in description_chunks]
        
        # Split owner into chunks
        owner_text = str(get_owner(data, catalog_nodes, model_name))
        owner_chunks = split_text_into_chunks(owner_text)
        owner_rich_text = [{"text": {"content": chunk}} for chunk in owner_chunks]
        
        # Split relation_name into chunks
        relation_chunks = split_text_into_chunks(data['relation_name'])
        relation_rich_text = [{"text": {"content": chunk}} for chunk in relation_chunks]
        
        # Split depends_on into chunks
        depends_on_text = json.dumps(data['depends_on'])
        depends_on_chunks = split_text_into_chunks(depends_on_text)
        depends_on_rich_text = [{"text": {"content": chunk}} for chunk in depends_on_chunks]
        
        # Split tags into chunks
        tags_text = json.dumps(data['tags'])
        tags_chunks = split_text_into_chunks(tags_text)
        tags_rich_text = [{"text": {"content": chunk}} for chunk in tags_chunks]
        
        record_obj = {
          "parent": {
            "database_id": database_id
          },
          "properties": {
            "Name": {
              "title": [
                {
                  "text": {
                    "content": data['name']
                  }
                }
              ]
            },
            "Description": {
              "rich_text": description_rich_text
            },
            "Owner": {
              "rich_text": owner_rich_text
            },
            "Relation": {
              "rich_text": relation_rich_text
            },
            "Approx Rows": {
              "number": get_paths_or_empty(
                catalog_nodes,
                [[model_name, 'stats', 'num_rows', 'value'],
                 [model_name, 'stats', 'row_count', 'value']],
                NUMERIC_ZERO_VALUE
              )
            },
            "Approx GB": {
              "number": get_paths_or_empty(
                catalog_nodes,
                [[model_name, 'stats', 'bytes', 'value'],
                 [model_name, 'stats', 'num_bytes', 'value']],
                NUMERIC_ZERO_VALUE
              ) / 1e9
            },
            "Depends On": {
              "rich_text": depends_on_rich_text
            },
            "Tags": {
              "rich_text": tags_rich_text
            }
          }
        }
  
        ###### query to see if record already exists ######
        query_obj = {
          "filter": {
            "property": "Name",
            "title": {
              "equals": data['name']
            }
          }
        }
        record_query_resp = make_request(
          endpoint='databases/',
          querystring=f'{database_id}/query',
          method='POST',
          json=query_obj
        )
  
        if record_query_resp['results']:
          print(f'\nupdating {model_name} record')
          record_id = record_query_resp['results'][0]['id']
          _record_update_resp = make_request(
            endpoint=f'pages/{record_id}',
            querystring='',
            method='PATCH',
            json=record_obj
          )
  
          # children can't be updated via record update, so we'll delete and re-add
          record_children_resp = make_request(
            endpoint='blocks/',
            querystring=f'{record_id}/children',
            method='GET'
          )
          for record_child in record_children_resp['results']:
            record_child_id = record_child['id']
            _record_child_deletion_resp = make_request(
              endpoint='blocks/',
              querystring=record_child_id,
              method='DELETE'
            )
  
          # Handle batching for tables with many columns
          if len(columns_table_children_obj) >= 100:
            batch_size = 98
            # First, create all column tables
            for i in range(0, len(columns_table_children_obj), batch_size):
              batched_array = columns_table_children_obj[i:i + batch_size]
              
              if i == 0:
                # First batch includes table of contents and column heading
                record_children_obj = [
                  # Table of contents
                  {
                    "object": "block",
                    "type": "table_of_contents",
                    "table_of_contents": {
                      "color": "default"
                    }
                  },
                  # Columns
                  {
                    "object": "block",
                    "type": "heading_1",
                    "heading_1": {
                      "rich_text": [
                        {
                          "type": "text",
                          "text": { "content": "Columns" }
                        }
                      ]
                    }
                  },
                  {
                    "object": "block",
                    "type": "table",
                    "table": {
                      "table_width": 3,
                      "has_column_header": True,
                      "has_row_header": False,
                      "children": batched_array
                    }
                  }
                ]
              else:
                # Subsequent batches only include additional column tables without headers
                record_children_obj = [
                  {
                    "object": "block",
                    "type": "table",
                    "table": {
                      "table_width": 3,
                      "has_column_header": True,
                      "has_row_header": False,
                      "children": batched_array
                    }
                  }
                ]
              
              _record_children_replacement_resp = make_request(
                endpoint='blocks/',
                querystring=f'{record_id}/children',
                method='PATCH',
                json={"children": record_children_obj}
              )
            
            # After all column tables are created, add the code blocks
            _record_children_replacement_resp = make_request(
              endpoint='blocks/',
              querystring=f'{record_id}/children',
              method='PATCH',
              json={"children": raw_code_blocks + compiled_code_blocks}
            )
          else:
            # For tables with fewer columns, use the original approach
            _record_children_replacement_resp = make_request(
              endpoint='blocks/',
              querystring=f'{record_id}/children',
              method='PATCH',
              json={"children": record_children_obj}
            )
  
        else:
          print(f'\ncreating {model_name} record')
          
          # Handle batching for tables with many columns
          if len(columns_table_children_obj) >= 100:
            batch_size = 98
            # First, create all column tables
            for i in range(0, len(columns_table_children_obj), batch_size):
              batched_array = columns_table_children_obj[i:i + batch_size]
              
              if i == 0:
                # First batch includes table of contents and column heading
                record_children_obj = [
                  # Table of contents
                  {
                    "object": "block",
                    "type": "table_of_contents",
                    "table_of_contents": {
                      "color": "default"
                    }
                  },
                  # Columns
                  {
                    "object": "block",
                    "type": "heading_1",
                    "heading_1": {
                      "rich_text": [
                        {
                          "type": "text",
                          "text": { "content": "Columns" }
                        }
                      ]
                    }
                  },
                  {
                    "object": "block",
                    "type": "table",
                    "table": {
                      "table_width": 3,
                      "has_column_header": True,
                      "has_row_header": False,
                      "children": batched_array
                    }
                  }
                ]
              else:
                # Subsequent batches only include additional column tables without headers
                record_children_obj = [
                  {
                    "object": "block",
                    "type": "table",
                    "table": {
                      "table_width": 3,
                      "has_column_header": True,
                      "has_row_header": False,
                      "children": batched_array
                    }
                  }
                ]
              
              _record_children_replacement_resp = make_request(
                endpoint='blocks/',
                querystring=f'{record_id}/children',
                method='PATCH',
                json={"children": record_children_obj}
              )
          else:
            # For tables with fewer columns, use the original approach
            record_obj['children'] = record_children_obj
            _record_creation_resp = make_request(
              endpoint='pages/',
              querystring='',
              method='POST',
              json=record_obj
            )
    except Exception as e:
      print(e)
      continue


if __name__ == '__main__':
  main()
