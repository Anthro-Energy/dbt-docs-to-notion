import json
import os
import unittest
from unittest.mock import patch

from dbt_docs_to_notion import main, SYNC_HASH_PROPERTY
from tests.mock_data import (
  DBT_MOCK_MANIFEST,
  DBT_MOCK_CATALOG,
  NOTION_MOCK_EXISTENT_CHILD_PAGE_QUERY,
  NOTION_MOCK_EXISTENT_DATABASE_RECORDS_QUERY,
  NOTION_MOCK_NONEXISTENT_QUERY,
  NOTION_MOCK_DATABASE_CREATE,
  NOTION_MOCK_RECORD_CREATE,
  NOTION_MOCK_RECORD_CHILDREN,
  NOTION_MOCK_DATABASE_RETRIEVE_CURRENT,
  NOTION_MOCK_DATABASE_RETRIEVE_LEGACY,
)


class TestDbtDocsToNotionIntegration(unittest.TestCase):

    def setUp(self):
        self.mock_json_load = patch('dbt_docs_to_notion.json.load').start()
        self.mock_json_load.side_effect = [DBT_MOCK_MANIFEST, DBT_MOCK_CATALOG]
        patch('dbt_docs_to_notion.open', new_callable=unittest.mock.mock_open, read_data="data").start()
        self.comparison_catalog = DBT_MOCK_CATALOG['nodes']['model.test.model_1']
        self.comparison_manifest = DBT_MOCK_MANIFEST['nodes']['model.test.model_1']
        self.recorded_requests = []

    def tearDown(self):
        patch.stopall()

    def _verify_database_obj(self, database_obj):
      title = database_obj['title'][0]
      self.assertEqual(title['type'], 'text')
      self.assertEqual(title['text']['content'], os.environ['DATABASE_NAME'])
      parent = database_obj['parent']
      self.assertEqual(parent['type'], 'page_id')
      self.assertEqual(parent['page_id'], os.environ['DATABASE_PARENT_ID'])
      properties = database_obj['properties']
      self.assertEqual(properties['Name'], {'title': {}})
      self.assertEqual(properties['Description'], {'rich_text': {}})
      self.assertEqual(properties['Owner'], {'rich_text': {}})
      self.assertEqual(properties['Relation'], {'rich_text': {}})
      self.assertEqual(properties['Depends On'], {'rich_text': {}})
      self.assertEqual(properties['Tags'], {'rich_text': {}})
      self.assertEqual(properties[SYNC_HASH_PROPERTY], {'rich_text': {}})
      self.assertNotIn('Approx Rows', properties)
      self.assertNotIn('Approx GB', properties)

    def _verify_record_properties(self, properties):
      self.assertEqual(properties['Name']['title'][0]['text']['content'], self.comparison_manifest['name'])
      self.assertEqual(properties['Description']['rich_text'][0]['text']['content'], self.comparison_manifest['description'])
      self.assertEqual(properties['Owner']['rich_text'][0]['text']['content'], self.comparison_catalog['metadata']['owner'])
      self.assertEqual(properties['Relation']['rich_text'][0]['text']['content'], self.comparison_manifest['relation_name'])
      self.assertEqual(properties['Depends On']['rich_text'][0]['text']['content'], json.dumps(self.comparison_manifest['depends_on']))
      self.assertEqual(properties['Tags']['rich_text'][0]['text']['content'], json.dumps(self.comparison_manifest['tags']))
      self.assertNotIn('Approx Rows', properties)
      self.assertNotIn('Approx GB', properties)

    def _verify_record_obj(self, record_obj):
      parent = record_obj['parent']
      self.assertEqual(parent['database_id'], NOTION_MOCK_DATABASE_CREATE['id'])
      self._verify_record_properties(record_obj['properties'])
      # the sync hash is only written after the children write succeeds, and
      # children are appended in separate requests
      self.assertNotIn(SYNC_HASH_PROPERTY, record_obj['properties'])
      self.assertNotIn('children', record_obj)

    def _verify_record_children_obj(self, record_children_obj):
      toc_child_block = record_children_obj[0]
      self.assertEqual(toc_child_block['object'], 'block')
      self.assertEqual(toc_child_block['type'], 'table_of_contents')
      columns_header_child_block = record_children_obj[1]
      self.assertEqual(columns_header_child_block['object'], 'block')
      self.assertEqual(columns_header_child_block['type'], 'heading_1')
      self.assertEqual(columns_header_child_block['heading_1']['rich_text'][0]['text']['content'], 'Columns')
      columns_child_block = record_children_obj[2]
      self.assertEqual(columns_child_block['object'], 'block')
      self.assertEqual(columns_child_block['type'], 'table')
      self.assertEqual(columns_child_block['table']['table_width'], 3)
      self.assertEqual(columns_child_block['table']['has_column_header'], True)
      self.assertEqual(columns_child_block['table']['has_row_header'], False)
      columns_table_children_obj = columns_child_block['table']['children']
      columns_table_header_row = columns_table_children_obj[0]
      self.assertEqual(columns_table_header_row['type'], 'table_row')
      self.assertEqual(columns_table_header_row['table_row']['cells'][0][0]['plain_text'], 'Column')
      self.assertEqual(columns_table_header_row['table_row']['cells'][1][0]['plain_text'], 'Type')
      self.assertEqual(columns_table_header_row['table_row']['cells'][2][0]['plain_text'], 'Description')
      columns_table_row = columns_table_children_obj[1]
      self.assertEqual(columns_table_row['type'], 'table_row')
      self.assertEqual(columns_table_row['table_row']['cells'][0][0]['plain_text'], list(self.comparison_catalog['columns'].keys())[0])
      self.assertEqual(columns_table_row['table_row']['cells'][1][0]['plain_text'], list(self.comparison_catalog['columns'].values())[0]['type'])
      self.assertEqual(columns_table_row['table_row']['cells'][2][0]['plain_text'], list(self.comparison_manifest['columns'].values())[0]['description'])
      raw_code_header_child_block = record_children_obj[3]
      self.assertEqual(raw_code_header_child_block['object'], 'block')
      self.assertEqual(raw_code_header_child_block['type'], 'heading_1')
      self.assertEqual(raw_code_header_child_block['heading_1']['rich_text'][0]['text']['content'], 'Raw Code')
      raw_code_child_block = record_children_obj[4]
      self.assertEqual(raw_code_child_block['object'], 'block')
      self.assertEqual(raw_code_child_block['type'], 'code')
      self.assertEqual(raw_code_child_block['code']['language'], 'sql')
      self.assertEqual(raw_code_child_block['code']['rich_text'][0]['text']['content'], self.comparison_manifest['raw_code'])
      compiled_code_header_child_block = record_children_obj[5]
      self.assertEqual(compiled_code_header_child_block['object'], 'block')
      self.assertEqual(compiled_code_header_child_block['type'], 'heading_1')
      self.assertEqual(compiled_code_header_child_block['heading_1']['rich_text'][0]['text']['content'], 'Compiled Code')
      compiled_code_child_block = record_children_obj[6]
      self.assertEqual(compiled_code_child_block['object'], 'block')
      self.assertEqual(compiled_code_child_block['type'], 'code')
      self.assertEqual(compiled_code_child_block['code']['language'], 'sql')
      self.assertEqual(compiled_code_child_block['code']['rich_text'][0]['text']['content'], self.comparison_manifest['compiled_code'])

    def _verify_hash_patch(self, properties):
      self._verify_record_properties(properties)
      sync_hash = properties[SYNC_HASH_PROPERTY]['rich_text'][0]['text']['content']
      self.assertRegex(sync_hash, r'^[0-9a-f]{32}$')
      return sync_hash

    @patch('dbt_docs_to_notion.make_request')
    def test_create_new_database(self, mock_make_request):
        def _mocked_make_request(endpoint, querystring='', method='GET', **request_kwargs):
          self.recorded_requests.append((endpoint, method))
          if endpoint == 'blocks/' and method == 'GET':
              return NOTION_MOCK_NONEXISTENT_QUERY
          elif endpoint == 'databases/' and querystring == '' and method == 'POST':
              self._verify_database_obj(request_kwargs['json'])
              return NOTION_MOCK_DATABASE_CREATE
          elif endpoint == 'databases/' and '/query' in querystring and method == 'POST':
              return NOTION_MOCK_NONEXISTENT_QUERY
          elif endpoint == 'pages/' and method == 'POST':
              self._verify_record_obj(request_kwargs['json'])
              return NOTION_MOCK_RECORD_CREATE
          elif endpoint == 'blocks/' and method == 'PATCH':
              self._verify_record_children_obj(request_kwargs['json']['children'])
              return {}
          elif endpoint == 'pages/mock_record_id' and method == 'PATCH':
              self._verify_hash_patch(request_kwargs['json']['properties'])
              return {}
        mock_make_request.side_effect = _mocked_make_request

        self.assertEqual(main(argv=['prog', 'project_dir', 'all']), 0)

        self.assertEqual(
          self.recorded_requests,
          [
            ('blocks/', 'GET'),
            ('databases/', 'POST'),
            ('databases/', 'POST'),
            ('pages/', 'POST'),
            ('blocks/', 'PATCH'),
            ('pages/mock_record_id', 'PATCH'),
          ]
        )

    @patch('dbt_docs_to_notion.make_request')
    def test_update_existing_database(self, mock_make_request):
        def _mocked_make_request(endpoint, querystring='', method='GET', **request_kwargs):
          self.recorded_requests.append((endpoint, method))
          if endpoint == 'blocks/' and method == 'GET':
              if querystring.startswith('mock_record_id'):
                  return NOTION_MOCK_RECORD_CHILDREN
              return NOTION_MOCK_EXISTENT_CHILD_PAGE_QUERY
          elif endpoint == 'databases/' and method == 'GET':
              return NOTION_MOCK_DATABASE_RETRIEVE_CURRENT
          elif endpoint == 'databases/' and '/query' in querystring and method == 'POST':
              return NOTION_MOCK_EXISTENT_DATABASE_RECORDS_QUERY
          elif endpoint == 'blocks/' and method == 'DELETE':
              return {}
          elif endpoint == 'blocks/' and method == 'PATCH':
              self._verify_record_children_obj(request_kwargs['json']['children'])
              return {}
          elif endpoint == 'pages/mock_record_id' and method == 'PATCH':
              self._verify_hash_patch(request_kwargs['json']['properties'])
              return {}
        mock_make_request.side_effect = _mocked_make_request

        self.assertEqual(main(argv=['prog', 'project_dir', 'all']), 0)

        self.assertEqual(
          self.recorded_requests,
          [
            ('blocks/', 'GET'),
            ('databases/', 'GET'),
            ('databases/', 'POST'),
            ('blocks/', 'GET'),
            ('blocks/', 'DELETE'),
            ('blocks/', 'PATCH'),
            ('pages/mock_record_id', 'PATCH'),
          ]
        )

    @patch('dbt_docs_to_notion.make_request')
    def test_legacy_schema_is_migrated(self, mock_make_request):
        def _mocked_make_request(endpoint, querystring='', method='GET', **request_kwargs):
          self.recorded_requests.append((endpoint, method))
          if endpoint == 'blocks/' and method == 'GET':
              return NOTION_MOCK_EXISTENT_CHILD_PAGE_QUERY
          elif endpoint == 'databases/' and method == 'GET':
              return NOTION_MOCK_DATABASE_RETRIEVE_LEGACY
          elif endpoint == 'databases/' and method == 'PATCH':
              self.assertEqual(
                request_kwargs['json'],
                {'properties': {
                  'Approx Rows': None,
                  'Approx GB': None,
                  SYNC_HASH_PROPERTY: {'rich_text': {}},
                }}
              )
              return {}
        mock_make_request.side_effect = _mocked_make_request

        # write no model records, just exercise the schema migration
        self.assertEqual(main(argv=['prog', 'project_dir', 'no_such_model']), 0)

        self.assertEqual(
          self.recorded_requests,
          [
            ('blocks/', 'GET'),
            ('databases/', 'GET'),
            ('databases/', 'PATCH'),
          ]
        )

    @patch('dbt_docs_to_notion.make_request')
    def test_unchanged_record_is_skipped(self, mock_make_request):
        written_hashes = []

        def _mocked_make_request(endpoint, querystring='', method='GET', **request_kwargs):
          self.recorded_requests.append((endpoint, method))
          if endpoint == 'blocks/' and method == 'GET':
              return NOTION_MOCK_EXISTENT_CHILD_PAGE_QUERY
          elif endpoint == 'databases/' and method == 'GET':
              return NOTION_MOCK_DATABASE_RETRIEVE_CURRENT
          elif endpoint == 'databases/' and '/query' in querystring and method == 'POST':
              if not written_hashes:
                  return NOTION_MOCK_NONEXISTENT_QUERY
              return {
                'results': [{
                  'id': 'mock_record_id',
                  'properties': {
                    SYNC_HASH_PROPERTY: {
                      'rich_text': [{'plain_text': written_hashes[-1]}]
                    }
                  },
                }]
              }
          elif endpoint == 'pages/' and method == 'POST':
              return NOTION_MOCK_RECORD_CREATE
          elif endpoint == 'blocks/' and method == 'PATCH':
              return {}
          elif endpoint == 'pages/mock_record_id' and method == 'PATCH':
              written_hashes.append(self._verify_hash_patch(request_kwargs['json']['properties']))
              return {}
        mock_make_request.side_effect = _mocked_make_request

        # first run creates the record and stamps its sync hash
        self.assertEqual(main(argv=['prog', 'project_dir', 'all']), 0)
        self.assertEqual(len(written_hashes), 1)

        # second run sees a matching hash and writes nothing
        self.mock_json_load.side_effect = [DBT_MOCK_MANIFEST, DBT_MOCK_CATALOG]
        self.recorded_requests = []
        self.assertEqual(main(argv=['prog', 'project_dir', 'all']), 0)
        self.assertEqual(
          self.recorded_requests,
          [
            ('blocks/', 'GET'),
            ('databases/', 'GET'),
            ('databases/', 'POST'),
          ]
        )
        self.assertEqual(len(written_hashes), 1)

    @patch('dbt_docs_to_notion.make_request')
    def test_failed_model_gives_nonzero_exit(self, mock_make_request):
        def _mocked_make_request(endpoint, querystring='', method='GET', **request_kwargs):
          self.recorded_requests.append((endpoint, method))
          if endpoint == 'blocks/' and method == 'GET':
              return NOTION_MOCK_EXISTENT_CHILD_PAGE_QUERY
          elif endpoint == 'databases/' and method == 'GET':
              return NOTION_MOCK_DATABASE_RETRIEVE_CURRENT
          elif endpoint == 'databases/' and '/query' in querystring and method == 'POST':
              raise Exception('Request returned status code 500')
        mock_make_request.side_effect = _mocked_make_request

        self.assertEqual(main(argv=['prog', 'project_dir', 'all']), 1)


if __name__ == '__main__':
    unittest.main()
