import os
import unittest
from unittest.mock import patch, Mock, call

from dbt_docs_to_notion import (
  make_request,
  make_paginated_request,
  get_paths_or_empty,
  get_owner,
  batch_children,
  build_column_tables,
  MAX_ATTEMPTS,
)
from tests.mock_data import DBT_MOCK_MANIFEST, DBT_MOCK_CATALOG, NOTION_MOCK_DATABASE_CREATE


@patch('dbt_docs_to_notion.time.sleep')
class TestMakeRequest(unittest.TestCase):
    @patch('dbt_docs_to_notion.requests.request')
    def test_valid_request(self, mock_request, _mock_sleep):
        mock_request.return_value = Mock(status_code=200, json=lambda: NOTION_MOCK_DATABASE_CREATE)
        response = make_request("some_endpoint")
        self.assertEqual(response, NOTION_MOCK_DATABASE_CREATE)

    @patch('dbt_docs_to_notion.requests.request')
    def test_bearer_prefix_is_added(self, mock_request, _mock_sleep):
        mock_request.return_value = Mock(status_code=200, json=lambda: {})
        make_request("some_endpoint")
        headers = mock_request.call_args.kwargs['headers']
        token = os.environ['NOTION_TOKEN']
        expected = token if token.startswith('Bearer ') else f'Bearer {token}'
        self.assertEqual(headers['Authorization'], expected)
        self.assertTrue(headers['Authorization'].startswith('Bearer '))

    @patch('dbt_docs_to_notion.requests.request')
    def test_invalid_token_not_retried(self, mock_request, _mock_sleep):
        mock_request.return_value = Mock(
          status_code=401, text='{"message": "API token is invalid."}'
        )
        with self.assertRaises(Exception) as context:
            make_request("some_endpoint")
        self.assertIn("Request returned status code 401", str(context.exception))
        self.assertEqual(mock_request.call_count, 1)

    @patch('dbt_docs_to_notion.requests.request')
    def test_rate_limit_retried_with_retry_after(self, mock_request, mock_sleep):
        mock_request.side_effect = [
          Mock(status_code=429, headers={'Retry-After': '7'}, text='rate limited'),
          Mock(status_code=200, json=lambda: NOTION_MOCK_DATABASE_CREATE),
        ]
        response = make_request("some_endpoint")
        self.assertEqual(response, NOTION_MOCK_DATABASE_CREATE)
        self.assertEqual(mock_request.call_count, 2)
        self.assertIn(call(7.0), mock_sleep.call_args_list)

    @patch('dbt_docs_to_notion.requests.request')
    def test_server_error_exhausts_retries(self, mock_request, _mock_sleep):
        mock_request.return_value = Mock(status_code=500, headers={}, text='server error')
        with self.assertRaises(Exception) as context:
            make_request("some_endpoint")
        self.assertIn("Request returned status code 500", str(context.exception))
        self.assertEqual(mock_request.call_count, MAX_ATTEMPTS)


class TestMakePaginatedRequest(unittest.TestCase):
    @patch('dbt_docs_to_notion.make_request')
    def test_get_follows_cursor(self, mock_make_request):
        mock_make_request.side_effect = [
          {'results': [1, 2], 'has_more': True, 'next_cursor': 'cursor_a'},
          {'results': [3]},
        ]
        results = make_paginated_request('blocks/', 'page_id/children', 'GET')
        self.assertEqual(results, [1, 2, 3])
        self.assertIn('start_cursor=cursor_a', mock_make_request.call_args_list[1].args[1])

    @patch('dbt_docs_to_notion.make_request')
    def test_post_passes_cursor_in_body(self, mock_make_request):
        mock_make_request.side_effect = [
          {'results': [1], 'has_more': True, 'next_cursor': 'cursor_b'},
          {'results': [2]},
        ]
        query = {'filter': {'property': 'Name'}}
        results = make_paginated_request('databases/', 'db_id/query', 'POST', json=query)
        self.assertEqual(results, [1, 2])
        second_body = mock_make_request.call_args_list[1].kwargs['json']
        self.assertEqual(second_body['start_cursor'], 'cursor_b')
        self.assertEqual(second_body['filter'], query['filter'])
        # original body must not be mutated
        self.assertNotIn('start_cursor', query)


class TestBatching(unittest.TestCase):
    def test_small_children_fit_one_batch(self):
        children = [{'type': 'paragraph'} for _ in range(50)]
        self.assertEqual(len(batch_children(children)), 1)

    def test_tables_count_nested_rows(self):
        table = {'type': 'table', 'table': {'children': [{'type': 'table_row'}] * 98}}
        children = [table, table, {'type': 'paragraph'}]
        batches = batch_children(children)
        # each 99-weight table needs its own request; the paragraph packs into
        # the second one at exactly 100
        self.assertEqual(len(batches), 2)
        self.assertEqual(batches[0], [table])
        self.assertEqual(batches[1], [table, {'type': 'paragraph'}])

    def test_wide_models_split_into_multiple_tables(self):
        cols = [(f'col_{i}', {'type': 'TEXT'}) for i in range(200)]
        tables = build_column_tables(cols, {})
        self.assertEqual(len(tables), 3)
        # every table carries its own header row
        for table in tables:
            first_row = table['table']['children'][0]
            self.assertEqual(first_row['table_row']['cells'][0][0]['plain_text'], 'Column')
        total_data_rows = sum(len(t['table']['children']) - 1 for t in tables)
        self.assertEqual(total_data_rows, 200)

    def test_no_columns_still_renders_header_table(self):
        tables = build_column_tables([], {})
        self.assertEqual(len(tables), 1)
        self.assertEqual(len(tables[0]['table']['children']), 1)


class TestGetPathsOrEmpty(unittest.TestCase):
    def test_valid_path(self):
        result = get_paths_or_empty(DBT_MOCK_MANIFEST["nodes"]["model.test.model_1"], [["description"]])
        self.assertEqual(result, "Description for model 1")

    def test_invalid_path(self):
        result = get_paths_or_empty(DBT_MOCK_MANIFEST["nodes"]["model.test.model_1"], [["invalid_path"]])
        self.assertEqual(result, '')


class TestGetOwner(unittest.TestCase):
    def test_owner_in_config(self):
        data = DBT_MOCK_MANIFEST["nodes"]["model.test.model_1"]
        catalog_nodes = DBT_MOCK_CATALOG["nodes"]
        result = get_owner(data, catalog_nodes, "model.test.model_1")
        self.assertEqual(result, "owner@example.com")


if __name__ == '__main__':
    unittest.main()
