# Mock Data for dbt and Notion API
import os

# Mock dbt Data
DBT_MOCK_CATALOG = {
  "nodes": {
    "model.test.model_1": {
      "columns": {
        "column_1": {
          "type": "TEXT"
        },
        "column_2": {
          "type": "TEXT"
        },
      },
      "metadata": {
        "owner": "owner@example.com"
      },
      "stats": {
        "row_count": {
          "value": 1,
        },
        "bytes": {
          "value": 1000000,
        },
      },
    },
  },
}

DBT_MOCK_MANIFEST = {
  "nodes": {
    "model.test.model_1": {
      "resource_type": "model",
      "columns": {
        "column_1": {
          "description": "Description for column 1"
        },
        "column_2": {
          "description": "Description for column 2"
        },
      },
      "raw_code": "SELECT 1",
      "compiled_code": "SELECT 1",
      "name": "model_1",
      "description": "Description for model 1",
      "relation_name": "model.test.model_1",
      "depends_on": ["model.test.model_2"],
      "tags": ["tag1", "tag2"],
    },
  },
}

# Mock Notion API Responses
NOTION_MOCK_EXISTENT_CHILD_PAGE_QUERY = {
  "results": [
    {
      "id": "mock_child_id",
      "child_database": {
        "title": os.environ['DATABASE_NAME'],
      },
    },
  ],
}

NOTION_MOCK_EXISTENT_DATABASE_RECORDS_QUERY = {
  "results": [
    {
      "id": "mock_record_id",
    },
  ],
}

NOTION_MOCK_NONEXISTENT_QUERY = {
  "results": [],
}

NOTION_MOCK_DATABASE_CREATE = {
  "id": "mock_database_id",
}

NOTION_MOCK_RECORD_CREATE = {
  "id": "mock_record_id",
}

NOTION_MOCK_RECORD_CHILDREN = {
  "results": [
    {
      "id": "mock_block_id",
    },
  ],
}

# Database schema as this version of the exporter creates it
NOTION_MOCK_DATABASE_RETRIEVE_CURRENT = {
  "id": "mock_child_id",
  "properties": {
    "Name": {"title": {}},
    "Description": {"rich_text": {}},
    "Owner": {"rich_text": {}},
    "Relation": {"rich_text": {}},
    "Depends On": {"rich_text": {}},
    "Tags": {"rich_text": {}},
    "Sync Hash": {"rich_text": {}},
  },
}

# Database schema from before Sync Hash existed and Approx Rows/GB were retired
NOTION_MOCK_DATABASE_RETRIEVE_LEGACY = {
  "id": "mock_child_id",
  "properties": {
    "Name": {"title": {}},
    "Description": {"rich_text": {}},
    "Owner": {"rich_text": {}},
    "Relation": {"rich_text": {}},
    "Approx Rows": {"number": {"format": "number_with_commas"}},
    "Approx GB": {"number": {"format": "number_with_commas"}},
    "Depends On": {"rich_text": {}},
    "Tags": {"rich_text": {}},
  },
}
