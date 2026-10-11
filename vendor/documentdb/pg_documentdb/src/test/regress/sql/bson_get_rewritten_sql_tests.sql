SET search_path TO documentdb_api_catalog, documentdb_api, documentdb_api_internal, documentdb_core, public;
SET documentdb.next_collection_id TO 1984000;
SET documentdb.next_collection_index_id TO 1984000;

-- Insert test data
SELECT documentdb_api.insert_one('db', 'rewrite_sql_test', '{ "_id": 1, "country": "Mexico", "city": "CDMX" }');
SELECT documentdb_api.insert_one('db', 'rewrite_sql_test', '{ "_id": 2, "country": "USA", "city": "NYC" }');

-- Test aggregate with $match
SELECT documentdb_api_internal.bson_get_rewritten_sql('db',
  '{ "aggregate": "rewrite_sql_test", "pipeline": [{ "$match": { "country": "Mexico" } }], "cursor": {} }'::documentdb_core.bson);

-- Test aggregate with $project and $limit
SELECT documentdb_api_internal.bson_get_rewritten_sql('db',
  '{ "aggregate": "rewrite_sql_test", "pipeline": [{ "$project": { "country": 1 } }, { "$limit": 10 }], "cursor": {} }'::documentdb_core.bson);

-- Test find with filter
SELECT documentdb_api_internal.bson_get_rewritten_sql('db',
  '{ "find": "rewrite_sql_test", "filter": { "country": "Mexico" } }'::documentdb_core.bson);

-- Test find with filter, projection, sort, skip, limit
SELECT documentdb_api_internal.bson_get_rewritten_sql('db',
  '{ "find": "rewrite_sql_test", "filter": { "_id": { "$gt": 0 } }, "projection": { "country": 1 }, "sort": { "_id": 1 }, "skip": 0, "limit": 5 }'::documentdb_core.bson);

-- Test count
SELECT documentdb_api_internal.bson_get_rewritten_sql('db',
  '{ "count": "rewrite_sql_test" }'::documentdb_core.bson);

-- Test count with query filter
SELECT documentdb_api_internal.bson_get_rewritten_sql('db',
  '{ "count": "rewrite_sql_test", "query": { "country": "Mexico" } }'::documentdb_core.bson);

-- Test distinct
SELECT documentdb_api_internal.bson_get_rewritten_sql('db',
  '{ "distinct": "rewrite_sql_test", "key": "country" }'::documentdb_core.bson);

-- Test error on unsupported command type
SELECT documentdb_api_internal.bson_get_rewritten_sql('db',
  '{ "unsupported": "rewrite_sql_test" }'::documentdb_core.bson);

-- Test error on empty spec
SELECT documentdb_api_internal.bson_get_rewritten_sql('db',
  '{ }'::documentdb_core.bson);
