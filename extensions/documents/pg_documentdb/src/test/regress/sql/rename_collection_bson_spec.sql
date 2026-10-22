-- Tests for rename_collection BSON signature: rename_collection(bson)
-- This is the new signature used by the Gateway
SET search_path TO documentdb_api_catalog;
SET documentdb.next_collection_id TO 1400;
SET documentdb.next_collection_index_id TO 1400;

-- Setup: create collections for testing
SELECT documentdb_api.create_collection('rename_bson_db','source_coll');
SELECT documentdb_api.insert_one('rename_bson_db','source_coll','{"_id":"1", "a":1}');

-- Test 1: basic rename via BSON
SELECT documentdb_api.rename_collection('{"renameCollection": "rename_bson_db.source_coll", "to": "rename_bson_db.target_coll"}'::documentdb_core.bson);
-- verify old name gone, new name has data
SELECT count(*) FROM documentdb_api.collection('rename_bson_db','source_coll');
SELECT count(*) FROM documentdb_api.collection('rename_bson_db','target_coll');

-- Test 2: rename to existing name without dropTarget → ERROR
SELECT documentdb_api.create_collection('rename_bson_db','existing_coll');
SELECT documentdb_api.rename_collection('{"renameCollection": "rename_bson_db.target_coll", "to": "rename_bson_db.existing_coll"}'::documentdb_core.bson);

-- Test 3: rename with dropTarget=true → succeeds
SELECT documentdb_api.rename_collection('{"renameCollection": "rename_bson_db.target_coll", "to": "rename_bson_db.existing_coll", "dropTarget": true}'::documentdb_core.bson);
-- verify data moved
SELECT count(*) FROM documentdb_api.collection('rename_bson_db','target_coll');
SELECT count(*) FROM documentdb_api.collection('rename_bson_db','existing_coll');

-- Test 4: rename non-existent collection → ERROR
SELECT documentdb_api.rename_collection('{"renameCollection": "rename_bson_db.no_such_coll", "to": "rename_bson_db.something"}'::documentdb_core.bson);

-- Test 5: NULL BSON → ERROR
SELECT documentdb_api.rename_collection(NULL::documentdb_core.bson);

-- Test 6: missing "to" field → ERROR
SELECT documentdb_api.rename_collection('{"renameCollection": "rename_bson_db.existing_coll"}'::documentdb_core.bson);

-- Test 7: missing "renameCollection" field → ERROR
SELECT documentdb_api.rename_collection('{"to": "rename_bson_db.something"}'::documentdb_core.bson);

-- Test 8: extra fields (lsid, $db, writeConcern) are ignored
SELECT documentdb_api.create_collection('rename_bson_db','extra_fields_coll');
SELECT documentdb_api.rename_collection('{"renameCollection": "rename_bson_db.extra_fields_coll", "to": "rename_bson_db.extra_fields_renamed", "lsid": {"id": "abc"}, "$db": "admin", "writeConcern": {"w": 1}}'::documentdb_core.bson);
SELECT count(*) FROM documentdb_api.collection('rename_bson_db','extra_fields_renamed');

-- Test 9: invalid namespace format (no dot) → ERROR
SELECT documentdb_api.rename_collection('{"renameCollection": "noDotHere", "to": "alsoNoDot"}'::documentdb_core.bson);

-- Test 10: cross-database rename → ERROR
SELECT documentdb_api.rename_collection('{"renameCollection": "rename_bson_db.existing_coll", "to": "otherdb.existing_coll"}'::documentdb_core.bson);

-- Test 11: rename collection to itself → ERROR
SELECT documentdb_api.rename_collection('{"renameCollection": "rename_bson_db.existing_coll", "to": "rename_bson_db.existing_coll"}'::documentdb_core.bson);

-- Test 12: unknown field → ERROR
SELECT documentdb_api.rename_collection('{"renameCollection": "rename_bson_db.existing_coll", "to": "rename_bson_db.new_coll", "unknownField": true}'::documentdb_core.bson);

-- Cleanup
SELECT documentdb_api.drop_database('rename_bson_db');
