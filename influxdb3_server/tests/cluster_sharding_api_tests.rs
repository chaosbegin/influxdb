// influxdb3_server/tests/cluster_sharding_api_tests.rs
use influxdb3_server::http::cluster::UpdateTableShardingPayload;
use reqwest::StatusCode;
use serde_json::json;
use std::sync::Arc;
use influxdb3_server_testing::TestServer; // Assuming a test server utility like this exists
use influxdb3_catalog::catalog::Catalog; // For direct catalog interaction in tests

// Helper function to get a TestServer instance
async fn setup_server() -> TestServer {
    // This would ideally use a shared test setup or a builder pattern
    // For now, assume TestServer::new() sets up a server with an in-memory catalog
    TestServer::new().await.expect("Failed to start test server")
}

// Helper to get catalog for direct verification (implementation specific)
fn get_catalog_from_server(server: &TestServer) -> Arc<Catalog> {
    // This is highly dependent on TestServer's implementation.
    // It might expose its HttpApi, which in turn has a catalog() method.
    // server.http_api().catalog()
    // For this conceptual implementation, we'll assume such a method exists.
    // If direct catalog access is hard, tests might need to rely solely on API responses,
    // or specific test-only API endpoints to fetch internal state.
    server.http_api().catalog()
}


#[tokio::test]
async fn test_update_table_sharding_config_api() {
    let server = setup_server().await;
    let client = reqwest::Client::new();
    let base_url = server.url();

    let db_name = "test_db_sharding_api";
    let table_name = "test_table_sharding_api";

    // Setup: Create database and table (implicitly or explicitly)
    // Assuming writes can create DB/Table, or a test utility for direct catalog manipulation
    let catalog = get_catalog_from_server(&server);
    catalog.create_database(db_name).await.expect("Failed to create DB");
    catalog.create_table(
        db_name,
        table_name,
        &["tagA", "tagB"], // Pre-define potential shard keys as columns
        &[
            ("field1".to_string(), influxdb3_catalog::log::FieldDataType::Integer),
            ("time".to_string(), influxdb3_catalog::log::FieldDataType::Timestamp),
        ]
    ).await.expect("Failed to create table");

    let sharding_url = format!("{}/api/v3/cluster/tables/sharding", base_url);

    // 1. Set Initial Config
    let initial_payload = UpdateTableShardingPayload {
        db_name: db_name.to_string(),
        table_name: table_name.to_string(),
        shard_keys: Some(vec!["tagA".to_string()]),
        num_hash_partitions: Some(4),
    };
    let res_initial = client.put(&sharding_url)
        .json(&initial_payload)
        .send()
        .await
        .expect("Request failed");
    assert_eq!(res_initial.status(), StatusCode::OK, "Initial config failed. Body: {:?}", res_initial.text().await);

    let table_def1 = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap();
    assert_eq!(table_def1.shard_keys, vec!["tagA".to_string()]);
    assert_eq!(table_def1.num_hash_partitions, 4);

    // 2. Update Only Keys
    let update_keys_payload = UpdateTableShardingPayload {
        db_name: db_name.to_string(),
        table_name: table_name.to_string(),
        shard_keys: Some(vec!["tagB".to_string()]),
        num_hash_partitions: None, // Omit to not change
    };
    let res_update_keys = client.put(&sharding_url)
        .json(&update_keys_payload)
        .send()
        .await
        .expect("Request failed");
    assert_eq!(res_update_keys.status(), StatusCode::OK, "Update keys failed. Body: {:?}", res_update_keys.text().await);

    let table_def2 = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap();
    assert_eq!(table_def2.shard_keys, vec!["tagB".to_string()]);
    assert_eq!(table_def2.num_hash_partitions, 4); // Should remain 4

    // 3. Update Only Partitions
    let update_partitions_payload = UpdateTableShardingPayload {
        db_name: db_name.to_string(),
        table_name: table_name.to_string(),
        shard_keys: None, // Omit to not change
        num_hash_partitions: Some(8),
    };
    let res_update_partitions = client.put(&sharding_url)
        .json(&update_partitions_payload)
        .send()
        .await
        .expect("Request failed");
    assert_eq!(res_update_partitions.status(), StatusCode::OK, "Update partitions failed. Body: {:?}", res_update_partitions.text().await);

    let table_def3 = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap();
    assert_eq!(table_def3.shard_keys, vec!["tagB".to_string()]); // Should remain ["tagB"]
    assert_eq!(table_def3.num_hash_partitions, 8);

    // 4. Clear Keys
    let clear_keys_payload = UpdateTableShardingPayload {
        db_name: db_name.to_string(),
        table_name: table_name.to_string(),
        shard_keys: Some(vec![]), // Empty vec to clear
        num_hash_partitions: None,
    };
    let res_clear_keys = client.put(&sharding_url)
        .json(&clear_keys_payload)
        .send()
        .await
        .expect("Request failed");
    assert_eq!(res_clear_keys.status(), StatusCode::OK, "Clear keys failed. Body: {:?}", res_clear_keys.text().await);

    let table_def4 = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap();
    assert!(table_def4.shard_keys.is_empty());
    assert_eq!(table_def4.num_hash_partitions, 8); // Should remain 8

    // 5. Set Partitions to 1 (effectively disabling explicit hash sharding)
    let partitions_to_one_payload = UpdateTableShardingPayload {
        db_name: db_name.to_string(),
        table_name: table_name.to_string(),
        shard_keys: None,
        num_hash_partitions: Some(1),
    };
    let res_partitions_to_one = client.put(&sharding_url)
        .json(&partitions_to_one_payload)
        .send()
        .await
        .expect("Request failed");
    assert_eq!(res_partitions_to_one.status(), StatusCode::OK, "Set partitions to 1 failed. Body: {:?}", res_partitions_to_one.text().await);

    let table_def5 = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap();
    assert_eq!(table_def5.num_hash_partitions, 1);

    // 6. Invalid: Partitions to 0
    let partitions_to_zero_payload = UpdateTableShardingPayload {
        db_name: db_name.to_string(),
        table_name: table_name.to_string(),
        shard_keys: None,
        num_hash_partitions: Some(0),
    };
    let res_partitions_to_zero = client.put(&sharding_url)
        .json(&partitions_to_zero_payload)
        .send()
        .await
        .expect("Request failed");
    assert_eq!(res_partitions_to_zero.status(), StatusCode::BAD_REQUEST, "Partitions to 0 should fail. Body: {:?}", res_partitions_to_zero.text().await);
    // Optionally, check error message if API guarantees it
    // let error_body: serde_json::Value = res_partitions_to_zero.json().await.unwrap();
    // assert_eq!(error_body["error"], "num_hash_partitions cannot be 0");


    // 7. Non-existent Table
    let non_existent_table_payload = UpdateTableShardingPayload {
        db_name: db_name.to_string(),
        table_name: "does_not_exist_table".to_string(),
        shard_keys: None,
        num_hash_partitions: Some(2),
    };
    let res_non_existent_table = client.put(&sharding_url)
        .json(&non_existent_table_payload)
        .send()
        .await
        .expect("Request failed");
    assert_eq!(res_non_existent_table.status(), StatusCode::NOT_FOUND, "Non-existent table. Body: {:?}", res_non_existent_table.text().await);

    // 8. Non-existent DB
    let non_existent_db_payload = UpdateTableShardingPayload {
        db_name: "does_not_exist_db".to_string(),
        table_name: table_name.to_string(),
        shard_keys: None,
        num_hash_partitions: Some(2),
    };
    let res_non_existent_db = client.put(&sharding_url)
        .json(&non_existent_db_payload)
        .send()
        .await
        .expect("Request failed");
    assert_eq!(res_non_existent_db.status(), StatusCode::NOT_FOUND, "Non-existent DB. Body: {:?}", res_non_existent_db.text().await);
}
