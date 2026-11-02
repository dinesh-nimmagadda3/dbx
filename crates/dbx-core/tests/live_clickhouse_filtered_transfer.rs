//! Per-table transfer filters with a ClickHouse source -> SQLite target.
//!
//! The filter runs through the OFFSET paging fallback, so the batch size is
//! kept below the filtered row count to cover more than one page.

use dbx_core::connection::AppState;
use dbx_core::db::clickhouse_driver::{execute_query, ChClient};
use dbx_core::models::connection::{ConnectionConfig, DatabaseType};
use dbx_core::transfer::{
    transfer_table, TransferColumnNameCase, TransferContent, TransferMode, TransferOwnershipPolicy, TransferRequest,
    TransferTableNameCase,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn sqlite_config(id: &str, path: &str) -> ConnectionConfig {
    ConnectionConfig {
        oracle_oci_nls_lang: None,
        oracle_oci_tns_admin: None,
        docs_notes_path: None,
        id: id.to_string(),
        name: id.to_string(),
        note: String::new(),
        db_type: DatabaseType::Sqlite,
        driver_profile: None,
        driver_label: None,
        url_params: None,
        agent_java_options: Vec::new(),
        host: path.to_string(),
        port: 0,
        username: String::new(),
        password: String::new(),
        database: None,
        default_schema: None,
        visible_databases: None,
        visible_database_patterns: None,
        visible_schemas: None,
        attached_databases: Vec::new(),
        init_script: None,
        color: None,
        transport_layers: Vec::new(),
        connect_timeout_secs: 15,
        query_timeout_secs: 30,
        idle_timeout_secs: 60,
        manual_transaction_timeout_secs: 300,
        idle_disconnect_secs: 0,
        keepalive_interval_secs: 0,
        ssl: false,
        ca_cert_path: String::new(),
        client_cert_path: String::new(),
        client_key_path: String::new(),
        sysdba: false,
        oracle_connection_type: None,
        connection_string: None,
        redis_connection_mode: None,
        redis_sentinel_master: String::new(),
        redis_sentinel_nodes: String::new(),
        redis_sentinel_username: String::new(),
        redis_sentinel_password: String::new(),
        redis_sentinel_tls: false,
        redis_cluster_nodes: String::new(),
        redis_key_separator: dbx_core::models::connection::default_redis_key_separator(),
        redis_scan_page_size: None,
        redis_database_aliases: Default::default(),
        redis_key_templates: Vec::new(),
        redis_key_filter: None,
        redis_key_grouping: None,
        etcd_endpoints: String::new(),
        gbase_server: String::new(),
        informix_server: String::new(),
        external_config: None,
        plugin_id: None,
        plugin_connection_provider: None,
        plugin_connection_type: None,
        connection_secrets: Default::default(),
        jdbc_driver_class: None,
        jdbc_driver_paths: Vec::new(),
        one_time: false,
        save_password: true,
        read_only: false,
        is_production: false,
        production_databases: vec![],
        production_badge_scope: None,
        show_system_schemas: false,
        sidebar_auto_load_all_tables: false,
        show_database_links: None,
        show_native_jobs: None,
        show_user_admin: None,
        database_info: None,
    }
}

struct LiveClickHouse {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

impl LiveClickHouse {
    fn from_env() -> Self {
        Self {
            host: std::env::var("DBX_LIVE_CLICKHOUSE_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
            port: std::env::var("DBX_LIVE_CLICKHOUSE_PORT").ok().and_then(|value| value.parse().ok()).unwrap_or(8123),
            user: std::env::var("DBX_LIVE_CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string()),
            password: std::env::var("DBX_LIVE_CLICKHOUSE_PASSWORD").expect("DBX_LIVE_CLICKHOUSE_PASSWORD"),
            database: std::env::var("DBX_LIVE_CLICKHOUSE_DATABASE").unwrap_or_else(|_| "dbx".to_string()),
        }
    }

    fn client(&self) -> ChClient {
        ChClient::new(
            &format!("http://{}:{}", self.host, self.port),
            Some(self.user.clone()),
            Some(self.password.clone()),
            Duration::from_secs(30),
        )
    }

    fn config(&self, id: &str) -> ConnectionConfig {
        let mut config = sqlite_config(id, "");
        config.db_type = DatabaseType::ClickHouse;
        config.host = self.host.clone();
        config.port = self.port;
        config.username = self.user.clone();
        config.password = self.password.clone();
        config.database = Some(self.database.clone());
        config
    }
}

/// Transfers `table` into a fresh SQLite file with `filter` and returns the
/// moved row count and the target ids in ascending order.
async fn transfer_filtered(
    clickhouse: &LiveClickHouse,
    table: &str,
    filter: &str,
    dir: &std::path::Path,
    run: &str,
) -> Result<(u64, Vec<i64>), String> {
    let source_id = format!("live-ch-source-{run}");
    let target_id = format!("live-sqlite-target-{run}");
    let target_path = dir.join(format!("{run}.sqlite")).to_string_lossy().into_owned();
    dbx_core::db::sqlite::connect_path_create_if_missing(&target_path).await?;

    let storage = dbx_core::persistence::test_storage::open(&dir.join(format!("{run}-storage.db")))
        .await
        .map_err(|error| format!("open storage: {error}"))?;
    let state = Arc::new(AppState::new(storage));
    state.configs.write().await.insert(source_id.clone(), clickhouse.config(&source_id));
    state.configs.write().await.insert(target_id.clone(), sqlite_config(&target_id, &target_path));
    let source_pool_key = state.get_or_create_pool(&source_id, Some(&clickhouse.database)).await?;
    let target_pool_key = state.get_or_create_pool(&target_id, Some("main")).await?;

    let request = TransferRequest {
        table_filters: HashMap::from([(table.to_string(), filter.to_string())]),
        transfer_id: format!("live-ch-filter-{run}"),
        source_connection_id: source_id,
        source_database: clickhouse.database.clone(),
        source_schema: clickhouse.database.clone(),
        source_catalog: None,
        target_connection_id: target_id,
        target_database: "main".to_string(),
        target_schema: "main".to_string(),
        target_catalog: None,
        tables: vec![table.to_string()],
        create_table: true,
        drop_target_before_create: false,
        drop_target_confirmed: false,
        oracle_packages: None,
        content: TransferContent::default(),
        objects: Some(Vec::new()),
        mode: TransferMode::Append,
        target_table_name_case: TransferTableNameCase::Preserve,
        target_column_name_case: TransferColumnNameCase::Preserve,
        quote_target_column_names: true,
        ownership_policy: TransferOwnershipPolicy::Preserve,
        batch_size: 4,
    };
    dbx_core::transfer::validate_transfer_request(&request)?;

    let moved = transfer_table(
        &state,
        &request,
        table,
        0,
        &DatabaseType::ClickHouse,
        &DatabaseType::Sqlite,
        &source_pool_key,
        &target_pool_key,
        &HashMap::new(),
        &mut Vec::new(),
        None,
        |_| {},
    )
    .await?;

    let target = dbx_core::db::sqlite::connect_path_create_if_missing(&target_path).await?;
    let rows = dbx_core::db::sqlite::execute_query(&target, &format!("SELECT id FROM \"{table}\" ORDER BY id")).await?;
    let ids = rows
        .rows
        .iter()
        .map(|row| match &row[0] {
            serde_json::Value::Number(number) => number.as_i64().ok_or_else(|| format!("non-integer id {number}")),
            serde_json::Value::String(text) => text.parse::<i64>().map_err(|error| format!("id {text:?}: {error}")),
            other => Err(format!("unexpected id value {other}")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((moved, ids))
}

#[test]
#[ignore = "requires DBX_LIVE_CLICKHOUSE_PASSWORD (and optional HOST/PORT/USER/DATABASE) pointing at ClickHouse over HTTP"]
fn live_clickhouse_table_filters_transfer_only_matching_rows() {
    // The unoptimized transfer futures exceed the default test-thread stack.
    std::thread::Builder::new()
        .name("clickhouse-filtered-transfer".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(run_clickhouse_filtered_transfer());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn run_clickhouse_filtered_transfer() {
    let clickhouse = LiveClickHouse::from_env();
    let client = clickhouse.client();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("dbx_filter_events_{}", &suffix[..12]);
    let qualified = format!("`{}`.`{table}`", clickhouse.database);

    execute_query(
        &client,
        &clickhouse.database,
        &format!(
            "CREATE TABLE {qualified} (id UInt32, label String, created_at DateTime) ENGINE = MergeTree ORDER BY id"
        ),
    )
    .await
    .expect("create source table");
    // Ids 1-12 are a month old; ids 13-25 fall inside the last seven days.
    execute_query(
        &client,
        &clickhouse.database,
        &format!(
            "INSERT INTO {qualified} SELECT number + 1, concat('row-', toString(number + 1)), \
             if(number < 12, now() - INTERVAL 30 DAY, now() - toIntervalHour(number - 12)) FROM numbers(25)"
        ),
    )
    .await
    .expect("seed source table");

    let dir = std::env::temp_dir().join(format!("dbx-live-ch-filter-{suffix}"));
    std::fs::create_dir_all(&dir).expect("create temp directory");

    let predicate =
        transfer_filtered(&clickhouse, &table, "created_at >= now() - INTERVAL 7 DAY", &dir, "predicate").await;
    let query = transfer_filtered(
        &clickhouse,
        &table,
        &format!("SELECT * FROM {qualified} WHERE created_at >= now() - INTERVAL 7 DAY AND id % 2 = 1"),
        &dir,
        "query",
    )
    .await;

    let cleanup = execute_query(&client, &clickhouse.database, &format!("DROP TABLE IF EXISTS {qualified}")).await;
    let _ = std::fs::remove_dir_all(&dir);
    cleanup.expect("drop source table");

    let (moved, ids) = predicate.expect("WHERE predicate filter transfer");
    assert_eq!(ids, (13..=25).collect::<Vec<i64>>(), "predicate filter must copy only the last seven days");
    assert_eq!(moved, 13);

    let (moved, ids) = query.expect("full SELECT filter transfer");
    assert_eq!(ids, vec![13, 15, 17, 19, 21, 23, 25], "SELECT filter must copy only its result rows");
    assert_eq!(moved, 7);
}
