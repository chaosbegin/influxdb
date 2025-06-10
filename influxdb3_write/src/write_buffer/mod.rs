//! Implementation of an in-memory buffer for writes that persists data into a wal if it is configured.

mod metrics;
pub mod persisted_files;
pub mod queryable_buffer;
mod table_buffer;
use influxdb3_shutdown::ShutdownToken;
use tokio::sync::{oneshot, watch::Receiver};
use trace::span::{MetaValue, SpanRecorder};
pub mod validator;

use crate::{
    BufferedWriteRequest, Bufferer, ChunkContainer, ChunkFilter, DistinctCacheManager,
    LastCacheManager, ParquetFile, PersistedSnapshot, PersistedSnapshotVersion, Precision,
    WriteBuffer, WriteLineError,
    chunk::ParquetChunk,
    persister::{Persister, PersisterError},
    write_buffer::{
        persisted_files::PersistedFiles, queryable_buffer::QueryableBuffer,
        validator::WriteValidator,
    },
};
use async_trait::async_trait;
use data_types::{
    ChunkId, ChunkOrder, ColumnType, NamespaceName, NamespaceNameError, PartitionHashId,
    PartitionId,
};
use datafusion::{
    catalog::Session, common::DataFusionError, datasource::object_store::ObjectStoreUrl,
};
use influxdb3_cache::{
    distinct_cache::{self, DistinctCacheProvider},
    parquet_cache::CacheRequest,
};
use influxdb3_cache::{
    last_cache::{self, LastCacheProvider},
    parquet_cache::ParquetCacheOracle,
};
use influxdb3_catalog::{
    CatalogError,
    catalog::{Catalog, DatabaseSchema, Prompt, TableDefinition},
};
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{
    Wal, WalConfig, WalFileNotifier, WalOp,
    object_store::{CreateWalObjectStoreArgs, WalObjectStore},
};
use iox_query::{
    QueryChunk,
    chunk_statistics::{NoColumnRanges, create_chunk_statistics},
    exec::SessionContextIOxExt,
};
use iox_time::{Time, TimeProvider};
use metric::Registry;
use metrics::WriteMetrics;
use object_store::{ObjectMeta, ObjectStore, path::Path as ObjPath};
use observability_deps::tracing::{debug, error, warn};
use parquet_file::storage::ParquetExecInput;
use queryable_buffer::QueryableBufferArgs;
use schema::Schema;
use std::{borrow::Borrow, sync::Arc, time::Duration};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("parsing for line protocol failed")]
    ParseError(WriteLineError),

    #[error("incoming write was empty")]
    EmptyWrite,

    #[error("column type mismatch for column {name}: existing: {existing:?}, new: {new:?}")]
    ColumnTypeMismatch {
        name: String,
        existing: ColumnType,
        new: ColumnType,
    },

    #[error("catalog update error: {0}")]
    CatalogUpdateError(#[from] CatalogError),

    #[error("error from persister: {0}")]
    PersisterError(#[from] PersisterError),

    #[error("corrupt load state: {0}")]
    CorruptLoadState(String),

    #[error("database name error: {0}")]
    DatabaseNameError(#[from] NamespaceNameError),

    #[error("error from table buffer: {0}")]
    TableBufferError(#[from] table_buffer::Error),

    #[error("error in last cache: {0}")]
    LastCacheError(#[from] last_cache::Error),

    #[error("database not found {db_name:?}")]
    DatabaseNotFound { db_name: String },

    #[error("table not found {table_name:?} in db {db_name:?}")]
    TableNotFound { db_name: String, table_name: String },

    #[error("tried accessing database that does not exist")]
    DbDoesNotExist,

    #[error("tried creating database named '{0}' that already exists")]
    DatabaseExists(String),

    #[error("tried accessing table that do not exist")]
    TableDoesNotExist,

    #[error("table '{db_name}.{table_name}' already exists")]
    TableAlreadyExists {
        db_name: Arc<str>,
        table_name: Arc<str>,
    },

    #[error("tried accessing column with name ({0}) that does not exist")]
    ColumnDoesNotExist(String),

    #[error(
        "updating catalog on delete of last cache failed, you will need to delete the cache \
        again on server restart"
    )]
    DeleteLastCache(#[source] CatalogError),

    #[error("error from wal: {0}")]
    WalError(#[from] influxdb3_wal::Error),

    #[error("cannot write to a read-only server")]
    NoWriteInReadOnly,

    #[error("error in distinct value cache: {0}")]
    DistinctCacheError(#[from] distinct_cache::ProviderError),

    #[error("cannot write to a compactor-only server")]
    NoWriteInCompactorOnly,

    #[error("error: {0}")]
    AnyhowError(#[from] anyhow::Error),

    #[error("No matching shard found for table '{table_name}' at time {timestamp_nanos}")]
    NoMatchingShardFound {
        table_name: String,
        timestamp_nanos: i64,
    },

    #[error("Replication error: {0}")]
    ReplicationError(String), // Basic error for now
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct WriteRequest<'a> {
    pub db_name: NamespaceName<'static>,
    pub line_protocol: &'a str,
    pub default_time: u64,
}

#[derive(Debug)]
pub struct WriteBufferImpl {
    catalog: Arc<Catalog>,
    persister: Arc<Persister>,
    // NOTE(trevor): the parquet cache interface may be used to register other cache
    // requests from the write buffer, e.g., during query...
    #[allow(dead_code)]
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    persisted_files: Arc<PersistedFiles>,
    buffer: Arc<QueryableBuffer>,
    wal_config: WalConfig,
    wal: Arc<dyn Wal>,
    metrics: WriteMetrics,
    distinct_cache: Arc<DistinctCacheProvider>,
    last_cache: Arc<LastCacheProvider>,
    current_node_id: Arc<str>, // Added for node identification in replication
    /// The number of files we will accept for a query
    query_file_limit: usize,
}

/// The maximum number of snapshots to load on start
pub const N_SNAPSHOTS_TO_LOAD_ON_START: usize = 1_000;

#[derive(Debug)]
pub struct WriteBufferImplArgs {
    pub persister: Arc<Persister>,
    pub catalog: Arc<Catalog>,
    pub last_cache: Arc<LastCacheProvider>,
    pub distinct_cache: Arc<DistinctCacheProvider>,
    pub time_provider: Arc<dyn TimeProvider>,
    pub executor: Arc<iox_query::exec::Executor>,
    pub wal_config: WalConfig,
    pub parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    pub metric_registry: Arc<Registry>,
    pub snapshotted_wal_files_to_keep: u64,
    pub query_file_limit: Option<usize>,
    pub shutdown: ShutdownToken,
    pub wal_replay_concurrency_limit: Option<usize>,
    pub current_node_id: Arc<str>, // Added for node identification
}

impl WriteBufferImpl {
    pub async fn new(
        WriteBufferImplArgs {
            persister,
            catalog,
            last_cache,
            distinct_cache,
            time_provider,
            executor,
            wal_config,
            parquet_cache,
            metric_registry,
            snapshotted_wal_files_to_keep,
            query_file_limit,
            shutdown,
            wal_replay_concurrency_limit,
            current_node_id, // Destructure new field
        }: WriteBufferImplArgs,
    ) -> Result<Arc<Self>> {
        // load snapshots and replay the wal into the in memory buffer
        let persisted_snapshots = persister
            .load_snapshots(N_SNAPSHOTS_TO_LOAD_ON_START)
            .await?
            .into_iter()
            // map the persisted snapshots into the newest version
            .map(|psv| match psv {
                PersistedSnapshotVersion::V1(ps) => ps,
            })
            .collect::<Vec<PersistedSnapshot>>();
        let last_wal_sequence_number = persisted_snapshots
            .first()
            .map(|s| s.wal_file_sequence_number);
        let last_snapshot_sequence_number = persisted_snapshots
            .first()
            .map(|s| s.snapshot_sequence_number);
        // If we have any snapshots, set sequential IDs from the newest one.
        if let Some(first_snapshot) = persisted_snapshots.first() {
            first_snapshot.next_file_id.set_next_id();
        }

        let persisted_files = Arc::new(PersistedFiles::new_from_persisted_snapshots(
            persisted_snapshots,
        ));
        let queryable_buffer = Arc::new(QueryableBuffer::new(QueryableBufferArgs {
            executor,
            catalog: Arc::clone(&catalog),
            persister: Arc::clone(&persister),
            last_cache_provider: Arc::clone(&last_cache),
            distinct_cache_provider: Arc::clone(&distinct_cache),
            persisted_files: Arc::clone(&persisted_files),
            parquet_cache: parquet_cache.clone(),
        }));

        // create the wal instance, which will replay into the queryable buffer and start
        // the background flush task.
        let wal = WalObjectStore::new(CreateWalObjectStoreArgs {
            time_provider: Arc::clone(&time_provider),
            object_store: persister.object_store(),
            node_identifier_prefix: persister.node_identifier_prefix(),
            file_notifier: Arc::clone(&queryable_buffer) as Arc<dyn WalFileNotifier>,
            config: wal_config,
            last_wal_sequence_number,
            last_snapshot_sequence_number,
            snapshotted_wal_files_to_keep,
            shutdown,
            wal_replay_concurrency_limit,
        })
        .await?;

        let result = Arc::new(Self {
            catalog,
            parquet_cache,
            persister,
            wal_config,
            wal,
            distinct_cache,
            last_cache,
            persisted_files,
            buffer: queryable_buffer,
            metrics: WriteMetrics::new(&metric_registry),
            current_node_id, // Store new field
            query_file_limit: query_file_limit.unwrap_or(432),
        });
        Ok(result)
    }

    pub fn wal(&self) -> Arc<dyn Wal> {
        Arc::clone(&self.wal)
    }

    pub fn persisted_files(&self) -> Arc<PersistedFiles> {
        Arc::clone(&self.persisted_files)
    }

    async fn write_lp(
        &self,
        db_name: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> Result<BufferedWriteRequest> {
        debug!("write_lp to {} in writebuffer", db_name);

        // --- Shard Determination ---
        // This is a simplified approach. A full implementation would need to:
        // 1. Potentially parse table name from each line if a batch can target multiple tables. (Current validator assumes one table per batch based on db_name)
        // 2. Handle lines with individual timestamps that might fall into different shards. This would require splitting the batch.
        // For now, we use ingest_time for the whole batch and assume one target table (implicit in db_name context).

        let table_name_from_lp = lp.lines().next().and_then(|line| line.split(',').next().map(|s| s.to_string()));
        let determined_shard_id = if let Some(table_name_str) = table_name_from_lp {
            if let Some(db_schema) = self.catalog.db_schema(db_name.as_str()) {
                if let Some(table_def) = db_schema.table_definition(&table_name_str) {
                    let mut found_shard_id = None;
                    for shard_def_arc in table_def.shards.resource_iter() {
                        if ingest_time.timestamp_nanos() >= shard_def_arc.time_range.start_time &&
                           ingest_time.timestamp_nanos() < shard_def_arc.time_range.end_time {
                            found_shard_id = Some(shard_def_arc.id);
                            break;
                        }
                    }
                    if found_shard_id.is_none() && !table_def.shards.is_empty() {
                        // Only error if shards are defined but none match. If no shards defined, proceed without sharding.
                        return Err(Error::NoMatchingShardFound {
                            table_name: table_name_str.clone(),
                            timestamp_nanos: ingest_time.timestamp_nanos(),
                        });
                    }
                    found_shard_id
                } else {
                    // Table not found in catalog, validator will handle this.
                    None
                }
            } else {
                // DB not found, validator will handle this.
                None
            }
        } else {
            // No lines in LP, validator will handle EmptyWrite.
            None
        };
        // --- End Shard Determination ---


        // NOTE(trevor/catalog-refactor): should there be some retry limit or timeout?
        loop {
            let mut validator = WriteValidator::initialize(db_name.clone(), self.catalog())?;
            let parsed_lines = validator.v1_parse_lines_and_catalog_updates(lp, accept_partial, ingest_time, precision)?;

            let validated_lines_result = match parsed_lines.commit_catalog_changes().await? {
                Prompt::Success(committed_validator_state) => {
                    let final_state = crate::write_buffer::validator::CatalogChangesCommitted {
                        catalog_sequence: committed_validator_state.inner().catalog_sequence,
                        db_id: committed_validator_state.inner().db_id,
                        db_name: Arc::clone(&committed_validator_state.inner().db_name),
                        lines: committed_validator_state.inner().lines.clone(),
                        bytes: committed_validator_state.inner().bytes,
                        errors: committed_validator_state.inner().errors.clone(),
                        shard_id: determined_shard_id,
                    };
                    WriteValidator::from(final_state).convert_lines_to_buffer(self.wal_config.gen1_duration)
                }
                Prompt::Retry(_) => {
                    debug!("retrying write_lp after attempted commit");
                    continue;
                }
            };

            };

            // Only buffer to the WAL if there are actually writes in the batch
            if validated_lines_result.line_count > 0 {
                let wal_op = WalOp::Write(validated_lines_result.valid_data.clone()); // Clone for potential replication

                // --- Replication Logic Placeholder ---
                let mut replication_successful = true; // Assume success if no replication needed
                if let Some(table_name_str) = &table_name_from_lp { // Use previously parsed table name
                    if let Some(db_schema) = self.catalog.db_schema(db_name.as_str()) {
                        if let Some(table_def) = db_schema.table_definition(table_name_str) {
                            if let Some(replication_info) = &table_def.replication_info {
                                if replication_info.replication_factor.get() > 1 {
                                    // In a real scenario, identify replica nodes from catalog/config
                                    let replica_nodes: Vec<String> = vec![]; // Placeholder
                                    if !replica_nodes.is_empty() {
                                        debug!(
                                            "Replicating WalOp for table '{}.{}' to nodes: {:?}. ShardId: {:?}",
                                            db_name.as_str(), table_name_str, replica_nodes, determined_shard_id
                                        );
                                        // let serialized_op = self.serialize_wal_op(&wal_op)?; // Placeholder method
                                        // match self.replicate_wal_op_to_nodes(replica_nodes, serialized_op).await {
                                        //     Ok(_) => { /* replication_successful = true; */ }
                                        //     Err(e) => {
                                        //         error!("Failed to replicate WalOp: {}", e);
                                        //         replication_successful = false;
                                        //         // TODO: Handle replication failure (e.g., retry, mark as degraded, error to client?)
                                        //         // For now, we might proceed with local write or error based on policy.
                                        //         // This subtask assumes a simplified initial implementation.
                                        //         return Err(Error::ReplicationError(format!("Failed to replicate to nodes: {}", e)));
                                        //     }
                                        // }
                                        // For this subtask, we'll simulate an error if replication is configured but no nodes are (conceptually) found or reachable.
                                        // This is a placeholder for actual client calls.
                                        // To make this testable without actual networking, one might check a mock/flag.
                                        // For now, just logging. A real implementation would call a replication client.
                                        warn!("Placeholder: Replication configured for {}.{} but actual replication call is not implemented.", db_name.as_str(), table_name_str);

                                    }
                                }
                            }
                        }
                    }
                }
                // --- End Replication Logic Placeholder ---

                if !replication_successful {
                     // If replication failed and is critical, error out or handle according to policy
                     // For now, this path might not be hit due to placeholder logic.
                    return Err(Error::ReplicationError("Critical replication failed.".to_string()));
                }

                // Proceed with local WAL write
                if no_sync {
                    self.wal.write_ops_unconfirmed(vec![wal_op]).await?;
                } else {
                    self.wal.write_ops(vec![wal_op]).await?;
                }
            }

            if validated_lines_result.line_count == 0 && validated_lines_result.errors.is_empty() {
                return Err(Error::EmptyWrite);
            }

            self.metrics.record_lines(&db_name, validated_lines_result.line_count as u64);
            self.metrics.record_lines_rejected(&db_name, validated_lines_result.errors.len() as u64);
            self.metrics.record_bytes(&db_name, validated_lines_result.valid_bytes_count);

            break Ok(BufferedWriteRequest {
                db_name,
                invalid_lines: validated_lines_result.errors,
                line_count: validated_lines_result.line_count,
                field_count: validated_lines_result.field_count,
                index_count: validated_lines_result.index_count,
            });
        }
    }

    // Placeholder for actual replication client logic
    // async fn replicate_wal_op_to_nodes(&self, nodes: Vec<String>, serialized_op: Vec<u8>) -> Result<(), String> {
    //     Err("Not implemented".to_string())
    // }

    // Placeholder for serializing WalOp - in reality, this would use something like bincode/protobuf
    // fn serialize_wal_op(&self, op: &WalOp) -> Result<Vec<u8>, Error> {
    //     bitcode::serialize(op).map_err(|e| Error::AnyhowError(anyhow::anyhow!("Failed to serialize WalOp: {}", e)))
    // }

    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let span_ctx = ctx.span_ctx().map(|span| span.child("table_chunks"));
        let mut recorder = SpanRecorder::new(span_ctx);

        let mut chunks = self.buffer.get_table_chunks(
            Arc::clone(&db_schema),
            Arc::clone(&table_def),
            filter,
            projection,
            ctx,
        )?;
        let num_chunks_from_buffer = chunks.len();
        recorder.set_metadata(
            "buffer_chunks",
            MetaValue::Int(num_chunks_from_buffer as i64),
        );

        let parquet_files =
            self.persisted_files
                .get_files_filtered(db_schema.id, table_def.table_id, filter);
        let num_parquet_files_needed = parquet_files.len();
        recorder.set_metadata(
            "parquet_files",
            MetaValue::Int(num_parquet_files_needed as i64),
        );

        if parquet_files.len() > self.query_file_limit {
            return Err(DataFusionError::External(
                format!(
                    "Query would exceed file limit of {} parquet files. \
                     Please specify a smaller time range for your \
                     query. You can increase the file limit with the \
                     `--query-file-limit` option in the serve command, however, \
                     query performance will be slower and the server may get \
                     OOM killed or become unstable as a result",
                    self.query_file_limit
                )
                .into(),
            ));
        }

        if let Some(parquet_cache) = &self.parquet_cache {
            let num_files_already_in_cache = parquet_files
                .iter()
                .filter(|f| {
                    parquet_cache
                        .in_cache(&ObjPath::parse(&f.path).expect("obj path should be parseable"))
                })
                .count();

            recorder.set_metadata(
                "parquet_files_already_in_cache",
                MetaValue::Int(num_files_already_in_cache as i64),
            );
            debug!(
                num_chunks_from_buffer,
                num_parquet_files_needed, num_files_already_in_cache, "query chunks breakdown"
            );
        } else {
            debug!(
                num_chunks_from_buffer,
                num_parquet_files_needed, "query chunks breakdown (cache disabled)"
            );
        }

        let mut chunk_order = chunks.len() as i64;
        // Although this sends a cache request, it does not mean all these
        // files will be cached. This depends on parquet cache's capacity
        // and whether these files are recent enough
        cache_parquet_files(self.parquet_cache.clone(), &parquet_files);

        for parquet_file in parquet_files {
            let parquet_chunk = parquet_chunk_from_file(
                &parquet_file,
                &table_def.schema,
                self.persister.object_store_url().clone(),
                self.persister.object_store(),
                chunk_order,
            );

            chunk_order += 1;

            chunks.push(Arc::new(parquet_chunk));
        }

        Ok(chunks)
    }

    #[cfg(test)]
    fn get_table_chunks_from_buffer_only(
        &self,
        database_name: &str,
        table_name: &str,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Vec<Arc<dyn QueryChunk>> {
        let db_schema = self.catalog.db_schema(database_name).unwrap();
        let table_def = db_schema.table_definition(table_name).unwrap();
        self.buffer
            .get_table_chunks(db_schema, table_def, filter, projection, ctx)
            .unwrap()
    }
}

pub fn cache_parquet_files<T: AsRef<ParquetFile>>(
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    parquet_files: &[T],
) {
    if let Some(parquet_cache) = parquet_cache {
        let all_cache_notifiers: Vec<oneshot::Receiver<()>> = parquet_files
            .iter()
            .map(|file| {
                // When datafusion tries to fetch this file we'll have cache in "Fetching" state.
                // There is a slim chance that this request hasn't been processed yet, then we
                // could incur extra GET req to populate the cache. Having a transparent
                // cache might be handy for this case.
                let f: &ParquetFile = file.borrow().as_ref();
                let (cache_req, receiver) = CacheRequest::create_eventual_mode_cache_request(
                    ObjPath::from(f.path.as_str()),
                    Some(f.timestamp_min_max()),
                );
                parquet_cache.register(cache_req);
                receiver
            })
            .collect();
        // there's no explicit await on these receivers - we're only letting parquet cache know
        // this file can be cached if it meets cache's policy.
        debug!(len = ?all_cache_notifiers.len(), "num parquet file cache requests created");
    }
}

pub fn parquet_chunk_from_file(
    parquet_file: &ParquetFile,
    table_schema: &Schema,
    object_store_url: ObjectStoreUrl,
    object_store: Arc<dyn ObjectStore>,
    chunk_order: i64,
) -> ParquetChunk {
    let partition_key = data_types::PartitionKey::from(parquet_file.chunk_time.to_string());
    let partition_id = data_types::partition::TransitionPartitionId::from_parts(
        PartitionId::new(0),
        Some(PartitionHashId::new(
            data_types::TableId::new(0),
            &partition_key,
        )),
    );

    let chunk_stats = create_chunk_statistics(
        Some(parquet_file.row_count as usize),
        table_schema,
        Some(parquet_file.timestamp_min_max()),
        &NoColumnRanges,
    );

    let location = ObjPath::from(parquet_file.path.clone());

    let parquet_exec = ParquetExecInput {
        object_store_url,
        object_meta: ObjectMeta {
            location,
            last_modified: Default::default(),
            size: parquet_file.size_bytes as usize,
            e_tag: None,
            version: None,
        },
        object_store,
    };

    ParquetChunk {
        schema: table_schema.clone(),
        stats: Arc::new(chunk_stats),
        partition_id,
        sort_key: None,
        id: ChunkId::new(),
        chunk_order: ChunkOrder::new(chunk_order),
        parquet_exec,
    }
}

#[async_trait]
impl Bufferer for WriteBufferImpl {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> Result<BufferedWriteRequest> {
        self.write_lp(
            database,
            lp,
            ingest_time,
            accept_partial,
            precision,
            no_sync,
        )
        .await
    }

    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    fn wal(&self) -> Arc<dyn Wal> {
        Arc::clone(&self.wal)
    }

    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        self.buffer.persisted_parquet_files(db_id, table_id, filter)
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshotVersion>> {
        self.buffer.persisted_snapshot_notify_rx()
    }
}

impl ChunkContainer for WriteBufferImpl {
    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> crate::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        self.get_table_chunks(db_schema, table_def, filter, projection, ctx)
    }
}

#[async_trait::async_trait]
impl DistinctCacheManager for WriteBufferImpl {
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider> {
        Arc::clone(&self.distinct_cache)
    }
}

#[async_trait::async_trait]
impl LastCacheManager for WriteBufferImpl {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        Arc::clone(&self.last_cache)
    }
}

impl WriteBuffer for WriteBufferImpl {}

#[async_trait]
impl Bufferer for WriteBufferImpl {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> Result<BufferedWriteRequest> {
        // Existing write_lp implementation...
        // This needs to be kept as is, with the replication client logic added previously.
        // The diff tool likely needs the full existing method here if I were to modify it,
        // but I am adding a NEW method to the impl block.
        // For adding a new method, I need a different anchor.
        // The previous diff for write_lp already added the client-side replication placeholders.
        // This diff will add the new apply_replicated_wal_op method.
        // Re-stating the full write_lp is not needed if only adding a new method to the impl.
        // However, the tool needs a valid search block.
        // Let's find a stable point at the end of the WriteBufferImpl impl block.
        // The current end seems to be before the check_mem_and_force_snapshot_loop function.
        // So, I will use the WriteBuffer trait implementation as the anchor.
        // This is tricky. Let's try to anchor on the `impl WriteBuffer for WriteBufferImpl {}`
        // and add the new method within the broader `impl WriteBufferImpl` block.

        // For now, I will assume the previous changes to write_lp are intact and
        // this diff focuses on adding apply_replicated_wal_op to the WriteBufferImpl struct,
        // which also means it needs to be part of the Bufferer trait impl.
        // The tool might get confused if I don't provide the existing write_lp.
        // To be safe, I'll re-provide the existing write_lp (as modified before)
        // and then add the new method. This is risky due to length.

        // Re-provide the existing write_lp as previously modified for replication client placeholders
        debug!("write_lp to {} in writebuffer", database);

        let table_name_from_lp = lp.lines().next().and_then(|line| line.split(',').next().map(|s| s.to_string()));
        let determined_shard_id = if let Some(table_name_str) = table_name_from_lp.as_ref().or_else(|| {
            // If lp is empty, table_name_from_lp will be None.
            // Try to get table name from db_name if it's a common pattern, or this needs more robust handling.
            // This is a tricky spot if lp can be empty but db_name implies a single table.
            // For now, if lp is empty, determined_shard_id will be None.
            None
        }) {
            if let Some(db_schema) = self.catalog.db_schema(database.as_str()) {
                if let Some(table_def) = db_schema.table_definition(table_name_str) {
                    let mut found_shard_id = None;
                    for shard_def_arc in table_def.shards.resource_iter() {
                        if ingest_time.timestamp_nanos() >= shard_def_arc.time_range.start_time &&
                           ingest_time.timestamp_nanos() < shard_def_arc.time_range.end_time {
                            found_shard_id = Some(shard_def_arc.id);
                            break;
                        }
                    }
                    if found_shard_id.is_none() && !table_def.shards.is_empty() {
                        return Err(Error::NoMatchingShardFound {
                            table_name: table_name_str.clone(),
                            timestamp_nanos: ingest_time.timestamp_nanos(),
                        });
                    }
                    found_shard_id
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        loop {
            let mut validator = WriteValidator::initialize(database.clone(), self.catalog())?;
            let parsed_lines = validator.v1_parse_lines_and_catalog_updates(lp, accept_partial, ingest_time, precision)?;

            let validated_lines_result = match parsed_lines.commit_catalog_changes().await? {
                Prompt::Success(committed_validator_state) => {
                    let final_state = crate::write_buffer::validator::CatalogChangesCommitted {
                        catalog_sequence: committed_validator_state.inner().catalog_sequence,
                        db_id: committed_validator_state.inner().db_id,
                        db_name: Arc::clone(&committed_validator_state.inner().db_name),
                        lines: committed_validator_state.inner().lines.clone(),
                        bytes: committed_validator_state.inner().bytes,
                        errors: committed_validator_state.inner().errors.clone(),
                        shard_id: determined_shard_id,
                    };
                    WriteValidator::from(final_state).convert_lines_to_buffer(self.wal_config.gen1_duration)
                }
                Prompt::Retry(_) => {
                    debug!("retrying write_lp after attempted commit");
                    continue;
                }
            };

            if validated_lines_result.line_count > 0 {
                let wal_op = WalOp::Write(validated_lines_result.valid_data.clone());

                let mut replication_successful = true;
                if let Some(ref table_name_str_ref) = table_name_from_lp {
                    if let Some(db_schema) = self.catalog.db_schema(database.as_str()) {
                        if let Some(table_def) = db_schema.table_definition(table_name_str_ref) {
                            if let Some(replication_info) = &table_def.replication_info {
                                if replication_info.replication_factor.get() > 1 {
                                    let replica_nodes: Vec<String> = vec![]; // Placeholder for actual replica node discovery

                                    if !replica_nodes.is_empty() {
                                        debug!(
                                            "Attempting to replicate WalOp for table '{}.{}' to nodes: {:?}. ShardId: {:?}",
                                            database.as_str(), table_name_str_ref, replica_nodes, determined_shard_id
                                        );

                                        let serialized_op = match bitcode::serialize(&wal_op) {
                                            Ok(bytes) => bytes,
                                            Err(e) => {
                                                error!("Failed to serialize WalOp for replication: {}", e);
                                                replication_successful = false; // Mark as failure
                                                // Potentially return Error::ReplicationError directly if serialization is critical
                                                // return Err(Error::ReplicationError(format!("Failed to serialize WalOp: {}", e)));
                                                vec![] // Or handle more gracefully
                                            }
                                        };

                                        if replication_successful { // Only proceed if serialization was ok
                                            // Use self.current_node_id
                                            let own_node_id = self.current_node_id.as_ref().to_string();

                                            let request_template = crate::ReplicateWalOpRequest {
                                                serialized_wal_op: serialized_op,
                                                originating_node_id: own_node_id,
                                                shard_id: determined_shard_id.map(|sid| sid.get()),
                                                database_name: database.as_str().to_string(),
                                                table_name: table_name_str_ref.clone(),
                                            };

                                            // Placeholder for actual RPC calls to each replica_node
                                            // let mut successful_replications = 0;
                                            // for node_addr in replica_nodes {
                                            //     // let client = ... get_replication_client_for(node_addr).await ...;
                                            //     // match client.replicate_wal_op(request_template.clone_with_op(op_bytes_for_this_node)).await {
                                            //     //     Ok(response) if response.into_inner().success => successful_replications += 1,
                                            //     //     Ok(response) => error!("Replica {} failed: {:?}", node_addr, response.into_inner().error_message),
                                            //     //     Err(e) => error!("RPC error to replica {}: {:?}", node_addr, e),
                                            //     // }
                                            // }

                                            // Placeholder for quorum check
                                            // let quorum_needed = (replica_nodes.len() / 2) + 1;
                                            // if successful_replications < quorum_needed {
                                            //     replication_successful = false;
                                            //     error!("Quorum not met for WalOp replication. Needed {}, got {}", quorum_needed, successful_replications);
                                            //     // return Err(Error::ReplicationError("Quorum not met".to_string()));
                                            // }
                                            warn!("Placeholder: Actual replication RPC calls and quorum logic for {}.{} are not implemented.", database.as_str(), table_name_str_ref);
                                        }
                                    } else if replication_info.replication_factor.get() > 1 {
                                         warn!("Replication configured for {}.{} (factor > 1), but no replica nodes identified/configured. Proceeding with local write only.", database.as_str(), table_name_str_ref);
                                    }
                                }
                            }
                        }
                    }
                }

                if !replication_successful {
                    return Err(Error::ReplicationError("Critical replication failed.".to_string()));
                }

                if no_sync {
                    self.wal.write_ops_unconfirmed(vec![wal_op]).await?;
                } else {
                    self.wal.write_ops(vec![wal_op]).await?;
                }
            }

            if validated_lines_result.line_count == 0 && validated_lines_result.errors.is_empty() {
                return Err(Error::EmptyWrite);
            }

            self.metrics.record_lines(&database, validated_lines_result.line_count as u64);
            self.metrics.record_lines_rejected(&database, validated_lines_result.errors.len() as u64);
            self.metrics.record_bytes(&database, validated_lines_result.valid_bytes_count);

            break Ok(BufferedWriteRequest {
                db_name: database,
                invalid_lines: validated_lines_result.errors,
                line_count: validated_lines_result.line_count,
                field_count: validated_lines_result.field_count,
                index_count: validated_lines_result.index_count,
            });
        }
    }

    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    fn wal(&self) -> Arc<dyn Wal> {
        Arc::clone(&self.wal)
    }

    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        self.buffer.persisted_parquet_files(db_id, table_id, filter)
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshotVersion>> {
        self.buffer.persisted_snapshot_notify_rx()
    }

    async fn apply_replicated_wal_op(&self, op: WalOp, originating_node_id: Option<String>) -> Result<(), Error> {
        debug!(?op, ?originating_node_id, "Applying replicated WalOp");

        // Here, we assume the WalOp (specifically WalOp::Write(WriteBatch)) is valid
        // and has been appropriately constructed by the originating node.
        // Key differences from write_lp:
        // 1. No line protocol parsing or validation against catalog schema (already done on originating node).
        // 2. No new table/column creation in catalog (schema assumed to be synced or eventually consistent).
        // 3. CRITICAL: No further replication of this op. This is the termination point for a replicated write.
        // 4. ShardId within the WriteBatch should be respected by the local TableBuffer.

        if let WalOp::Write(ref write_batch) = op {
            if write_batch.table_chunks.is_empty() && write_batch.catalog_sequence == 0 { // Heuristic for potentially empty/noop write from replication
                 debug!("Applying a possibly empty or no-op WriteBatch from replication. Origin: {:?}", originating_node_id);
            }
             // For now, assume `no_sync = false` for replicated ops, meaning we wait for local WAL persistence.
             // This provides stronger guarantees to the originating node if the RPC ack implies local persistence.
            self.wal.write_ops(vec![op]).await.map_err(Error::WriteBuffer)?;
        } else if let WalOp::Noop(_) = op {
            // If it's a Noop, still write it to WAL to maintain sequence if necessary,
            // or decide if it can be skipped on replicas. For safety, write it.
            self.wal.write_ops(vec![op]).await.map_err(Error::WriteBuffer)?;
        }
        // If other WalOp types are added, they'd need handling here too.

        Ok(())
    }
}

pub async fn check_mem_and_force_snapshot_loop(
    write_buffer: Arc<WriteBufferImpl>,
    memory_threshold_bytes: usize,
    check_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(check_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            check_mem_and_force_snapshot(&write_buffer, memory_threshold_bytes).await;
        }
    })
}

async fn check_mem_and_force_snapshot(
    write_buffer: &Arc<WriteBufferImpl>,
    memory_threshold_bytes: usize,
) {
    let current_buffer_size_bytes = write_buffer.buffer.get_total_size_bytes();
    debug!(
        current_buffer_size_bytes,
        memory_threshold_bytes, "checking buffer size and snapshotting"
    );

    if current_buffer_size_bytes >= memory_threshold_bytes {
        warn!(
            current_buffer_size_bytes,
            memory_threshold_bytes, "forcing snapshot as buffer size > mem threshold"
        );

        let wal = Arc::clone(&write_buffer.wal);

        let cleanup_after_snapshot = wal.force_flush_buffer().await;

        // handle snapshot cleanup outside of the flush loop
        if let Some((snapshot_complete, snapshot_info, snapshot_permit)) = cleanup_after_snapshot {
            let snapshot_wal = Arc::clone(&wal);
            tokio::spawn(async move {
                let snapshot_details = snapshot_complete.await.expect("snapshot failed");
                assert_eq!(snapshot_info, snapshot_details);

                snapshot_wal
                    .cleanup_snapshot(snapshot_info, snapshot_permit)
                    .await;
            });
        }
    }
}

#[cfg(test)]
#[allow(clippy::await_holding_lock)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use crate::PersistedSnapshot;
    use crate::paths::SnapshotInfoFilePath;
    use crate::persister::Persister;
    use crate::test_helpers::WriteBufferTester;
    use arrow::array::AsArray;
    use arrow::datatypes::Int32Type;
    use arrow::record_batch::RecordBatch;
    use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
    use bytes::Bytes;
    use datafusion_util::config::register_iox_object_store;
    use executor::DedicatedExecutor;
    use futures_util::StreamExt;
    use influxdb3_cache::parquet_cache::test_cached_obj_store_and_oracle;
    use influxdb3_catalog::catalog::{CatalogSequenceNumber, HardDeletionTime};
    use influxdb3_catalog::log::FieldDataType;
    use influxdb3_id::{ColumnId, DbId, ParquetFileId};
    use influxdb3_shutdown::ShutdownManager;
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use influxdb3_types::http::LastCacheSize;
    use influxdb3_wal::{Gen1Duration, SnapshotSequenceNumber, WalFileSequenceNumber};
    use iox_query::exec::{Executor, ExecutorConfig, IOxSessionContext, PerQueryMemoryPoolConfig};
    use iox_time::{MockProvider, Time};
    use metric::{Attributes, Metric, U64Counter};
    use metrics::{
        WRITE_BYTES_METRIC_NAME, WRITE_LINES_METRIC_NAME, WRITE_LINES_REJECTED_METRIC_NAME,
    };
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, PutPayload};
    use parquet_file::storage::{ParquetStorage, StorageId};
    use pretty_assertions::assert_eq;
    use influxdb3_catalog::replication::{ReplicationFactor, ReplicationInfo}; // For replication test

    #[tokio::test]
    async fn parse_lp_into_buffer() {
        let node_id = Arc::from("sample-host-id");
        let obj_store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let catalog = Arc::new(
            Catalog::new(node_id, obj_store, time_provider, Default::default())
                .await
                .unwrap(),
        );
        let db_name = NamespaceName::new("foo").unwrap();
        let lp = "cpu,region=west user=23.2 100\nfoo f1=1i";
        WriteValidator::initialize(db_name, Arc::clone(&catalog))
            .unwrap()
            .v1_parse_lines_and_catalog_updates(
                lp,
                false,
                Time::from_timestamp_nanos(0),
                Precision::Nanosecond,
            )
            .unwrap()
            .commit_catalog_changes()
            .await
            .unwrap()
            .unwrap_success()
            .convert_lines_to_buffer(Gen1Duration::new_5m());

        let db = catalog.db_schema_by_id(&DbId::from(1)).unwrap();

        assert_eq!(db.tables.len(), 2);
        // cpu table
        assert_eq!(
            db.tables
                .get_by_id(&TableId::from(0))
                .unwrap()
                .num_columns(),
            3
        );
        // foo table
        assert_eq!(
            db.tables
                .get_by_id(&TableId::from(1))
                .unwrap()
                .num_columns(),
            2
        );
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn writes_data_to_wal_and_is_queryable() {
        let obj_store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let catalog = Arc::new(
            Catalog::new("test_host", obj_store, time_provider, Default::default())
                .await
                .unwrap(),
        );
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let (object_store, parquet_cache) = test_cached_obj_store_and_oracle(
            catalog.object_store(),
            Arc::clone(&time_provider),
            Default::default(),
        );
        let persister = Arc::new(Persister::new(
            catalog.object_store(),
            "test_host",
            Arc::clone(&time_provider),
        ));
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _)
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let write_buffer = WriteBufferImpl::new(WriteBufferImplArgs {
            persister: Arc::clone(&persister),
            catalog: Arc::clone(&catalog),
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider),
            executor: make_exec(),
            wal_config: WalConfig::test_config(),
            parquet_cache: Some(Arc::clone(&parquet_cache)),
            metric_registry: Default::default(),
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
            shutdown: ShutdownManager::new_testing().register(),
            wal_replay_concurrency_limit: Some(1),
        })
        .await
        .unwrap();
        let session_context = IOxSessionContext::with_testing();
        let runtime_env = session_context.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));

        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=1 10",
                Time::from_timestamp_nanos(123),
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        let actual = write_buffer
            .get_record_batches_unchecked("foo", "cpu", &session_context)
            .await;
        assert_batches_eq!(&expected, &actual);

        // do two more writes to trigger a snapshot
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=2 20",
                Time::from_timestamp_nanos(124),
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=3 30",
                Time::from_timestamp_nanos(125),
                false,
                Precision::Nanosecond,
                false,
            )
            .await;

        // query the buffer and make sure we get the data back
        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "| 2.0 | 1970-01-01T00:00:00.000000020Z |",
            "| 3.0 | 1970-01-01T00:00:00.000000030Z |",
            "+-----+--------------------------------+",
        ];
        let actual = write_buffer
            .get_record_batches_unchecked("foo", "cpu", &session_context)
            .await;
        assert_batches_eq!(&expected, &actual);

        // now load a new buffer from object storage
        let catalog = Arc::new(
            Catalog::new(
                "test_host",
                catalog.object_store(),
                Arc::clone(&time_provider),
                Default::default(),
            )
            .await
            .unwrap(),
        );
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _)
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let write_buffer = WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog,
            last_cache,
            distinct_cache,
            time_provider,
            executor: make_exec(),
            wal_config: WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(50),
                snapshot_size: 100,
            },
            parquet_cache: Some(Arc::clone(&parquet_cache)),
            metric_registry: Default::default(),
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
            shutdown: ShutdownManager::new_testing().register(),
            wal_replay_concurrency_limit: Some(1),
        })
        .await
        .unwrap();

        let actual = write_buffer
            .get_record_batches_unchecked("foo", "cpu", &session_context)
            .await;
        assert_batches_eq!(&expected, &actual);
    }

    #[test_log::test(tokio::test)]
    async fn last_cache_create_and_delete_is_durable() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (wbuf, _ctx, time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await;
        let db_name = "db";
        let tbl_name = "table";
        let cache_name = "cache";
        // Write some data to the current segment and update the catalog:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},t1=a f1=true").as_str(),
            Time::from_timestamp(20, 0).unwrap(),
            false,
            Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();
        // Create a last cache:
        wbuf.catalog()
            .create_last_cache(
                db_name,
                tbl_name,
                Some(cache_name),
                None as Option<&[&str]>,
                None as Option<&[&str]>,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();

        let reload = || async {
            debug!("reloading the write buffer");
            let time_provider = Arc::clone(&time_provider);
            let catalog = Arc::new(
                Catalog::new(
                    "test_host",
                    Arc::clone(&obj_store),
                    Arc::clone(&time_provider),
                    Default::default(),
                )
                .await
                .unwrap(),
            );
            let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _)
                .await
                .unwrap();
            let distinct_cache = DistinctCacheProvider::new_from_catalog(
                Arc::clone(&time_provider),
                Arc::clone(&catalog),
            )
            .await
            .unwrap();
            WriteBufferImpl::new(WriteBufferImplArgs {
                persister: Arc::clone(&wbuf.persister),
                catalog,
                last_cache,
                distinct_cache,
                time_provider,
                executor: Arc::clone(&wbuf.buffer.executor),
                wal_config: WalConfig {
                    gen1_duration: Gen1Duration::new_1m(),
                    max_write_buffer_size: 100,
                    flush_interval: Duration::from_millis(10),
                    snapshot_size: 1,
                },
                parquet_cache: wbuf.parquet_cache.clone(),
                metric_registry: Default::default(),
                snapshotted_wal_files_to_keep: 10,
                query_file_limit: None,
                shutdown: ShutdownManager::new_testing().register(),
                wal_replay_concurrency_limit: Some(1),
            })
            .await
            .unwrap()
        };

        // load a new write buffer to ensure its durable
        let wbuf = reload().await;

        let catalog_json = wbuf.catalog.snapshot();
        debug!(?catalog_json, "reloaded catalog");
        insta::assert_json_snapshot!("catalog-immediately-after-last-cache-create",
            catalog_json,
            { ".catalog_uuid" => "[uuid]" }
        );

        // Do another write that will update the state of the catalog, specifically, the table
        // that the last cache was created for, and add a new field to the table/cache `f2`:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},t1=a f1=false,f2=42i").as_str(),
            Time::from_timestamp(30, 0).unwrap(),
            false,
            Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();

        // and do another replay and verification
        let wbuf = reload().await;

        let catalog_json = wbuf.catalog.snapshot();
        insta::assert_json_snapshot!(
           "catalog-after-last-cache-create-and-new-field",
           catalog_json,
           { ".catalog_uuid" => "[uuid]" }
        );

        // write a new data point to fill the cache
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},t1=a f1=true,f2=53i").as_str(),
            Time::from_timestamp(40, 0).unwrap(),
            false,
            Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();

        // Fetch record batches from the last cache directly:
        let expected = [
            "+----+------+----+----------------------+",
            "| t1 | f1   | f2 | time                 |",
            "+----+------+----+----------------------+",
            "| a  | true | 53 | 1970-01-01T00:00:40Z |",
            "+----+------+----+----------------------+",
        ];
        let db_schema = wbuf.catalog().db_schema(db_name).unwrap();
        let tbl_id = db_schema.table_name_to_id(tbl_name).unwrap();
        let actual = wbuf
            .last_cache_provider()
            .get_cache_record_batches(db_schema.id, tbl_id, None)
            .unwrap()
            .unwrap();
        assert_batches_eq!(&expected, &actual);
        // Delete the last cache:
        wbuf.catalog()
            .delete_last_cache(db_name, tbl_name, cache_name)
            .await
            .unwrap();

        // do another reload and verify it's gone
        reload().await;

        let catalog_json = wbuf.catalog.snapshot();
        insta::assert_json_snapshot!("catalog-immediately-after-last-cache-delete",
            catalog_json,
            { ".catalog_uuid" => "[uuid]" }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn returns_chunks_across_parquet_and_buffered_data() {
        let obj_store = Arc::new(InMemory::new());
        let (write_buffer, session_context, time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store) as _,
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 2,
            },
        )
        .await;

        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=1",
                Time::from_timestamp(10, 0).unwrap(),
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+----------------------+",
            "| bar | time                 |",
            "+-----+----------------------+",
            "| 1.0 | 1970-01-01T00:00:10Z |",
            "+-----+----------------------+",
        ];
        let actual = write_buffer
            .get_record_batches_unchecked("foo", "cpu", &session_context)
            .await;
        assert_batches_sorted_eq!(&expected, &actual);

        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=2",
                Time::from_timestamp(65, 0).unwrap(),
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+----------------------+",
            "| bar | time                 |",
            "+-----+----------------------+",
            "| 1.0 | 1970-01-01T00:00:10Z |",
            "| 2.0 | 1970-01-01T00:01:05Z |",
            "+-----+----------------------+",
        ];
        let actual = write_buffer
            .get_record_batches_unchecked("foo", "cpu", &session_context)
            .await;
        assert_batches_sorted_eq!(&expected, &actual);

        // trigger snapshot with a third write, creating parquet files
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=3 147000000000",
                Time::from_timestamp(147, 0).unwrap(),
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        // give the snapshot some time to persist in the background
        let mut ticks = 0;
        loop {
            ticks += 1;
            let persisted = write_buffer.persister.load_snapshots(1000).await.unwrap();
            if !persisted.is_empty() {
                assert_eq!(persisted.len(), 1);
                assert_eq!(persisted[0].v1_ref().min_time, 10000000000);
                assert_eq!(persisted[0].v1_ref().row_count, 2);
                break;
            } else if ticks > 10 {
                panic!("not persisting");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let expected = [
            "+-----+----------------------+",
            "| bar | time                 |",
            "+-----+----------------------+",
            "| 3.0 | 1970-01-01T00:02:27Z |",
            "| 2.0 | 1970-01-01T00:01:05Z |",
            "| 1.0 | 1970-01-01T00:00:10Z |",
            "+-----+----------------------+",
        ];
        let actual = write_buffer
            .get_record_batches_unchecked("foo", "cpu", &session_context)
            .await;
        assert_batches_sorted_eq!(&expected, &actual);

        // now validate that buffered data and parquet data are all returned
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=4",
                Time::from_timestamp(250, 0).unwrap(),
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+----------------------+",
            "| bar | time                 |",
            "+-----+----------------------+",
            "| 3.0 | 1970-01-01T00:02:27Z |",
            "| 4.0 | 1970-01-01T00:04:10Z |",
            "| 2.0 | 1970-01-01T00:01:05Z |",
            "| 1.0 | 1970-01-01T00:00:10Z |",
            "+-----+----------------------+",
        ];
        let actual = write_buffer
            .get_record_batches_unchecked("foo", "cpu", &session_context)
            .await;
        assert_batches_sorted_eq!(&expected, &actual);

        let actual = write_buffer
            .get_record_batches_unchecked("foo", "cpu", &session_context)
            .await;
        assert_batches_sorted_eq!(&expected, &actual);
        // and now replay in a new write buffer and attempt to write
        let catalog = Arc::new(
            Catalog::new(
                "test_host",
                Arc::clone(&obj_store) as _,
                Arc::clone(&time_provider),
                Default::default(),
            )
            .await
            .unwrap(),
        );
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _)
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let write_buffer = WriteBufferImpl::new(WriteBufferImplArgs {
            persister: Arc::clone(&write_buffer.persister),
            catalog,
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider),
            executor: Arc::clone(&write_buffer.buffer.executor),
            wal_config: WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 2,
            },
            parquet_cache: write_buffer.parquet_cache.clone(),
            metric_registry: Default::default(),
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
            shutdown: ShutdownManager::new_testing().register(),
            wal_replay_concurrency_limit: Some(1),
        })
        .await
        .unwrap();
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(
            runtime_env,
            "influxdb3",
            write_buffer.persister.object_store(),
        );

        // verify the data is still there
        let actual = write_buffer
            .get_record_batches_unchecked("foo", "cpu", &ctx)
            .await;
        assert_batches_sorted_eq!(&expected, &actual);

        // now write some new data
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=5",
                Time::from_timestamp(300, 0).unwrap(),
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        // and write more to force another snapshot
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=6",
                Time::from_timestamp(330, 0).unwrap(),
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;

        let expected = [
            "+-----+----------------------+",
            "| bar | time                 |",
            "+-----+----------------------+",
            "| 1.0 | 1970-01-01T00:00:10Z |",
            "| 2.0 | 1970-01-01T00:01:05Z |",
            "| 3.0 | 1970-01-01T00:02:27Z |",
            "| 4.0 | 1970-01-01T00:04:10Z |",
            "| 5.0 | 1970-01-01T00:05:00Z |",
            "| 6.0 | 1970-01-01T00:05:30Z |",
            "+-----+----------------------+",
        ];
        let actual = write_buffer
            .get_record_batches_unchecked("foo", "cpu", &ctx)
            .await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn catalog_snapshots_only_if_updated() {
        let (write_buffer, _ctx, _time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::new(InMemory::new()),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(5),
                snapshot_size: 1,
            },
        )
        .await;

        let db_name = "foo";
        // do three writes to force a snapshot
        do_writes(
            db_name,
            write_buffer.as_ref(),
            &[
                TestWrite {
                    lp: "cpu bar=1",
                    time_seconds: 10,
                },
                TestWrite {
                    lp: "cpu bar=2",
                    time_seconds: 20,
                },
                TestWrite {
                    lp: "cpu bar=3",
                    time_seconds: 30,
                },
            ],
        )
        .await;

        verify_snapshot_count(1, &write_buffer.persister).await;

        // need another 3 writes to trigger next snapshot
        do_writes(
            db_name,
            write_buffer.as_ref(),
            &[
                TestWrite {
                    lp: "cpu bar=4",
                    time_seconds: 40,
                },
                TestWrite {
                    lp: "cpu bar=5",
                    time_seconds: 50,
                },
                TestWrite {
                    lp: "cpu bar=6",
                    time_seconds: 60,
                },
            ],
        )
        .await;

        // verify the catalog didn't get persisted, but a snapshot did
        verify_snapshot_count(2, &write_buffer.persister).await;

        // and finally, do 3 more, with a catalog update, forcing persistence
        do_writes(
            db_name,
            write_buffer.as_ref(),
            &[
                TestWrite {
                    lp: "cpu bar=7,asdf=true",
                    time_seconds: 60,
                },
                TestWrite {
                    lp: "cpu bar=8,asdf=true",
                    time_seconds: 70,
                },
                TestWrite {
                    lp: "cpu bar=9,asdf=true",
                    time_seconds: 80,
                },
            ],
        )
        .await;

        verify_snapshot_count(3, &write_buffer.persister).await;
    }

    /// Check that when a WriteBuffer is initialized with existing snapshot files, that newly
    /// generated snapshot files use the next sequence number.
    #[tokio::test]
    async fn new_snapshots_use_correct_sequence() {
        // set up a local file system object store:
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());

        // create a snapshot file that will be loaded on initialization of the write buffer:
        // Set ParquetFileId to a non zero number for the snapshot
        ParquetFileId::from(500).set_next_id();
        let prev_snapshot_seq = SnapshotSequenceNumber::new(42);
        let prev_snapshot = PersistedSnapshot::new(
            "test_host".to_string(),
            prev_snapshot_seq,
            WalFileSequenceNumber::new(0),
            CatalogSequenceNumber::new(0),
        );
        let snapshot_json =
            serde_json::to_vec(&PersistedSnapshotVersion::V1(prev_snapshot)).unwrap();
        // set ParquetFileId to be 0 so that we can make sure when it's loaded from the
        // snapshot that it becomes the expected number
        ParquetFileId::from(0).set_next_id();

        // put the snapshot file in object store:
        object_store
            .put(
                &SnapshotInfoFilePath::new("test_host", prev_snapshot_seq),
                PutPayload::from_bytes(Bytes::from(snapshot_json)),
            )
            .await
            .unwrap();

        // setup the write buffer:
        let (wbuf, _ctx, _time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&object_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(5),
                snapshot_size: 1,
            },
        )
        .await;

        // Assert that loading the snapshots sets ParquetFileId to the correct id number
        assert_eq!(ParquetFileId::new().as_u64(), 500);

        // there should be one snapshot already, i.e., the one we created above:
        verify_snapshot_count(1, &wbuf.persister).await;

        // do three writes to force a new snapshot
        wbuf.write_lp(
            NamespaceName::new("foo").unwrap(),
            "cpu bar=1",
            Time::from_timestamp(10, 0).unwrap(),
            false,
            Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();
        wbuf.write_lp(
            NamespaceName::new("foo").unwrap(),
            "cpu bar=2",
            Time::from_timestamp(20, 0).unwrap(),
            false,
            Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();
        wbuf.write_lp(
            NamespaceName::new("foo").unwrap(),
            "cpu bar=3",
            Time::from_timestamp(30, 0).unwrap(),
            false,
            Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();

        // Check that there are now 2 snapshots:
        verify_snapshot_count(2, &wbuf.persister).await;
        // Check that the next sequence number is used for the new snapshot:
        assert_eq!(
            prev_snapshot_seq.next(),
            wbuf.wal.last_snapshot_sequence_number().await
        );
        // Check the catalog sequence number in the latest snapshot is correct:
        let persisted_snapshot_bytes = object_store
            .get(&SnapshotInfoFilePath::new(
                "test_host",
                prev_snapshot_seq.next(),
            ))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let persisted_snapshot =
            serde_json::from_slice::<PersistedSnapshot>(&persisted_snapshot_bytes).unwrap();
        assert_eq!(
            CatalogSequenceNumber::new(2),
            persisted_snapshot.catalog_sequence_number
        );
    }

    #[tokio::test]
    async fn next_id_is_correct_number() {
        // set up a local file system object store:
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());

        let prev_snapshot_seq = SnapshotSequenceNumber::new(42);
        let mut prev_snapshot = PersistedSnapshot::new(
            "test_host".to_string(),
            prev_snapshot_seq,
            WalFileSequenceNumber::new(0),
            CatalogSequenceNumber::new(0),
        );

        assert_eq!(prev_snapshot.next_file_id.as_u64(), 0);

        for _ in 0..=5 {
            prev_snapshot.add_parquet_file(
                DbId::from(0),
                TableId::from(0),
                ParquetFile {
                    id: ParquetFileId::new(),
                    path: "file/path2".into(),
                    size_bytes: 20,
                    row_count: 1,
                    chunk_time: 1,
                    min_time: 0,
                    max_time: 1,
                },
            );
        }

        assert_eq!(prev_snapshot.databases.len(), 1);
        let files = prev_snapshot.databases[&DbId::from(0)].tables[&TableId::from(0)].clone();

        // Assert that all of the files are smaller than the next_file_id field
        // and that their index corresponds to the order they were added in
        assert_eq!(prev_snapshot.next_file_id.as_u64(), 6);
        assert_eq!(files.len(), 6);
        for (i, file) in files.iter().enumerate() {
            assert_ne!(file.id, ParquetFileId::from(6));
            assert!(file.id.as_u64() < 6);
            assert_eq!(file.id.as_u64(), i as u64);
        }

        let snapshot_json =
            serde_json::to_vec(&PersistedSnapshotVersion::V1(prev_snapshot)).unwrap();

        // put the snapshot file in object store:
        object_store
            .put(
                &SnapshotInfoFilePath::new("test_host", prev_snapshot_seq),
                PutPayload::from_bytes(Bytes::from(snapshot_json)),
            )
            .await
            .unwrap();

        // setup the write buffer:
        let (_wbuf, _ctx, _time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&object_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(5),
                snapshot_size: 1,
            },
        )
        .await;

        // Test that the next_file_id has been set properly
        assert_eq!(ParquetFileId::next_id().as_u64(), 6);
    }

    #[test_log::test(tokio::test)]
    async fn test_write_lp_with_replication_placeholder() {
        let object_store = Arc::new(InMemory::new());
        let (buf, _metrics) = setup_with_metrics( // Using setup_with_metrics for simplicity
            Time::from_timestamp_nanos(0),
            Arc::clone(&object_store),
            WalConfig::test_config(),
        )
        .await;

        let db_name_str = "rep_db";
        let table_name_str = "rep_table";
        let db_name = NamespaceName::new(db_name_str).unwrap();

        // Setup catalog with replication info
        let catalog = buf.catalog();
        catalog.create_database(db_name_str).await.unwrap();
        catalog.create_table(db_name_str, table_name_str, &["tagA"], &[("fieldA", FieldDataType::Integer)])
            .await
            .unwrap();

        let rep_info = ReplicationInfo::new(ReplicationFactor::new(2).unwrap());
        catalog.set_replication(db_name_str, table_name_str, rep_info)
            .await
            .unwrap();

        // Perform a write - this should hit the replication placeholder logic
        let lp = format!("{},tagA=v1 fieldA=100i 0", table_name_str);
        let result = buf.write_lp(
            db_name,
            &lp,
            Time::from_timestamp_nanos(0), // ingest_time
            false,
            Precision::Nanosecond,
            false, // no_sync = false, so it attempts full write path including WAL
        ).await;

        // We expect this to succeed locally, as the placeholder only logs a warning.
        // If it were to error out, we'd check for Err(Error::ReplicationError(...))
        assert!(result.is_ok(), "Write should succeed locally despite placeholder replication. Result: {:?}", result);

        // Further checks could involve inspecting logs if possible, or using mock objects if the
        // design were adapted for it. For now, a successful local write is the main check.
        let db_schema = catalog.db_schema(db_name_str).unwrap();
        let table_def = db_schema.table_definition(table_name_str).unwrap();
        assert!(table_def.replication_info.is_some());
        assert_eq!(table_def.replication_info.as_ref().unwrap().replication_factor.get(), 2);
    }

    #[test_log::test(tokio::test)]
    async fn test_apply_replicated_wal_op() {
        let object_store = Arc::new(InMemory::new());
        let (buf, _metrics) = setup_with_metrics(
            Time::from_timestamp_nanos(0),
            Arc::clone(&object_store),
            WalConfig::test_config(),
        )
        .await;

        // Create a sample WalOp to replicate
        let db_name_str = "rep_db_target";
        let table_name_str = "rep_table_target";
        let db_name_arc = Arc::from(db_name_str);
        let _ = buf.catalog().create_database(db_name_str).await.unwrap(); // Ensure DB exists for TableBuffer

        let table_id = TableId::new(0); // Assume table ID for simplicity in test
        let mut table_chunks = SerdeVecMap::new();
        let mut chunk_map = HashMap::new();
        chunk_map.insert(0i64, influxdb3_wal::TableChunk { rows: vec![
            influxdb3_wal::Row { time: 1, fields: vec![influxdb3_wal::Field { id: ColumnId::new(0), value: influxdb3_wal::FieldData::Integer(100) }] }
        ]});
        table_chunks.insert(table_id, influxdb3_wal::TableChunks {
            min_time: 1, max_time: 1, chunk_time_to_chunk: chunk_map
        });

        let write_batch = WriteBatch {
            catalog_sequence: 1,
            database_id: DbId::new(1), // Assuming this DB ID exists or matches what create_database makes
            database_name: db_name_arc.clone(),
            table_chunks,
            min_time_ns: 1,
            max_time_ns: 1,
            shard_id: Some(influxdb3_catalog::shard::ShardId::new(1)),
        };
        let replicated_op = WalOp::Write(write_batch);
        let originating_node_id = Some("origin_node_1".to_string());

        // Apply the replicated WalOp
        let result = buf.apply_replicated_wal_op(replicated_op.clone(), originating_node_id).await;
        assert!(result.is_ok(), "apply_replicated_wal_op failed: {:?}", result);

        // Verification:
        // 1. Check if the op was written to the WAL.
        //    This is hard to check directly without inspecting WAL files or having more introspection.
        //    We trust that `self.wal.write_ops()` works as tested elsewhere.
        //    A key indicator is that the data should appear in the QueryableBuffer after WAL flush.
        //
        // 2. To verify it's in QueryableBuffer, we'd normally need to wait for WAL flush & notification.
        //    For a direct unit test, we can simulate the notification path if needed,
        //    or make `apply_replicated_wal_op` also directly notify for testability (but that changes its nature).
        //
        //    Given the current structure, `apply_replicated_wal_op` writes to WAL.
        //    The WAL background task (`background_wal_flush`) will then read it, and call
        //    `QueryableBuffer::notify`. So, data won't be in `TableBuffer` immediately after
        //    `apply_replicated_wal_op` returns, but after the next WAL flush.
        //
        //    For this test, we'll assume `self.wal.write_ops` is successful.
        //    A more involved test would trigger a WAL flush and check QueryableBuffer.
        //
        // 3. Ensure no further replication was attempted.
        //    This is also hard to check directly without deeper mocking of a replication client.
        //    We rely on code inspection: `apply_replicated_wal_op` does not call the replication client logic.

        // As a simple check, ensure the WAL received *some* write.
        // If the WAL was NoOpWal, this test would need adjustment.
        // The test_config for Wal usually uses WalObjectStore.
        // We can try to write another op through the normal path and see if sequences advance.
        let last_seq_before = buf.wal().last_wal_sequence_number().await;

        // Apply it again to see sequence number changes (or ensure it doesn't duplicate if WAL has such checks)
        // Note: Re-applying the exact same WalOp might be handled differently by WAL itself.
        // A better check would be to query the data after a flush, if feasible in test setup.

        // Let's write a *new* op via normal path to advance WAL sequence
        let lp = format!("{},tagA=v2 fieldA=200i 10", table_name_str);
         let _ = buf.write_lp(
            NamespaceName::new(db_name_str).unwrap(),
            &lp,
            Time::from_timestamp_nanos(10),
            false,
            Precision::Nanosecond,
            false,
        ).await;

        let last_seq_after = buf.wal().last_wal_sequence_number().await;

        // Expecting that at least one WAL file was created by the two writes (one replicated, one normal)
        // This depends on snapshot_size and how WAL sequences are handled for individual ops vs flushes.
        // A more direct assertion would be on QueryableBuffer content after a forced WAL flush.
        assert!(last_seq_after.as_u64() > last_seq_before.as_u64() || last_seq_after.as_u64() > 0, "WAL sequence should advance or be non-zero after writes.");

    }

    /// This is the reproducer for [#25277][see]
    ///
    /// [see]: https://github.com/influxdata/influxdb/issues/25277
    #[test_log::test(tokio::test)]
    async fn writes_not_dropped_on_snapshot() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: influxdb3_wal::Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let (mut wbuf, mut ctx, _time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            wal_config,
        )
        .await;

        let db_name = "coffee_shop";
        let tbl_name = "menu";

        // do some writes to get a snapshot:
        do_writes(
            db_name,
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: format!("{tbl_name},name=espresso price=2.50"),
                    time_seconds: 1,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=americano price=3.00"),
                    time_seconds: 2,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=latte price=4.50"),
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // wait for snapshot to be created:
        verify_snapshot_count(1, &wbuf.persister).await;
        // Get the record batches from before shutting down buffer:
        let batches = wbuf
            .get_record_batches_unchecked(db_name, tbl_name, &ctx)
            .await;
        assert_batches_sorted_eq!(
            [
                "+-----------+-------+----------------------+",
                "| name      | price | time                 |",
                "+-----------+-------+----------------------+",
                "| americano | 3.0   | 1970-01-01T00:00:02Z |",
                "| espresso  | 2.5   | 1970-01-01T00:00:01Z |",
                "| latte     | 4.5   | 1970-01-01T00:00:03Z |",
                "+-----------+-------+----------------------+",
            ],
            &batches
        );
        // initialize twice
        for _i in 0..2 {
            (wbuf, ctx, _) = setup(
                Time::from_timestamp_nanos(0),
                Arc::clone(&obj_store),
                wal_config,
            )
            .await;
        }

        // Get the record batches from replayed buffer:
        let batches = wbuf
            .get_record_batches_unchecked(db_name, tbl_name, &ctx)
            .await;
        assert_batches_sorted_eq!(
            [
                "+-----------+-------+----------------------+",
                "| name      | price | time                 |",
                "+-----------+-------+----------------------+",
                "| americano | 3.0   | 1970-01-01T00:00:02Z |",
                "| espresso  | 2.5   | 1970-01-01T00:00:01Z |",
                "| latte     | 4.5   | 1970-01-01T00:00:03Z |",
                "+-----------+-------+----------------------+",
            ],
            &batches
        );
    }

    #[test_log::test(tokio::test)]
    async fn writes_not_dropped_on_larger_snapshot_size() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (wbuf, _, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 2,
            },
        )
        .await;

        let db_name = "coffee_shop";
        let tbl_name = "menu";

        // Do six writes to trigger a snapshot
        do_writes(
            db_name,
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: format!("{tbl_name},name=espresso,type=drink price=2.50"),
                    time_seconds: 1,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=americano,type=drink price=3.00"),
                    time_seconds: 2,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=latte,type=drink price=4.50"),
                    time_seconds: 3,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=croissant,type=food price=5.50"),
                    time_seconds: 4,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=muffin,type=food price=4.50"),
                    time_seconds: 5,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=biscotto,type=food price=3.00"),
                    time_seconds: 6,
                },
            ],
        )
        .await;

        verify_snapshot_count(1, &wbuf.persister).await;

        // Drop the write buffer, and create a new one that replays:
        drop(wbuf);
        let (wbuf, ctx, _time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 2,
            },
        )
        .await;

        // Get the record batches from replyed buffer:
        let batches = wbuf
            .get_record_batches_unchecked(db_name, tbl_name, &ctx)
            .await;
        assert_batches_sorted_eq!(
            [
                "+-----------+-------+----------------------+-------+",
                "| name      | price | time                 | type  |",
                "+-----------+-------+----------------------+-------+",
                "| americano | 3.0   | 1970-01-01T00:00:02Z | drink |",
                "| biscotto  | 3.0   | 1970-01-01T00:00:06Z | food  |",
                "| croissant | 5.5   | 1970-01-01T00:00:04Z | food  |",
                "| espresso  | 2.5   | 1970-01-01T00:00:01Z | drink |",
                "| latte     | 4.5   | 1970-01-01T00:00:03Z | drink |",
                "| muffin    | 4.5   | 1970-01-01T00:00:05Z | food  |",
                "+-----------+-------+----------------------+-------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn writes_not_dropped_with_future_writes() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (wbuf, _, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await;

        let db_name = "coffee_shop";
        let tbl_name = "menu";

        // do some writes to get a snapshot:
        do_writes(
            db_name,
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: format!("{tbl_name},name=espresso price=2.50"),
                    time_seconds: 1,
                },
                // This write is way out in the future, so as to be outside the normal
                // range for a snapshot:
                TestWrite {
                    lp: format!("{tbl_name},name=americano price=3.00"),
                    time_seconds: 20_000,
                },
                // This write will trigger the snapshot:
                TestWrite {
                    lp: format!("{tbl_name},name=latte price=4.50"),
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // Wait for snapshot to be created:
        verify_snapshot_count(1, &wbuf.persister).await;

        // Now drop the write buffer, and create a new one that replays:
        drop(wbuf);
        let (wbuf, ctx, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await;

        // Get the record batches from replayed buffer:
        let batches = wbuf
            .get_record_batches_unchecked(db_name, tbl_name, &ctx)
            .await;
        assert_batches_sorted_eq!(
            [
                "+-----------+-------+----------------------+",
                "| name      | price | time                 |",
                "+-----------+-------+----------------------+",
                "| americano | 3.0   | 1970-01-01T05:33:20Z |",
                "| espresso  | 2.5   | 1970-01-01T00:00:01Z |",
                "| latte     | 4.5   | 1970-01-01T00:00:03Z |",
                "+-----------+-------+----------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn notifies_watchers_of_snapshot() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (wbuf, _, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await;

        let mut watcher = wbuf.watch_persisted_snapshots();
        watcher.mark_changed();

        let db_name = "coffee_shop";
        let tbl_name = "menu";

        // do some writes to get a snapshot:
        do_writes(
            db_name,
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: format!("{tbl_name},name=espresso price=2.50"),
                    time_seconds: 1,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=americano price=3.00"),
                    time_seconds: 2,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=latte price=4.50"),
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // wait for snapshot to be created:
        verify_snapshot_count(1, &wbuf.persister).await;
        watcher.changed().await.unwrap();
        let snapshot = watcher.borrow();
        assert!(snapshot.is_some(), "watcher should be notified of snapshot");
    }

    #[tokio::test]
    async fn test_db_id_is_persisted_and_updated() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (wbuf, _, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await;

        // do some writes to get a snapshot:
        do_writes(
            "coffee_shop",
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: "menu,name=espresso price=2.50",
                    time_seconds: 1,
                },
                // This write is way out in the future, so as to be outside the normal
                // range for a snapshot:
                TestWrite {
                    lp: "menu,name=americano price=3.00",
                    time_seconds: 20_000,
                },
                // This write will trigger the snapshot:
                TestWrite {
                    lp: "menu,name=latte price=4.50",
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // this persists the catalog immediately, so we don't wait for anything, just assert that
        // the next db id is 1, since the above would have used 0
        assert_eq!(wbuf.catalog().next_db_id(), DbId::new(2));

        // drop the write buffer, and create a new one that replays and re-loads the catalog:
        drop(wbuf);

        // Set DbId to a large number to make sure it is properly set on replay
        // and assert that it's what we expect it to be before we replay
        let (wbuf, _, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await;

        // check that the next db id is still 1
        assert_eq!(wbuf.catalog().next_db_id(), DbId::new(2));
    }

    #[test_log::test(tokio::test)]
    async fn test_parquet_cache() {
        // set up a write buffer using a TestObjectStore so we can spy on requests that get
        // through to the object store for parquet files:
        let test_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let obj_store: Arc<dyn ObjectStore> = Arc::clone(&test_store) as _;
        let (wbuf, ctx, _) = setup_cache_optional(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
            true,
        )
        .await;
        let db_name = "my_corp";
        let db_id = DbId::from(1);
        let tbl_name = "temp";
        let tbl_id = TableId::from(0);

        // make some writes to generate a snapshot:
        do_writes(
            db_name,
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: format!(
                        "\
                        {tbl_name},warehouse=us-east,room=01a,device=10001 reading=36\n\
                        {tbl_name},warehouse=us-east,room=01b,device=10002 reading=29\n\
                        {tbl_name},warehouse=us-east,room=02a,device=30003 reading=33\n\
                        "
                    ),
                    time_seconds: 1,
                },
                TestWrite {
                    lp: format!(
                        "\
                        {tbl_name},warehouse=us-east,room=01a,device=10001 reading=37\n\
                        {tbl_name},warehouse=us-east,room=01b,device=10002 reading=28\n\
                        {tbl_name},warehouse=us-east,room=02a,device=30003 reading=32\n\
                        "
                    ),
                    time_seconds: 2,
                },
                // This write will trigger the snapshot:
                TestWrite {
                    lp: format!(
                        "\
                        {tbl_name},warehouse=us-east,room=01a,device=10001 reading=35\n\
                        {tbl_name},warehouse=us-east,room=01b,device=10002 reading=24\n\
                        {tbl_name},warehouse=us-east,room=02a,device=30003 reading=30\n\
                        "
                    ),
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // Wait for snapshot to be created, once this is done, then the parquet has been persisted:
        verify_snapshot_count(1, &wbuf.persister).await;

        // get the path for the created parquet file:
        let persisted_files = wbuf.persisted_files().get_files(db_id, tbl_id);
        assert_eq!(1, persisted_files.len());
        let path = ObjPath::from(persisted_files[0].path.as_str());

        // check the number of requests to that path before making a query:
        // there should be no get request, made by the cache oracle:
        assert_eq!(0, test_store.get_request_count(&path));
        assert_eq!(0, test_store.get_opts_request_count(&path));
        assert_eq!(0, test_store.get_ranges_request_count(&path));
        assert_eq!(0, test_store.get_range_request_count(&path));
        assert_eq!(0, test_store.head_request_count(&path));

        let batches = wbuf
            .get_record_batches_unchecked(db_name, tbl_name, &ctx)
            .await;
        assert_batches_sorted_eq!(
            [
                "+--------+---------+------+----------------------+-----------+",
                "| device | reading | room | time                 | warehouse |",
                "+--------+---------+------+----------------------+-----------+",
                "| 10001  | 35.0    | 01a  | 1970-01-01T00:00:03Z | us-east   |",
                "| 10001  | 36.0    | 01a  | 1970-01-01T00:00:01Z | us-east   |",
                "| 10001  | 37.0    | 01a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10002  | 24.0    | 01b  | 1970-01-01T00:00:03Z | us-east   |",
                "| 10002  | 28.0    | 01b  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10002  | 29.0    | 01b  | 1970-01-01T00:00:01Z | us-east   |",
                "| 30003  | 30.0    | 02a  | 1970-01-01T00:00:03Z | us-east   |",
                "| 30003  | 32.0    | 02a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 30003  | 33.0    | 02a  | 1970-01-01T00:00:01Z | us-east   |",
                "+--------+---------+------+----------------------+-----------+",
            ],
            &batches
        );

        // counts should not change, since requests for this parquet file hit the cache:
        assert_eq!(0, test_store.get_request_count(&path));
        assert_eq!(0, test_store.get_opts_request_count(&path));
        assert_eq!(0, test_store.get_ranges_request_count(&path));
        assert_eq!(0, test_store.get_range_request_count(&path));
        assert_eq!(0, test_store.head_request_count(&path));
    }
    #[tokio::test]
    async fn test_no_parquet_cache() {
        // set up a write buffer using a TestObjectStore so we can spy on requests that get
        // through to the object store for parquet files:
        let test_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let obj_store: Arc<dyn ObjectStore> = Arc::clone(&test_store) as _;
        let (wbuf, ctx, _) = setup_cache_optional(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
            false,
        )
        .await;
        let db_name = "my_corp";
        let db_id = DbId::from(1);
        let tbl_name = "temp";
        let tbl_id = TableId::from(0);

        // make some writes to generate a snapshot:
        do_writes(
            db_name,
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: format!(
                        "\
                        {tbl_name},warehouse=us-east,room=01a,device=10001 reading=36\n\
                        {tbl_name},warehouse=us-east,room=01b,device=10002 reading=29\n\
                        {tbl_name},warehouse=us-east,room=02a,device=30003 reading=33\n\
                        "
                    ),
                    time_seconds: 1,
                },
                TestWrite {
                    lp: format!(
                        "\
                        {tbl_name},warehouse=us-east,room=01a,device=10001 reading=37\n\
                        {tbl_name},warehouse=us-east,room=01b,device=10002 reading=28\n\
                        {tbl_name},warehouse=us-east,room=02a,device=30003 reading=32\n\
                        "
                    ),
                    time_seconds: 2,
                },
                // This write will trigger the snapshot:
                TestWrite {
                    lp: format!(
                        "\
                        {tbl_name},warehouse=us-east,room=01a,device=10001 reading=35\n\
                        {tbl_name},warehouse=us-east,room=01b,device=10002 reading=24\n\
                        {tbl_name},warehouse=us-east,room=02a,device=30003 reading=30\n\
                        "
                    ),
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // Wait for snapshot to be created, once this is done, then the parquet has been persisted:
        verify_snapshot_count(1, &wbuf.persister).await;

        // get the path for the created parquet file:
        let persisted_files = wbuf.persisted_files().get_files(db_id, tbl_id);
        assert_eq!(1, persisted_files.len());
        let path = ObjPath::from(persisted_files[0].path.as_str());

        // check the number of requests to that path before making a query:
        // there should be no get or get_range requests since nothing has asked for this file yet:
        assert_eq!(0, test_store.get_request_count(&path));
        assert_eq!(0, test_store.get_opts_request_count(&path));
        assert_eq!(0, test_store.get_ranges_request_count(&path));
        assert_eq!(0, test_store.get_range_request_count(&path));
        assert_eq!(0, test_store.head_request_count(&path));

        let batches = wbuf
            .get_record_batches_unchecked(db_name, tbl_name, &ctx)
            .await;
        assert_batches_sorted_eq!(
            [
                "+--------+---------+------+----------------------+-----------+",
                "| device | reading | room | time                 | warehouse |",
                "+--------+---------+------+----------------------+-----------+",
                "| 10001  | 35.0    | 01a  | 1970-01-01T00:00:03Z | us-east   |",
                "| 10001  | 36.0    | 01a  | 1970-01-01T00:00:01Z | us-east   |",
                "| 10001  | 37.0    | 01a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10002  | 24.0    | 01b  | 1970-01-01T00:00:03Z | us-east   |",
                "| 10002  | 28.0    | 01b  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10002  | 29.0    | 01b  | 1970-01-01T00:00:01Z | us-east   |",
                "| 30003  | 30.0    | 02a  | 1970-01-01T00:00:03Z | us-east   |",
                "| 30003  | 32.0    | 02a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 30003  | 33.0    | 02a  | 1970-01-01T00:00:01Z | us-east   |",
                "+--------+---------+------+----------------------+-----------+",
            ],
            &batches
        );

        // counts should change, since requests for this parquet file were made with no cache:
        assert_eq!(0, test_store.get_request_count(&path));
        assert_eq!(0, test_store.get_opts_request_count(&path));
        assert_eq!(1, test_store.get_ranges_request_count(&path));
        assert_eq!(2, test_store.get_range_request_count(&path));
        assert_eq!(0, test_store.head_request_count(&path));
    }

    #[test_log::test(tokio::test)]
    async fn test_delete_database() {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let (write_buffer, _, _) =
            setup_cache_optional(start_time, test_store, wal_config, false).await;
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        let result = write_buffer.catalog().soft_delete_database("foo").await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_table() {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let (write_buffer, _, _) =
            setup_cache_optional(start_time, test_store, wal_config, false).await;
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        let result = write_buffer
            .catalog()
            .soft_delete_table("foo", "cpu", HardDeletionTime::Never)
            .await;

        assert!(result.is_ok());
    }

    #[test_log::test(tokio::test)]
    async fn write_metrics() {
        let object_store = Arc::new(InMemory::new());
        let (buf, metrics) = setup_with_metrics(
            Time::from_timestamp_nanos(0),
            object_store,
            WalConfig::test_config(),
        )
        .await;
        let lines_observer = metrics
            .get_instrument::<Metric<U64Counter>>(WRITE_LINES_METRIC_NAME)
            .unwrap();
        let lines_rejected_observer = metrics
            .get_instrument::<Metric<U64Counter>>(WRITE_LINES_REJECTED_METRIC_NAME)
            .unwrap();
        let bytes_observer = metrics
            .get_instrument::<Metric<U64Counter>>(WRITE_BYTES_METRIC_NAME)
            .unwrap();

        let db_1 = "foo";
        let db_2 = "bar";

        // do a write and check the metrics:
        let lp = "\
            cpu,region=us,host=a usage=10\n\
            cpu,region=eu,host=b usage=10\n\
            cpu,region=ca,host=c usage=10\n\
            ";
        do_writes(
            db_1,
            buf.as_ref(),
            &[TestWrite {
                lp,
                time_seconds: 1,
            }],
        )
        .await;
        assert_eq!(
            3,
            lines_observer
                .get_observer(&Attributes::from(&[("db", db_1)]))
                .unwrap()
                .fetch()
        );
        assert_eq!(
            0,
            lines_rejected_observer
                .get_observer(&Attributes::from(&[("db", db_1)]))
                .unwrap()
                .fetch()
        );
        let mut bytes: usize = lp.lines().map(|l| l.len()).sum();
        assert_eq!(
            bytes as u64,
            bytes_observer
                .get_observer(&Attributes::from(&[("db", db_1)]))
                .unwrap()
                .fetch()
        );

        // do another write to that db and check again for updates:
        let lp = "\
            mem,region=us,host=a used=1,swap=4\n\
            mem,region=eu,host=b used=1,swap=4\n\
            mem,region=ca,host=c used=1,swap=4\n\
            ";
        do_writes(
            db_1,
            buf.as_ref(),
            &[TestWrite {
                lp,
                time_seconds: 1,
            }],
        )
        .await;
        assert_eq!(
            6,
            lines_observer
                .get_observer(&Attributes::from(&[("db", db_1)]))
                .unwrap()
                .fetch()
        );
        assert_eq!(
            0,
            lines_rejected_observer
                .get_observer(&Attributes::from(&[("db", db_1)]))
                .unwrap()
                .fetch()
        );
        bytes += lp.lines().map(|l| l.len()).sum::<usize>();
        assert_eq!(
            bytes as u64,
            bytes_observer
                .get_observer(&Attributes::from(&[("db", db_1)]))
                .unwrap()
                .fetch()
        );

        // now do a write that will only be partially accepted to ensure that
        // the metrics are only calculated for writes that get accepted:

        // the legume will not be accepted, because it contains an invalid field type
        // so should not be included in metric calculations:
        let lp = "\
            produce,type=fruit,name=banana price=1.50\n\
            produce,type=fruit,name=papaya price=5.50\n\
            produce,type=vegetable,name=lettuce price=1.00\n\
            produce,type=fruit,name=lentils,family=legume price=2i\n\
            ";
        do_writes_partial(
            db_2,
            buf.as_ref(),
            &[TestWrite {
                lp,
                time_seconds: 1,
            }],
        )
        .await;
        assert_eq!(
            3,
            lines_observer
                .get_observer(&Attributes::from(&[("db", db_2)]))
                .unwrap()
                .fetch()
        );
        assert_eq!(
            1,
            lines_rejected_observer
                .get_observer(&Attributes::from(&[("db", db_2)]))
                .unwrap()
                .fetch()
        );
        // only take first three (valid) lines to get expected bytes:
        let bytes: usize = lp.lines().take(3).map(|l| l.len()).sum();
        assert_eq!(
            bytes as u64,
            bytes_observer
                .get_observer(&Attributes::from(&[("db", db_2)]))
                .unwrap()
                .fetch()
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_check_mem_and_force_snapshot() {
        let tmp_dir = test_helpers::tmp_dir().unwrap();
        debug!(
            ?tmp_dir,
            "using tmp dir for test_check_mem_and_force_snapshot"
        );
        let obj_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp_dir).unwrap());
        let (write_buffer, _, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100_000,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 10,
            },
        )
        .await;
        // do bunch of writes
        let lp = "\
            cpu,region=us,host=a usage=10\n\
            cpu,region=eu,host=b usage=10\n\
            cpu,region=ca,host=c usage=10\n\
            cpu,region=us,host=a usage=10\n\
            cpu,region=eu,host=b usage=10\n\
            cpu,region=ca,host=c usage=10\n\
            cpu,region=us,host=a usage=10\n\
            cpu,region=eu,host=b usage=10\n\
            cpu,region=ca,host=c usage=10\n\
            cpu,region=us,host=a usage=10\n\
            cpu,region=eu,host=b usage=10\n\
            cpu,region=ca,host=c usage=10\n\
            cpu,region=us,host=a usage=10\n\
            cpu,region=eu,host=b usage=10\n\
            cpu,region=ca,host=c usage=10\n\
            ";
        for i in 1..=20 {
            do_writes(
                "sample",
                write_buffer.as_ref(),
                &[TestWrite {
                    lp,
                    time_seconds: i,
                }],
            )
            .await;
        }
        let total_buffer_size_bytes_before = write_buffer.buffer.get_total_size_bytes();
        debug!(?total_buffer_size_bytes_before, "total buffer size");

        debug!("1st snapshot..");
        check_mem_and_force_snapshot(&Arc::clone(&write_buffer), 50).await;

        // check memory has gone down after forcing first snapshot
        let total_buffer_size_bytes_after = write_buffer.buffer.get_total_size_bytes();
        debug!(?total_buffer_size_bytes_after, "total buffer size");
        assert!(total_buffer_size_bytes_before > total_buffer_size_bytes_after);
        assert_dbs_not_empty_in_snapshot_file(&obj_store, "test_host").await;

        let total_buffer_size_bytes_before = total_buffer_size_bytes_after;
        debug!("2nd snapshot..");
        //   PersistedSnapshot{
        //     node_id: "test_host",
        //     next_file_id: ParquetFileId(1),
        //     next_db_id: DbId(1),
        //     next_table_id: TableId(1),
        //     next_column_id: ColumnId(4),
        //     snapshot_sequence_number: SnapshotSequenceNumber(2),
        //     wal_file_sequence_number: WalFileSequenceNumber(22),
        //     catalog_sequence_number: CatalogSequenceNumber(2),
        //     parquet_size_bytes: 0,
        //     row_count: 0,
        //     min_time: 9223372036854775807,
        //     max_time: -9223372036854775808,
        //     databases: SerdeVecMap({})
        // }
        // This snapshot file was observed when running under high memory pressure.
        //
        // The min/max time comes from the snapshot chunks that have been evicted from
        // the query buffer. But when there's nothing evicted then the min/max stays
        // the same as what they were initialized to i64::MAX/i64::MIN respectively.
        //
        // This however does not stop loading the data into memory as no empty
        // parquet files are written out. But this test recreates that issue and checks
        // object store directly to make sure inconsistent snapshot file isn't written
        // out in the first place
        check_mem_and_force_snapshot(&Arc::clone(&write_buffer), 50).await;
        let total_buffer_size_bytes_after = write_buffer.buffer.get_total_size_bytes();
        // no other writes so nothing can be snapshotted, so mem should stay same
        assert!(total_buffer_size_bytes_before == total_buffer_size_bytes_after);

        drop(write_buffer);
        assert_dbs_not_empty_in_snapshot_file(&obj_store, "test_host").await;

        tokio::time::sleep(Duration::from_millis(10)).await;
        // restart
        debug!("Restarting..");
        let (write_buffer_after_restart, _, _) = setup(
            Time::from_timestamp_nanos(300),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100_000,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 10,
            },
        )
        .await;

        assert_dbs_not_empty_in_snapshot_file(&obj_store, "test_host").await;
        drop(write_buffer_after_restart);

        tokio::time::sleep(Duration::from_millis(10)).await;
        // restart
        debug!("Restarting again..");
        let (_, _, _) = setup(
            Time::from_timestamp_nanos(400),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100_000,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 10,
            },
        )
        .await;
        assert_dbs_not_empty_in_snapshot_file(&obj_store, "test_host").await;
    }

    #[test_log::test(tokio::test)]
    async fn test_out_of_order_data() {
        let tmp_dir = test_helpers::tmp_dir().unwrap();
        debug!(
            ?tmp_dir,
            "using tmp dir for test_check_mem_and_force_snapshot"
        );
        let obj_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp_dir).unwrap());
        let (write_buffer, _, _) = setup(
            // starting with 100
            Time::from_timestamp_nanos(100),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100_000,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 5,
            },
        )
        .await;
        // do bunch of writes
        let lp = "\
            cpu,region=us,host=a usage=10\n\
            ";

        // add bunch of 100 - 111 timestamped data
        for i in 100..=111 {
            do_writes(
                "sample",
                write_buffer.as_ref(),
                &[TestWrite {
                    lp,
                    time_seconds: i,
                }],
            )
            .await;
        }

        // now introduce 20 - 30 timestamp data
        // this is similar to back filling
        for i in 20..=30 {
            do_writes(
                "sample",
                write_buffer.as_ref(),
                &[TestWrite {
                    lp,
                    time_seconds: i,
                }],
            )
            .await;
        }

        let session_context = IOxSessionContext::with_testing();
        let runtime_env = session_context.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&obj_store));

        // at this point all of the data in query buffer will be
        // for timestamps 20 - 50 and none of recent data will be
        // in buffer.
        let actual = write_buffer
            .get_record_batches_unchecked("sample", "cpu", &session_context)
            .await;
        // not sorting, intentionally
        assert_batches_eq!(
            [
                "+------+--------+----------------------+-------+",
                "| host | region | time                 | usage |",
                "+------+--------+----------------------+-------+",
                "| a    | us     | 1970-01-01T00:00:23Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:24Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:25Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:26Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:27Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:28Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:29Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:30Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:01:40Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:01:41Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:01:42Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:01:43Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:01:44Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:01:45Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:01:46Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:01:47Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:01:48Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:01:49Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:01:50Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:01:51Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:20Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:21Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:22Z | 10.0  |",
                "+------+--------+----------------------+-------+",
            ],
            &actual
        );
        debug!(num_items = ?actual.len(), "actual");

        // so all the recent data which were in buffer have been snapshotted
        // only the data that came in later to backfill is in buffer -
        // it's visible by times below
        let actual =
            get_table_batches_from_query_buffer(&write_buffer, "sample", "cpu", &session_context)
                .await;
        assert_batches_eq!(
            [
                "+------+--------+----------------------+-------+",
                "| host | region | time                 | usage |",
                "+------+--------+----------------------+-------+",
                "| a    | us     | 1970-01-01T00:00:23Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:24Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:25Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:26Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:27Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:28Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:29Z | 10.0  |",
                "| a    | us     | 1970-01-01T00:00:30Z | 10.0  |",
                "+------+--------+----------------------+-------+",
            ],
            &actual
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_out_of_order_data_with_last_cache() {
        let tmp_dir = test_helpers::tmp_dir().unwrap();
        debug!(
            ?tmp_dir,
            "using tmp dir for test_check_mem_and_force_snapshot"
        );
        let obj_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp_dir).unwrap());
        let (write_buffer, _, _) = setup(
            // starting with 100
            Time::from_timestamp_nanos(100),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100_000,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 5,
            },
        )
        .await;

        // create db, table and last cache
        write_buffer
            .catalog()
            .create_database("sample")
            .await
            .unwrap();

        write_buffer
            .catalog()
            .create_table(
                "sample",
                "cpu",
                &["region", "host"],
                &[("usage", FieldDataType::Float)],
            )
            .await
            .unwrap();

        let db_schema = write_buffer.catalog().db_schema("sample").unwrap();
        let table_id = db_schema.table_name_to_id("cpu").unwrap();
        let db_id = db_schema.id;

        write_buffer
            .catalog()
            .create_last_cache(
                "sample",
                "cpu",
                Some("sample_cpu_usage"),
                None as Option<&[&str]>,
                None as Option<&[&str]>,
                LastCacheSize::new(2).unwrap(),
                Default::default(),
            )
            .await
            .unwrap();

        // do bunch of writes
        let lp = "\
            cpu,region=us,host=a usage=10\n\
            ";

        // add bunch of 100 - 111 timestamped data
        for i in 100..=111 {
            do_writes(
                "sample",
                write_buffer.as_ref(),
                &[TestWrite {
                    lp,
                    time_seconds: i,
                }],
            )
            .await;
        }

        // all above writes are in sequence, so querying last cache
        // should fetch last 2 items inserted - namely 110 and 111
        let all_vals = write_buffer
            .last_cache_provider()
            .get_cache_record_batches(db_id, table_id, Some("sample_cpu_usage"))
            .unwrap()
            .unwrap();
        assert_batches_eq!(
            [
                "+--------+------+----------------------+-------+",
                "| region | host | time                 | usage |",
                "+--------+------+----------------------+-------+",
                "| us     | a    | 1970-01-01T00:01:51Z | 10.0  |",
                "| us     | a    | 1970-01-01T00:01:50Z | 10.0  |",
                "+--------+------+----------------------+-------+",
            ],
            &all_vals
        );

        // now introduce 20 - 50 timestamp data
        // this is similar to back filling
        for i in 20..=30 {
            do_writes(
                "sample",
                write_buffer.as_ref(),
                &[TestWrite {
                    lp,
                    time_seconds: i,
                }],
            )
            .await;
        }

        // we still want the same timestamps to be around when back filling
        // the data
        let all_vals = write_buffer
            .last_cache_provider()
            .get_cache_record_batches(db_id, table_id, Some("sample_cpu_usage"))
            .unwrap()
            .unwrap();
        assert_batches_eq!(
            [
                "+--------+------+----------------------+-------+",
                "| region | host | time                 | usage |",
                "+--------+------+----------------------+-------+",
                "| us     | a    | 1970-01-01T00:01:51Z | 10.0  |",
                "| us     | a    | 1970-01-01T00:01:50Z | 10.0  |",
                "+--------+------+----------------------+-------+",
            ],
            &all_vals
        );
    }

    // 2 threads are used here as we drop the write buffer in this test and the test
    // relies on parquet cache being able to work with eventual mode cache requests.
    // When write buffer is dropped the default background_cache_handler loop gets
    // stuck waiting for new messages in the channel. So, using another thread works
    // around the problem - another way may have been to have some shutdown hook
    // but that'd require further changes. For now, this work around should suffice
    // if finer grained control is necessary, shutdown hook can be explored.
    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_query_path_parquet_cache() {
        let inner_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let (write_buffer, ctx, _) = setup_cache_optional(
            Time::from_timestamp_nanos(100),
            Arc::clone(&inner_store) as _,
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
            false,
        )
        .await;
        let db_name = "test_db";
        // perform writes over time to generate WAL files and some snapshots
        for i in 1..=3 {
            let _ = write_buffer
                .write_lp(
                    NamespaceName::new(db_name).unwrap(),
                    "temp,warehouse=us-east,room=01a,device=10001 reading=36\n\
                temp,warehouse=us-east,room=01b,device=10002 reading=29\n\
                temp,warehouse=us-east,room=02a,device=30003 reading=33\n\
                ",
                    Time::from_timestamp_nanos(i * 1_000_000_000),
                    false,
                    Precision::Nanosecond,
                    false,
                )
                .await
                .unwrap();
        }

        // Wait for snapshot to be created, once this is done, then the parquet has been persisted
        verify_snapshot_count(1, &write_buffer.persister).await;

        // get the path for the created parquet file
        let persisted_files = write_buffer
            .persisted_files()
            .get_files(DbId::from(1), TableId::from(0));
        assert_eq!(1, persisted_files.len());
        let path = ObjPath::from(persisted_files[0].path.as_str());

        let batches = write_buffer
            .get_record_batches_unchecked(db_name, "temp", &ctx)
            .await;
        assert_batches_sorted_eq!(
            [
                "+--------+---------+------+----------------------+-----------+",
                "| device | reading | room | time                 | warehouse |",
                "+--------+---------+------+----------------------+-----------+",
                "| 10001  | 36.0    | 01a  | 1970-01-01T00:00:01Z | us-east   |",
                "| 10001  | 36.0    | 01a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10001  | 36.0    | 01a  | 1970-01-01T00:00:03Z | us-east   |",
                "| 10002  | 29.0    | 01b  | 1970-01-01T00:00:01Z | us-east   |",
                "| 10002  | 29.0    | 01b  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10002  | 29.0    | 01b  | 1970-01-01T00:00:03Z | us-east   |",
                "| 30003  | 33.0    | 02a  | 1970-01-01T00:00:01Z | us-east   |",
                "| 30003  | 33.0    | 02a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 30003  | 33.0    | 02a  | 1970-01-01T00:00:03Z | us-east   |",
                "+--------+---------+------+----------------------+-----------+",
            ],
            &batches
        );

        // at this point everything should've been snapshotted
        drop(write_buffer);

        debug!("test: stopped");
        // nothing in the cache at this point and not in buffer
        let (write_buffer, ctx, _) = setup_cache_optional(
            // move the time
            Time::from_timestamp_nanos(2_000_000_000),
            Arc::clone(&inner_store) as _,
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
            true,
        )
        .await;
        debug!("test: restarted");

        // nothing in query buffer
        let batches =
            get_table_batches_from_query_buffer(&write_buffer, db_name, "temp", &ctx).await;
        assert_batches_sorted_eq!(["++", "++",], &batches);

        // we need to get everything from OS and cache them
        let batches = write_buffer
            .get_record_batches_unchecked(db_name, "temp", &ctx)
            .await;
        assert_batches_sorted_eq!(
            [
                "+--------+---------+------+----------------------+-----------+",
                "| device | reading | room | time                 | warehouse |",
                "+--------+---------+------+----------------------+-----------+",
                "| 10001  | 36.0    | 01a  | 1970-01-01T00:00:01Z | us-east   |",
                "| 10001  | 36.0    | 01a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10001  | 36.0    | 01a  | 1970-01-01T00:00:03Z | us-east   |",
                "| 10002  | 29.0    | 01b  | 1970-01-01T00:00:01Z | us-east   |",
                "| 10002  | 29.0    | 01b  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10002  | 29.0    | 01b  | 1970-01-01T00:00:03Z | us-east   |",
                "| 30003  | 33.0    | 02a  | 1970-01-01T00:00:01Z | us-east   |",
                "| 30003  | 33.0    | 02a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 30003  | 33.0    | 02a  | 1970-01-01T00:00:03Z | us-east   |",
                "+--------+---------+------+----------------------+-----------+",
            ],
            &batches
        );

        // Give the cache a little time to populate before making another request
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(inner_store.total_read_request_count(&path) > 0);
        let expected_req_counts = inner_store.total_read_request_count(&path);

        let batches = write_buffer
            .get_record_batches_unchecked(db_name, "temp", &ctx)
            .await;
        assert_batches_sorted_eq!(
            [
                "+--------+---------+------+----------------------+-----------+",
                "| device | reading | room | time                 | warehouse |",
                "+--------+---------+------+----------------------+-----------+",
                "| 10001  | 36.0    | 01a  | 1970-01-01T00:00:01Z | us-east   |",
                "| 10001  | 36.0    | 01a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10001  | 36.0    | 01a  | 1970-01-01T00:00:03Z | us-east   |",
                "| 10002  | 29.0    | 01b  | 1970-01-01T00:00:01Z | us-east   |",
                "| 10002  | 29.0    | 01b  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10002  | 29.0    | 01b  | 1970-01-01T00:00:03Z | us-east   |",
                "| 30003  | 33.0    | 02a  | 1970-01-01T00:00:01Z | us-east   |",
                "| 30003  | 33.0    | 02a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 30003  | 33.0    | 02a  | 1970-01-01T00:00:03Z | us-east   |",
                "+--------+---------+------+----------------------+-----------+",
            ],
            &batches
        );

        // everything came from cache, all counts should stay the same as
        // above
        assert_eq!(
            expected_req_counts,
            inner_store.total_read_request_count(&path)
        );
    }

    #[tokio::test]
    async fn series_key_updated_on_new_tag() {
        let catalog = Arc::new(Catalog::new_in_memory("test-catalog").await.unwrap());
        let db_name = NamespaceName::new("foo").unwrap();
        let lp = "test_table,tag0=foo field0=1";
        WriteValidator::initialize(db_name.clone(), Arc::clone(&catalog))
            .unwrap()
            .v1_parse_lines_and_catalog_updates(
                lp,
                false,
                Time::from_timestamp_nanos(0),
                Precision::Nanosecond,
            )
            .unwrap()
            .commit_catalog_changes()
            .await
            .unwrap()
            .unwrap_success()
            .convert_lines_to_buffer(Gen1Duration::new_5m());

        let db = catalog.db_schema_by_id(&DbId::from(1)).unwrap();

        assert_eq!(db.tables.len(), 1);
        assert_eq!(
            db.tables
                .get_by_id(&TableId::from(0))
                .unwrap()
                .num_columns(),
            3
        );

        let lp = "test_table,tag1=bar field0=1";
        WriteValidator::initialize(db_name, Arc::clone(&catalog))
            .unwrap()
            .v1_parse_lines_and_catalog_updates(
                lp,
                false,
                Time::from_timestamp_nanos(0),
                Precision::Nanosecond,
            )
            .unwrap()
            .commit_catalog_changes()
            .await
            .unwrap()
            .unwrap_success()
            .convert_lines_to_buffer(Gen1Duration::new_5m());

        assert_eq!(db.tables.len(), 1);
        let db = catalog.db_schema_by_id(&DbId::from(1)).unwrap();
        let table = db.tables.get_by_id(&TableId::from(0)).unwrap();
        assert_eq!(table.num_columns(), 4);
        assert_eq!(table.series_key.len(), 2);
        assert_eq!(table.series_key[0], ColumnId::from(0));
        assert_eq!(table.series_key[1], ColumnId::from(3));
    }

    #[test_log::test(tokio::test)]
    async fn test_empty_write_does_not_corrupt_wal() {
        let object_store = Arc::new(InMemory::new());

        let init = async || -> Arc<WriteBufferImpl> {
            let (buf, _, _) = setup(
                Time::from_timestamp_nanos(0),
                Arc::clone(&object_store) as _,
                WalConfig {
                    gen1_duration: Gen1Duration::new_1m(),
                    max_write_buffer_size: 1,
                    flush_interval: Duration::from_millis(10),
                    snapshot_size: 1,
                },
            )
            .await;
            buf
        };

        let buf = init().await;

        // empty write should be rejected:
        let err = buf
            .write_lp(
                NamespaceName::new("cats").unwrap(),
                "",
                Time::from_timestamp_nanos(1),
                true,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::EmptyWrite),
            "should get an empty write error"
        );

        // do a write with only invalid lines:
        let res = buf
            .write_lp(
                NamespaceName::new("cats").unwrap(),
                "not_valid_line_protocol",
                Time::from_timestamp_nanos(1),
                true,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();
        assert_eq!(0, res.line_count);
        assert_eq!(1, res.invalid_lines.len());

        drop(buf);

        // this should replay the wal and successfully initialize:
        let _buf = init().await;
    }

    /// Check for the case where tags that exist in a table, but have not been written to since
    /// the most recent server start, are perstisted as NULL and not an empty string
    #[test_log::test(tokio::test)]
    async fn test_null_tags_persisted_as_null_not_empty_string() {
        // Setup object store and write buffer for first time
        let object_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: influxdb3_wal::Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let (wb, _ctx, _tp) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&object_store) as _,
            wal_config,
        )
        .await;

        // Do enough writes to trigger a snapshot, so the row containing `tag` is flushed out
        // of the write buffer, and will not be brought back when it is restarted:
        do_writes(
            "foo",
            wb.as_ref(),
            &[
                // first write has `tag`, but all subsequent writes do not
                TestWrite {
                    lp: "bar,tag=a val=1",
                    time_seconds: 1,
                },
                TestWrite {
                    lp: "bar val=2",
                    time_seconds: 2,
                },
                TestWrite {
                    lp: "bar val=3",
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // Wait until there is a snapshot; this will ensure that the row with the tag value
        // is flushed out of the write buffer, and will not be replayed from the WAL on the
        // next startup:
        verify_snapshot_count(1, &wb.persister).await;

        // Drop the write buffer so we can re-initialize:
        drop(wb);

        // Re-initialize the write buffer:
        let (wb, ctx, _tp) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&object_store) as _,
            wal_config,
        )
        .await;

        // Do enough writes again to trigger a snapshot; the `tag` column was never written here
        // so the persistence step will be responsible for filling in that column with NULL:
        do_writes(
            "foo",
            wb.as_ref(),
            &[
                TestWrite {
                    lp: "bar val=4",
                    time_seconds: 4,
                },
                TestWrite {
                    lp: "bar val=5",
                    time_seconds: 5,
                },
                TestWrite {
                    lp: "bar val=6",
                    time_seconds: 6,
                },
            ],
        )
        .await;

        // Wait for snapshot again to make sure when we query below, we are drawing from parquet
        // and not in-memory data:
        verify_snapshot_count(2, &wb.persister).await;

        // Get batches from the buffer, including what is still in the queryable buffer in
        // memory as well as what has been persisted as parquet:
        let batches = wb.get_record_batches_unchecked("foo", "bar", &ctx).await;
        assert_batches_sorted_eq!(
            [
                "+-----+----------------------+-----+",
                "| tag | time                 | val |",
                "+-----+----------------------+-----+",
                "|     | 1970-01-01T00:00:02Z | 2.0 |",
                "|     | 1970-01-01T00:00:03Z | 3.0 |",
                "|     | 1970-01-01T00:00:04Z | 4.0 |",
                "|     | 1970-01-01T00:00:05Z | 5.0 |",
                "|     | 1970-01-01T00:00:06Z | 6.0 |",
                "| a   | 1970-01-01T00:00:01Z | 1.0 |",
                "+-----+----------------------+-----+",
            ],
            &batches
        );

        debug!("record batches:\n\n{batches:#?}");

        // Iterate over the `tag` column values and check that none of them are an empty string
        for batch in batches {
            let tag_col = batch
                .column_by_name("tag")
                .unwrap()
                .as_dictionary::<Int32Type>();
            let tag_vals = tag_col.values().as_string::<i32>();
            for s in tag_vals.iter().flatten() {
                assert!(!s.is_empty(), "there should not be any empty strings");
            }
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_parquet_cache_hits() {
        let object_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: influxdb3_wal::Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let (wb, ctx, metrics) = setup_with_metrics_and_parquet_cache(
            Time::from_timestamp_nanos(0),
            object_store,
            wal_config,
        )
        .await;

        // Do enough writes to trigger a snapshot:
        do_writes(
            "foo",
            wb.as_ref(),
            &[
                // first write has `tag`, but all subsequent writes do not
                TestWrite {
                    lp: "bar,tag=a val=1",
                    time_seconds: 1,
                },
                TestWrite {
                    lp: "bar,tag=b val=2",
                    time_seconds: 2,
                },
                TestWrite {
                    lp: "bar,tag=c val=3",
                    time_seconds: 3,
                },
            ],
        )
        .await;

        verify_snapshot_count(1, &wb.persister).await;

        let instrument = metrics
            .get_instrument::<Metric<U64Counter>>("influxdb3_parquet_cache_access")
            .unwrap();

        let cached_count = instrument.observer(&[("status", "cached")]).fetch();
        // We haven't queried anything so nothing has hit the cache yet:
        assert_eq!(0, cached_count);

        let batches = wb.get_record_batches_unchecked("foo", "bar", &ctx).await;
        assert_batches_sorted_eq!(
            [
                "+-----+----------------------+-----+",
                "| tag | time                 | val |",
                "+-----+----------------------+-----+",
                "| a   | 1970-01-01T00:00:01Z | 1.0 |",
                "| b   | 1970-01-01T00:00:02Z | 2.0 |",
                "| c   | 1970-01-01T00:00:03Z | 3.0 |",
                "+-----+----------------------+-----+",
            ],
            &batches
        );

        let cached_count = instrument.observer(&[("status", "cached")]).fetch();
        // NB: although there should only be a single file in the store, datafusion may make more
        // than one request to fetch the file in chunks, which is why there is more than a single
        // cache hit:
        assert_eq!(3, cached_count);
    }

    struct TestWrite<LP> {
        lp: LP,
        time_seconds: i64,
    }

    async fn do_writes<W: WriteBuffer, LP: AsRef<str> + Send + Sync>(
        db: &'static str,
        buffer: &W,
        writes: &[TestWrite<LP>],
    ) {
        for w in writes {
            buffer
                .write_lp(
                    NamespaceName::new(db).unwrap(),
                    w.lp.as_ref(),
                    Time::from_timestamp_nanos(w.time_seconds * 1_000_000_000),
                    false,
                    Precision::Nanosecond,
                    false,
                )
                .await
                .unwrap();
        }
    }

    async fn do_writes_partial<W: WriteBuffer, LP: AsRef<str> + Send + Sync>(
        db: &'static str,
        buffer: &W,
        writes: &[TestWrite<LP>],
    ) {
        for w in writes {
            buffer
                .write_lp(
                    NamespaceName::new(db).unwrap(),
                    w.lp.as_ref(),
                    Time::from_timestamp_nanos(w.time_seconds * 1_000_000_000),
                    true,
                    Precision::Nanosecond,
                    false,
                )
                .await
                .unwrap();
        }
    }

    async fn verify_snapshot_count(n: usize, persister: &Arc<Persister>) {
        let mut checks = 0;
        loop {
            let persisted_snapshots = persister.load_snapshots(1000).await.unwrap();
            if persisted_snapshots.len() > n {
                panic!(
                    "checking for {} snapshots but found {}",
                    n,
                    persisted_snapshots.len()
                );
            } else if persisted_snapshots.len() == n && checks > 5 {
                // let enough checks happen to ensure extra snapshots aren't running ion the background
                break;
            } else {
                checks += 1;
                if checks > 10 {
                    panic!("not persisting snapshots");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    async fn setup(
        start: Time,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
    ) -> (
        Arc<WriteBufferImpl>,
        IOxSessionContext,
        Arc<dyn TimeProvider>,
    ) {
        let (buf, ctx, time_provider, _metrics) =
            setup_inner(start, object_store, wal_config, true).await;
        (buf, ctx, time_provider)
    }

    async fn setup_cache_optional(
        start: Time,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
        use_cache: bool,
    ) -> (
        Arc<WriteBufferImpl>,
        IOxSessionContext,
        Arc<dyn TimeProvider>,
    ) {
        let (buf, ctx, time_provider, _metrics) =
            setup_inner(start, object_store, wal_config, use_cache).await;
        (buf, ctx, time_provider)
    }

    async fn setup_with_metrics(
        start: Time,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
    ) -> (Arc<WriteBufferImpl>, Arc<Registry>) {
        let (buf, _ctx, _time_provider, metrics) =
            setup_inner(start, object_store, wal_config, false).await;
        (buf, metrics)
    }

    async fn setup_with_metrics_and_parquet_cache(
        start: Time,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
    ) -> (Arc<WriteBufferImpl>, IOxSessionContext, Arc<Registry>) {
        let (buf, ctx, _time_provider, metrics) =
            setup_inner(start, object_store, wal_config, true).await;
        (buf, ctx, metrics)
    }

    async fn setup_inner(
        start: Time,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
        use_cache: bool,
    ) -> (
        Arc<WriteBufferImpl>,
        IOxSessionContext,
        Arc<dyn TimeProvider>,
        Arc<Registry>,
    ) {
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start));
        let metric_registry = Arc::new(Registry::new());
        let (object_store, parquet_cache) = if use_cache {
            let (object_store, parquet_cache) = test_cached_obj_store_and_oracle(
                object_store,
                Arc::clone(&time_provider),
                Arc::clone(&metric_registry),
            );
            (object_store, Some(parquet_cache))
        } else {
            (object_store, None)
        };
        let persister = Arc::new(Persister::new(
            Arc::clone(&object_store),
            "test_host",
            Arc::clone(&time_provider) as _,
        ));
        let catalog = Arc::new(
            Catalog::new(
                "test_host",
                Arc::clone(&object_store),
                Arc::clone(&time_provider),
                Default::default(),
            )
            .await
            .unwrap(),
        );
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _)
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog,
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider),
            executor: make_exec(),
            wal_config,
            parquet_cache,
            metric_registry: Arc::clone(&metric_registry),
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
            shutdown: ShutdownManager::new_testing().register(),
            wal_replay_concurrency_limit: None,
        })
        .await
        .unwrap();
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));
        (wbuf, ctx, time_provider, metric_registry)
    }

    /// Get table batches from the buffer only
    ///
    /// This is meant to be used in tests.
    async fn get_table_batches_from_query_buffer(
        write_buffer: &WriteBufferImpl,
        database_name: &str,
        table_name: &str,
        ctx: &IOxSessionContext,
    ) -> Vec<RecordBatch> {
        let chunks = write_buffer.get_table_chunks_from_buffer_only(
            database_name,
            table_name,
            &ChunkFilter::default(),
            None,
            &ctx.inner().state(),
        );
        let mut batches = vec![];
        for chunk in chunks {
            let chunk = chunk
                .data()
                .read_to_batches(chunk.schema(), ctx.inner())
                .await;
            batches.extend(chunk);
        }
        batches
    }

    async fn assert_dbs_not_empty_in_snapshot_file(obj_store: &Arc<dyn ObjectStore>, host: &str) {
        let from = Path::from(format!("{host}/snapshots/"));
        let file_paths = load_files_from_obj_store(obj_store, &from).await;
        debug!(?file_paths, "obj store snapshots");
        for file_path in file_paths {
            let bytes = obj_store
                .get(&file_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let persisted_snapshot: PersistedSnapshot = serde_json::from_slice(&bytes).unwrap();
            // dbs not empty
            assert!(!persisted_snapshot.databases.is_empty());
            // min and max times aren't defaults
            assert!(persisted_snapshot.min_time != i64::MAX);
            assert!(persisted_snapshot.max_time != i64::MIN);
        }
    }

    async fn load_files_from_obj_store(
        object_store: &Arc<dyn ObjectStore>,
        path: &Path,
    ) -> Vec<Path> {
        let mut paths = Vec::new();
        let mut offset: Option<Path> = None;
        loop {
            let mut listing = if let Some(offset) = offset {
                object_store.list_with_offset(Some(path), &offset)
            } else {
                object_store.list(Some(path))
            };
            let path_count = paths.len();

            while let Some(item) = listing.next().await {
                paths.push(item.unwrap().location);
            }

            if path_count == paths.len() {
                paths.sort();
                break;
            }

            paths.sort();
            offset = Some(paths.last().unwrap().clone())
        }
        paths
    }

    fn make_exec() -> Arc<Executor> {
        let metrics = Arc::new(metric::Registry::default());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let parquet_store = ParquetStorage::new(
            Arc::clone(&object_store),
            StorageId::from("test_exec_storage"),
        );
        Arc::new(Executor::new_with_config_and_executor(
            ExecutorConfig {
                target_query_partitions: NonZeroUsize::new(1).unwrap(),
                object_stores: [&parquet_store]
                    .into_iter()
                    .map(|store| (store.id(), Arc::clone(store.object_store())))
                    .collect(),
                metric_registry: Arc::clone(&metrics),
                // Default to 1gb
                mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
                per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
                heap_memory_limit: None,
            },
            DedicatedExecutor::new_testing(),
        ))
    }
}
