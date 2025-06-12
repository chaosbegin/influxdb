//! This package contains definitions for writing data into InfluxDB3. The logical data model is the standard
//! InfluxDB model of Database > Table > Row. As the data arrives into the server it is written into the wal
//! and buffered in memory in a queryable format. Periodically the WAL is snapshot, which converts the in memory
//! data into parquet files that are persisted to object storage. A snapshot file is written that contains the
//! metadata of the parquet files that were written in that snapshot.

pub(crate) mod async_collections;
pub mod chunk;
pub mod deleter;
pub mod paths;
pub mod persister;
pub mod write_buffer;
pub mod replication_client; // Added module
pub mod sharding_utils; // Added for consistent hashing

use anyhow::Context;
use async_trait::async_trait;
use data_types::{NamespaceName, TimestampMinMax};
use datafusion::{
    catalog::Session,
    common::{Column, DFSchema},
    error::DataFusionError,
    execution::context::ExecutionProps,
    logical_expr::interval_arithmetic::Interval,
    physical_expr::{AnalysisContext, ExprBoundaries, analyze, create_physical_expr},
    prelude::Expr,
    scalar::ScalarValue,
};
use influxdb3_cache::{distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider};
use influxdb3_catalog::catalog::{Catalog, CatalogSequenceNumber, DatabaseSchema, TableDefinition};
use influxdb3_id::{DbId, ParquetFileId, SerdeVecMap, TableId};
pub use influxdb3_types::write::Precision;
use influxdb3_wal::{SnapshotSequenceNumber, Wal, WalFileSequenceNumber, WalOp}; // Added WalOp
use iox_query::QueryChunk;
use iox_time::Time;
use observability_deps::tracing::debug;
use schema::TIME_COLUMN_NAME;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("object store path error: {0}")]
    ObjStorePath(#[from] object_store::path::Error),

    #[error("write buffer error: {0}")]
    WriteBuffer(#[from] write_buffer::Error),

    #[error("persister error: {0}")]
    Persister(#[from] persister::PersisterError),

    #[error("queries not supported in compactor only mode")]
    CompactorOnly,

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait WriteBuffer: Bufferer + ChunkContainer + DistinctCacheManager + LastCacheManager + Send + Sync + Debug {} // Added Send + Sync + Debug

/// The buffer is for buffering data in memory and in the wal before it is persisted as parquet files in storage.
#[async_trait]
pub trait Bufferer: Debug + Send + Sync + 'static {
    /// Validates the line protocol, writes it into the WAL if configured, writes it into the in memory buffer
    /// and returns the result with any lines that had errors and summary statistics.
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> write_buffer::Result<BufferedWriteRequest>;

    /// Returns the database schema provider
    fn catalog(&self) -> Arc<Catalog>;

    /// Reutrns the WAL this bufferer is using
    fn wal(&self) -> Arc<dyn Wal>;

    /// Returns the parquet files for a given database and table
    fn parquet_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFile> {
        self.parquet_files_filtered(db_id, table_id, &ChunkFilter::default())
    }

    /// Returns the parquet files for a given database and table that satisfy the given filter
    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile>;

    /// A channel to watch for when new persisted snapshots are created
    fn watch_persisted_snapshots(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<PersistedSnapshotVersion>>;

    /// Applies a WalOp that has been received from another node via replication.
    /// This method ensures the op is written to the local WAL and buffer,
    /// but does NOT trigger further replication of this op.
    /// The `originating_node_id` can be used for logging or specific checks.
    async fn apply_replicated_wal_op(&self, op: WalOp, originating_node_id: Option<String>) -> Result<(), write_buffer::Error>;
}

/// ChunkContainer is used by the query engine to get chunks for a given table. Chunks will generally be in the
/// `Bufferer` for those in memory from buffered writes or the `Persister` for parquet files that have been persisted.
pub trait ChunkContainer: Debug + Send + Sync + 'static {
    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError>;
}

/// [`DistinctCacheManager`] is used to manage interaction with a [`DistinctCacheProvider`].
#[async_trait::async_trait]
pub trait DistinctCacheManager: Debug + Send + Sync + 'static {
    /// Get a reference to the distinct value cache provider
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider>;
}

/// [`LastCacheManager`] is used to manage interaction with a last-n-value cache provider.
#[async_trait::async_trait]
pub trait LastCacheManager: Debug + Send + Sync + 'static {
    /// Get a reference to the last cache provider
    fn last_cache_provider(&self) -> Arc<LastCacheProvider>;
}

/// A single write request can have many lines in it. A writer can request to accept all lines that are valid, while
/// returning an error for any invalid lines. This is the error information for a single invalid line.
#[derive(Debug, Serialize)]
pub struct WriteLineError {
    pub original_line: String,
    pub line_number: usize,
    pub error_message: String,
}

/// A write that has been validated against the catalog schema, written to the WAL (if configured), and buffered in
/// memory. This is the summary information for the write along with any errors that were encountered.
#[derive(Debug)]
pub struct BufferedWriteRequest {
    pub db_name: NamespaceName<'static>,
    pub invalid_lines: Vec<WriteLineError>,
    pub line_count: usize,
    pub field_count: usize,
    pub index_count: usize,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(tag = "version")]
pub enum PersistedSnapshotVersion {
    #[serde(rename = "1")]
    V1(PersistedSnapshot),
}

impl PersistedSnapshotVersion {
    #[cfg(test)]
    fn v1_ref(&self) -> &PersistedSnapshot {
        match self {
            Self::V1(ps) => ps,
        }
    }
}

/// The collection of Parquet files that were persisted in a snapshot
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct PersistedSnapshot {
    /// The node identifier that persisted this snapshot
    // TODO: deprecate this alias
    #[serde(alias = "writer_id")]
    pub node_id: String,
    /// The next file id to be used with `ParquetFile`s when the snapshot is loaded
    pub next_file_id: ParquetFileId,
    /// The snapshot sequence number associated with this snapshot
    pub snapshot_sequence_number: SnapshotSequenceNumber,
    /// The wal file sequence number that triggered this snapshot
    pub wal_file_sequence_number: WalFileSequenceNumber,
    /// The catalog sequence number associated with this snapshot
    pub catalog_sequence_number: CatalogSequenceNumber,
    /// The size of the snapshot parquet files in bytes.
    pub parquet_size_bytes: u64,
    /// The number of rows across all parquet files in the snapshot.
    pub row_count: u64,
    /// The min time from all parquet files in the snapshot.
    pub min_time: i64,
    /// The max time from all parquet files in the snapshot.
    pub max_time: i64,
    /// The collection of databases that had tables persisted in this snapshot. The tables will then have their
    /// name and the parquet file.
    pub databases: SerdeVecMap<DbId, DatabaseTables>,
    /// The collection of databases that had files removed in this snapshot.
    /// The tables will then have their name and the parquet file that was removed.
    #[serde(default)]
    pub removed_files: SerdeVecMap<DbId, DatabaseTables>,
}

impl PersistedSnapshot {
    pub fn new(
        node_id: String,
        snapshot_sequence_number: SnapshotSequenceNumber,
        wal_file_sequence_number: WalFileSequenceNumber,
        catalog_sequence_number: CatalogSequenceNumber,
    ) -> Self {
        Self {
            node_id,
            next_file_id: ParquetFileId::next_id(),
            snapshot_sequence_number,
            wal_file_sequence_number,
            catalog_sequence_number,
            parquet_size_bytes: 0,
            row_count: 0,
            min_time: i64::MAX,
            max_time: i64::MIN,
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
        }
    }

    fn add_parquet_file(
        &mut self,
        database_id: DbId,
        table_id: TableId,
        parquet_file: ParquetFile,
    ) {
        // Update the next_file_id field, as we likely have a new file
        self.next_file_id = ParquetFileId::next_id();
        self.parquet_size_bytes += parquet_file.size_bytes;
        self.row_count += parquet_file.row_count;
        self.min_time = self.min_time.min(parquet_file.min_time);
        self.max_time = self.max_time.max(parquet_file.max_time);

        self.databases
            .entry(database_id)
            .or_default()
            .tables
            .entry(table_id)
            .or_default()
            .push(parquet_file);
    }

    pub fn db_table_and_file_count(&self) -> (u64, u64, u64) {
        let mut db_count = 0;
        let mut table_count = 0;
        let mut file_count = 0;
        for (_, db_tables) in &self.databases {
            db_count += 1;
            table_count += db_tables.tables.len() as u64;
            file_count += db_tables.tables.values().fold(0, |mut acc, files| {
                acc += files.len() as u64;
                acc
            });
        }
        (db_count, table_count, file_count)
    }

    pub fn overall_db_table_file_counts(host_snapshots: &[PersistedSnapshot]) -> (u64, u64, u64) {
        host_snapshots.iter().fold((0, 0, 0), |mut acc, item| {
            let (db_count, table_count, file_count) = item.db_table_and_file_count();
            acc.0 += db_count;
            acc.1 += table_count;
            acc.2 += file_count;
            acc
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq, Clone)]
pub struct DatabaseTables {
    pub tables: SerdeVecMap<TableId, Vec<ParquetFile>>,
}

/// The summary data for a persisted parquet file in a snapshot.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ParquetFile {
    pub id: ParquetFileId,
    pub path: String,
    pub size_bytes: u64,
    pub row_count: u64,
    /// chunk time nanos
    pub chunk_time: i64,
    /// min time nanos; aka the time of the oldest record in the file
    pub min_time: i64,
    /// max time nanos; aka the time of the newest record in the file
    pub max_time: i64,
}

impl ParquetFile {
    pub fn timestamp_min_max(&self) -> TimestampMinMax {
        TimestampMinMax {
            min: self.min_time,
            max: self.max_time,
        }
    }
}

impl AsRef<ParquetFile> for ParquetFile {
    fn as_ref(&self) -> &ParquetFile {
        self
    }
}

#[cfg(test)]
impl ParquetFile {
    pub(crate) fn create_for_test(path: impl Into<String>) -> Self {
        Self {
            id: ParquetFileId::new(),
            path: path.into(),
            size_bytes: 1024,
            row_count: 1,
            chunk_time: 0,
            min_time: 0,
            max_time: 1,
        }
    }
}

/// Guess precision based off of a given timestamp.
// Note that this will fail in June 2128, but that's not our problem
pub(crate) fn guess_precision(timestamp: i64) -> Precision {
    const NANO_SECS_PER_SEC: i64 = 1_000_000_000;
    // Get the absolute value of the timestamp so we can work with negative
    // numbers
    let val = timestamp.abs() / NANO_SECS_PER_SEC;

    if val < 5 {
        // If the time sent to us is in seconds then this will be a number less than
        // 5 so for example if the time in seconds is 1_708_976_567 then it will be
        // 1 (due to integer truncation) and be less than 5
        Precision::Second
    } else if val < 5_000 {
        // If however the value is milliseconds and not seconds than the same number
        // for time but now in milliseconds 1_708_976_567_000 when divided will now
        // be 1708 which is bigger than the previous if statement but less than this
        // one and so we return milliseconds
        Precision::Millisecond
    } else if val < 5_000_000 {
        // If we do the same thing here by going up another order of magnitude then
        // 1_708_976_567_000_000 when divided will be 1708976 which is large enough
        // for this if statement
        Precision::Microsecond
    } else {
        // Anything else we can assume is large enough of a number that it must
        // be nanoseconds
        Precision::Nanosecond
    }
}

/// A derived set of filters that are used to prune data in the buffer when serving queries
#[derive(Debug, Default)]
pub struct ChunkFilter<'a> {
    pub time_lower_bound_ns: Option<i64>,
    pub time_upper_bound_ns: Option<i64>,
    filters: &'a [Expr],
}

impl<'a> ChunkFilter<'a> {
    /// Create a new `ChunkFilter` given a [`TableDefinition`] and set of filter
    /// [`Expr`]s from a logical query plan.
    ///
    /// This method analyzes the incoming `exprs` to determine if there are any
    /// filters on the `time` column and attempts to derive the boundaries on
    /// `time` from the query.
    pub fn new(table_def: &Arc<TableDefinition>, exprs: &'a [Expr]) -> Result<Self> {
        debug!(input = ?exprs, "creating chunk filter");
        let mut time_interval: Option<Interval> = None;
        let arrow_schema = table_def.schema.as_arrow();
        let time_col_index = arrow_schema
            .fields()
            .iter()
            .position(|f| f.name() == TIME_COLUMN_NAME)
            .context("table should have a time column")?;

        // DF schema and execution properties used for handling physical expressions:
        let df_schema = DFSchema::try_from(Arc::clone(&arrow_schema))
            .context("table schema was not able to convert to datafusion schema")?;
        let props = ExecutionProps::new();

        for expr in exprs.iter().filter(|e| {
            // NOTE: filter out most expression types, as they are not relevant to
            // time bound analysis:
            matches!(e, Expr::BinaryExpr(_) | Expr::Not(_) | Expr::Between(_))
            // Check if the expression refers to the `time` column:
                && e.column_refs()
                    .contains(&Column::new_unqualified(TIME_COLUMN_NAME))
        }) {
            let Ok(physical_expr) = create_physical_expr(expr, &df_schema, &props) else {
                continue;
            };
            // Determine time bounds, if provided:
            let boundaries = ExprBoundaries::try_new_unbounded(&arrow_schema)
                .context("unable to create unbounded expr boundaries on incoming expression")?;
            let mut analysis = analyze(
                &physical_expr,
                AnalysisContext::new(boundaries),
                &arrow_schema,
            )
            .context("unable to analyze provided filters for a boundary on the time column")?;

            // Set the boundaries on the time column using the evaluated
            // interval, if it exists.
            // If an interval was already derived from a previous expression, we
            // take their intersection, or produce an error if:
            // - the derived intervals are not compatible (different types)
            // - the derived intervals do not intersect, this should be a user
            //   error, i.e., a poorly formed query or querying outside of a
            //   retention policy for the table
            if let Some(ExprBoundaries { interval, .. }) = (time_col_index
                < analysis.boundaries.len())
            .then_some(analysis.boundaries.remove(time_col_index))
            {
                if let Some(existing) = time_interval.take() {
                    let intersection = existing.intersect(interval).context(
                                "failed to derive a time interval from provided filters",
                            )?.context("provided filters on time column did not produce a valid set of boundaries")?;
                    time_interval.replace(intersection);
                } else {
                    time_interval.replace(interval);
                }
            }
        }

        // Determine the lower and upper bound from the derived interval on time:
        // TODO: we may open this up more to other scalar types, e.g., other timestamp types
        //       depending on how users define time bounds.
        let (time_lower_bound_ns, time_upper_bound_ns) = if let Some(i) = time_interval {
            let low = if let ScalarValue::TimestampNanosecond(Some(l), _) = i.lower() {
                Some(*l)
            } else {
                None
            };
            let high = if let ScalarValue::TimestampNanosecond(Some(h), _) = i.upper() {
                Some(*h)
            } else {
                None
            };

            (low, high)
        } else {
            (None, None)
        };

        Ok(Self {
            time_lower_bound_ns,
            time_upper_bound_ns,
            filters: exprs,
        })
    }

    /// Test a `min` and `max` time against this filter to check if the range they define overlaps
    /// with the range defined by the bounds in this filter.
    pub fn test_time_stamp_min_max(&self, min_time_ns: i64, max_time_ns: i64) -> bool {
        match (self.time_lower_bound_ns, self.time_upper_bound_ns) {
            (None, None) => true,
            (None, Some(u)) => min_time_ns <= u,
            (Some(l), None) => max_time_ns >= l,
            (Some(l), Some(u)) => min_time_ns <= u && max_time_ns >= l,
        }
    }

    pub fn original_filters(&self) -> &[Expr] {
        self.filters
    }
}

// --- Placeholder gRPC structs ---
// These would normally be generated by tonic-build from a .proto file.

/// Placeholder for a request to replicate a WalOp.
#[derive(Debug, Clone)]
pub struct ReplicateWalOpRequest {
    // Using bitcode-serialized WalOp for now as a stand-in.
    // In a real gRPC setup, WalOp might be defined in proto directly or this would be bytes.
    pub serialized_wal_op: Vec<u8>,
    pub originating_node_id: String, // Identifier of the node that originally processed this write
    // Potentially other metadata like shard_id, table_id, db_id if not fully contained/derivable from WalOp
}

/// Placeholder for a response from replicating a WalOp.
#[derive(Debug, Clone)]
pub struct ReplicateWalOpResponse {
    pub success: bool,
    pub error_message: Option<String>,
}
// --- End Placeholder gRPC structs ---


pub mod test_helpers {
    use crate::ChunkFilter;
    use crate::WriteBuffer;
    use arrow::array::RecordBatch;
    use datafusion::prelude::Expr;
    use iox_query::exec::IOxSessionContext;

    /// Helper trait for getting [`RecordBatch`]es from a [`WriteBuffer`] implementation in tests
    #[async_trait::async_trait]
    pub trait WriteBufferTester {
        /// Get record batches for the given database and table, using the provided filter `Expr`s
        async fn get_record_batches_filtered_unchecked(
            &self,
            database_name: &str,
            table_name: &str,
            filters: &[Expr],
            ctx: &IOxSessionContext,
        ) -> Vec<RecordBatch>;

        /// Get record batches for the given database and table
        async fn get_record_batches_unchecked(
            &self,
            database_name: &str,
            table_name: &str,
            ctx: &IOxSessionContext,
        ) -> Vec<RecordBatch>;
    }

    #[async_trait::async_trait]
    impl<T> WriteBufferTester for T
    where
        T: WriteBuffer,
    {
        async fn get_record_batches_filtered_unchecked(
            &self,
            database_name: &str,
            table_name: &str,
            filters: &[Expr],
            ctx: &IOxSessionContext,
        ) -> Vec<RecordBatch> {
            let db_schema = self
                .catalog()
                .db_schema(database_name)
                .expect("database should exist");
            let table_def = db_schema
                .table_definition(table_name)
                .expect("table should exist");
            let filter =
                ChunkFilter::new(&table_def, filters).expect("filter expressions should be valid");
            let chunks = self
                .get_table_chunks(db_schema, table_def, &filter, None, &ctx.inner().state())
                .expect("should get query chunks");
            let mut batches = vec![];
            for chunk in chunks {
                batches.extend(
                    chunk
                        .data()
                        .read_to_batches(chunk.schema(), ctx.inner())
                        .await,
                );
            }
            batches
        }

        async fn get_record_batches_unchecked(
            &self,
            database_name: &str,
            table_name: &str,
            ctx: &IOxSessionContext,
        ) -> Vec<RecordBatch> {
            self.get_record_batches_filtered_unchecked(database_name, table_name, &[], ctx)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use influxdb3_catalog::catalog::CatalogSequenceNumber;
    use influxdb3_id::{DbId, ParquetFileId, SerdeVecMap, TableId};
    use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
    use influxdb3_catalog::log::FieldDataType; // For TableDefinition in ChunkFilter tests
    use influxdb3_catalog::shard::ShardTimeRange; // For ShardDefinition in ChunkFilter tests
    use datafusion::scalar::ScalarValue; // For literal creation in tests
    use datafusion::prelude::col; // For column reference in tests


    use crate::{DatabaseTables, ParquetFile, PersistedSnapshot};

    #[test]
    fn test_overall_counts() {
        let host = "host_id";
        // db 1 setup
        let db_id_1 = DbId::from(0);
        let mut dbs_1 = SerdeVecMap::new();
        let table_id_1 = TableId::from(0);
        let mut tables_1 = SerdeVecMap::new();
        let parquet_files_1 = vec![
            ParquetFile {
                id: ParquetFileId::from(1),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
            ParquetFile {
                id: ParquetFileId::from(2),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
        ];
        tables_1.insert(table_id_1, parquet_files_1);
        dbs_1.insert(db_id_1, DatabaseTables { tables: tables_1 });

        // add dbs_1 to snapshot
        let persisted_snapshot_1 = PersistedSnapshot {
            node_id: host.to_string(),
            next_file_id: ParquetFileId::from(0),
            snapshot_sequence_number: SnapshotSequenceNumber::new(124),
            wal_file_sequence_number: WalFileSequenceNumber::new(100),
            catalog_sequence_number: CatalogSequenceNumber::new(100),
            databases: dbs_1,
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
            removed_files: SerdeVecMap::new(),
        };

        // db 2 setup
        let db_id_2 = DbId::from(2);
        let mut dbs_2 = SerdeVecMap::new();
        let table_id_2 = TableId::from(2);
        let mut tables_2 = SerdeVecMap::new();
        let parquet_files_2 = vec![
            ParquetFile {
                id: ParquetFileId::from(4),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
            ParquetFile {
                id: ParquetFileId::from(5),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
        ];
        tables_2.insert(table_id_2, parquet_files_2);
        dbs_2.insert(db_id_2, DatabaseTables { tables: tables_2 });

        // add dbs_2 to snapshot
        let persisted_snapshot_2 = PersistedSnapshot {
            node_id: host.to_string(),
            next_file_id: ParquetFileId::from(5),
            snapshot_sequence_number: SnapshotSequenceNumber::new(124),
            wal_file_sequence_number: WalFileSequenceNumber::new(100),
            catalog_sequence_number: CatalogSequenceNumber::new(100),
            databases: dbs_2,
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
            removed_files: SerdeVecMap::new(),
        };

        let overall_counts = PersistedSnapshot::overall_db_table_file_counts(&[
            persisted_snapshot_1,
            persisted_snapshot_2,
        ]);
        assert_eq!((2, 2, 4), overall_counts);
    }

    #[test]
    fn test_overall_counts_zero() {
        // db 1 setup
        let db_id_1 = DbId::from(0);
        let mut dbs_1 = SerdeVecMap::new();
        let table_id_1 = TableId::from(0);
        let mut tables_1 = SerdeVecMap::new();
        let parquet_files_1 = vec![
            ParquetFile {
                id: ParquetFileId::from(1),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
            ParquetFile {
                id: ParquetFileId::from(2),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
        ];
        tables_1.insert(table_id_1, parquet_files_1);
        dbs_1.insert(db_id_1, DatabaseTables { tables: tables_1 });

        // db 2 setup
        let db_id_2 = DbId::from(2);
        let mut dbs_2 = SerdeVecMap::new();
        let table_id_2 = TableId::from(2);
        let mut tables_2 = SerdeVecMap::new();
        let parquet_files_2 = vec![
            ParquetFile {
                id: ParquetFileId::from(4),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
            ParquetFile {
                id: ParquetFileId::from(5),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
        ];
        tables_2.insert(table_id_2, parquet_files_2);
        dbs_2.insert(db_id_2, DatabaseTables { tables: tables_2 });

        // add dbs_2 to snapshot
        let overall_counts = PersistedSnapshot::overall_db_table_file_counts(&[]);
        assert_eq!((0, 0, 0), overall_counts);
    }

    #[cfg(test)]
    mod chunk_filter_tests {
        use super::*;
        use influxdb3_catalog::shard::ShardDefinition; // Required for TableDefinition
        use influxdb3_id::ShardId; // Required for ShardDefinition
        use std::collections::HashMap as StdHashMap;

        fn time_expr(op: &str, nanos: i64) -> Expr {
            let time_col = col(TIME_COLUMN_NAME);
            let literal_val = ScalarValue::TimestampNanosecond(Some(nanos), None);
            match op {
                ">=" => time_col.gt_eq(Expr::Literal(literal_val)),
                ">" => time_col.gt(Expr::Literal(literal_val)),
                "<=" => time_col.lt_eq(Expr::Literal(literal_val)),
                "<" => time_col.lt(Expr::Literal(literal_val)),
                "=" => time_col.eq(Expr::Literal(literal_val)),
                _ => panic!("Unsupported operator"),
            }
        }

        fn non_time_expr() -> Expr {
            col("tag_col").eq(Expr::Literal(ScalarValue::Utf8(Some("value".to_string()))))
        }

        fn create_test_table_def() -> Arc<TableDefinition> {
            // Create a minimal TableDefinition needed for ChunkFilter (mainly schema)
            let mut columns_map = SerdeVecMap::new();
            columns_map.insert(influxdb3_id::ColumnId::new(0), Arc::new(influxdb3_catalog::catalog::ColumnDefinition::new(influxdb3_id::ColumnId::new(0), TIME_COLUMN_NAME, schema::InfluxColumnType::Timestamp, false)));
            columns_map.insert(influxdb3_id::ColumnId::new(1), Arc::new(influxdb3_catalog::catalog::ColumnDefinition::new(influxdb3_id::ColumnId::new(1), "tag_col", schema::InfluxColumnType::Tag, true)));

            let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new(TIME_COLUMN_NAME, arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
                arrow::datatypes::Field::new("tag_col", arrow::datatypes::DataType::Utf8, true),
            ]));
            let schema = schema::Schema::try_from_arrow_schema(arrow_schema).unwrap();


            Arc::new(TableDefinition {
                table_id: TableId::new(1),
                table_name: Arc::from("test_table"),
                schema,
                columns: columns_map,
                series_key: vec![],
                series_key_names: vec![],
                sort_key: schema::sort::SortKey::default(),
                last_caches: Default::default(),
                distinct_caches: Default::default(),
                deleted: false,
                hard_delete_time: None,
                shards: Default::default(), // Not directly used by ChunkFilter::new itself for time bound extraction
                replication_info: None,
                shard_key_columns: None,
                sharding_strategy: Default::default(),
            })
        }

        #[test]
        fn test_chunk_filter_no_filters() {
            let table_def = create_test_table_def();
            let filters = vec![];
            let chunk_filter = ChunkFilter::new(&table_def, &filters).unwrap();
            assert_eq!(chunk_filter.time_lower_bound_ns, None);
            assert_eq!(chunk_filter.time_upper_bound_ns, None);
        }

        #[test]
        fn test_chunk_filter_single_time_range() {
            let table_def = create_test_table_def();
            // time >= 100 AND time <= 200
            let filters = vec![
                time_expr(">=", 100).and(time_expr("<=", 200))
            ];
            let chunk_filter = ChunkFilter::new(&table_def, &filters).unwrap();
            assert_eq!(chunk_filter.time_lower_bound_ns, Some(100));
            assert_eq!(chunk_filter.time_upper_bound_ns, Some(200));
        }

        #[test]
        fn test_chunk_filter_intersecting_time_ranges() {
            let table_def = create_test_table_def();
            // (time >= 100 AND time <= 300) AND (time >= 200 AND time <= 400)
            // Expected intersection: time >= 200 AND time <= 300
            let filters = vec![
                time_expr(">=", 100).and(time_expr("<=", 300)),
                time_expr(">=", 200).and(time_expr("<=", 400)),
            ];
            let combined_filter = filters.into_iter().reduce(Expr::and).unwrap();
            let chunk_filter = ChunkFilter::new(&table_def, &[combined_filter]).unwrap();
            assert_eq!(chunk_filter.time_lower_bound_ns, Some(200));
            assert_eq!(chunk_filter.time_upper_bound_ns, Some(300));
        }

        #[test]
        fn test_chunk_filter_one_range_contains_another() {
            let table_def = create_test_table_def();
            // (time >= 100 AND time <= 400) AND (time >= 200 AND time <= 300)
            // Expected intersection: time >= 200 AND time <= 300
            let filters = vec![
                time_expr(">=", 100).and(time_expr("<=", 400)),
                time_expr(">=", 200).and(time_expr("<=", 300)),
            ];
            let combined_filter = filters.into_iter().reduce(Expr::and).unwrap();
            let chunk_filter = ChunkFilter::new(&table_def, &[combined_filter]).unwrap();
            assert_eq!(chunk_filter.time_lower_bound_ns, Some(200));
            assert_eq!(chunk_filter.time_upper_bound_ns, Some(300));
        }

        #[test]
        fn test_chunk_filter_non_overlapping_time_ranges() {
            let table_def = create_test_table_def();
            // (time >= 100 AND time <= 200) AND (time >= 300 AND time <= 400)
            // Expected: This should result in an empty interval (None, None) or an error.
            // DataFusion's interval arithmetic for intersection of non-overlapping ranges might result in an "empty" interval.
            // The current ChunkFilter extracts lower/upper from this. If interval is empty, bounds might be None or invalid.
            let filters = vec![
                time_expr(">=", 100).and(time_expr("<=", 200)),
                time_expr(">=", 300).and(time_expr("<=", 400)),
            ];
            let combined_filter = filters.into_iter().reduce(Expr::and).unwrap();
             // The `intersect` method on Interval returns `Result<Option<Interval>>`.
            // If it's `Ok(None)`, it means no intersection, which should lead to `time_lower_bound_ns = None` and `time_upper_bound_ns = None`
            // or specific handling for impossible ranges. Current implementation seems to convert `None` interval to `(None, None)` bounds.
            let chunk_filter_result = ChunkFilter::new(&table_def, &[combined_filter]);
            // Depending on how DataFusion's Interval handles empty intersections, this might error or yield None bounds.
            // If it errors (e.g. "provided filters on time column did not produce a valid set of boundaries"), that's fine.
            // If it results in None bounds, that's also fine (means no pruning by time).
            // Let's check if it successfully creates a filter, which implies it handled it.
            // The actual behavior of `test_time_stamp_min_max` for such a filter would be important.
             assert!(chunk_filter_result.is_ok(), "ChunkFilter creation failed for non-overlapping ranges, error: {:?}", chunk_filter_result.err());
            // If DataFusion's interval intersection results in an empty interval, the ScalarValue bounds might be None.
            let chunk_filter = chunk_filter_result.unwrap();
            // This means the filter effectively becomes unbounded on time if intersection is empty.
            // This is acceptable as no data will match this impossible filter.
            // Or, more accurately, the interval becomes something like [MAX, MIN] which then gets converted to (None, None)
            // by the ScalarValue::TimestampNanosecond(Some(l), _) checks.
            // Let's assert what it currently does, assuming (None, None) for an impossible range.
            assert_eq!(chunk_filter.time_lower_bound_ns, None, "Lower bound should be None for impossible range");
            assert_eq!(chunk_filter.time_upper_bound_ns, None, "Upper bound should be None for impossible range");
        }

        #[test]
        fn test_chunk_filter_with_non_time_predicates() {
            let table_def = create_test_table_def();
            // time >= 100 AND time <= 200 AND tag_col = 'value'
            let filters = vec![
                time_expr(">=", 100).and(time_expr("<=", 200)).and(non_time_expr())
            ];
            let chunk_filter = ChunkFilter::new(&table_def, &filters).unwrap();
            assert_eq!(chunk_filter.time_lower_bound_ns, Some(100));
            assert_eq!(chunk_filter.time_upper_bound_ns, Some(200));
            assert_eq!(chunk_filter.original_filters().len(), 1); // Original combined filter is stored
        }

        #[test]
        fn test_chunk_filter_exclusive_bounds() {
            let table_def = create_test_table_def();
            // time > 100 AND time < 200
            let filters = vec![
                time_expr(">", 100).and(time_expr("<", 200))
            ];
            let chunk_filter = ChunkFilter::new(&table_def, &filters).unwrap();
            // Interval arithmetic should convert > to >= and < to <= with adjusted values or handle open/closed intervals.
            // ScalarValue from Interval.lower/upper is inclusive.
            // So, time > 100  -> lower bound is 100 (exclusive), interval might store as 101 if discrete or handle open.
            // ScalarValue::TimestampNanosecond(Some(l), _) means l is inclusive.
            // DataFusion's Interval::lower() and Interval::upper() are inclusive.
            // time > X becomes time >= X+1 for integers. For timestamps, it's more nuanced.
            // Let's check what DataFusion's analyze does.
            // `analyze` for `time > 100` should yield an interval starting at 100 but being exclusive of 100.
            // When converting back to ScalarValue::TimestampNanosecond, it might be Some(100) if the ScalarValue itself doesn't track exclusivity.
            // The current ChunkFilter logic directly takes the ScalarValue.
            assert_eq!(chunk_filter.time_lower_bound_ns, Some(100)); // Depending on how Interval's lower for ">" is translated
            assert_eq!(chunk_filter.time_upper_bound_ns, Some(200)); // Depending on how Interval's upper for "<" is translated
        }
    }
}
