use std::sync::Arc;

use crate::{Precision, WriteLineError, write_buffer::Result};
use data_types::{NamespaceName, Timestamp};
use indexmap::IndexMap;
use influxdb3_catalog::catalog::{
    Catalog, CatalogSequenceNumber, DatabaseCatalogTransaction, Prompt,
};

use influxdb_line_protocol::{ParsedLine, parse_lines};
use influxdb3_id::{DbId, TableId};
use influxdb3_types::http::FieldDataType;
use influxdb3_wal::{Field, FieldData, Gen1Duration, Row, TableChunks, WriteBatch};
use iox_time::Time;
use observability_deps::tracing::trace;
use schema::TIME_COLUMN_NAME;

use super::Error;

/// Type state for the [`WriteValidator`] after it has been initialized
/// with the catalog.
#[derive(Debug)]
pub struct Initialized {
    catalog: Arc<Catalog>,
    txn: DatabaseCatalogTransaction,
}

/// Type state for the [`WriteValidator`] after it has parsed v1 or v3
/// line protocol.
#[derive(Debug)]
pub struct LinesParsed {
    catalog: Arc<Catalog>,
    txn: DatabaseCatalogTransaction,
    lines: Vec<QualifiedLine>,
    bytes: u64,
    errors: Vec<WriteLineError>,
}

impl LinesParsed {
    /// Convert this set of parsed and qualified lines into a set of rows
    ///
    /// This is useful for testing when you need to use the write validator to parse line protocol
    /// and get the raw row data for the WAL.
    pub fn to_rows(self) -> Vec<Row> {
        self.lines.into_iter().map(|line| line.row).collect()
    }

    pub fn txn(&self) -> &DatabaseCatalogTransaction {
        &self.txn
    }
}

/// Type state for [`WriteValidator`] after any catalog changes have been committed successfully
/// to the object store.
#[derive(Debug)]
pub struct CatalogChangesCommitted {
    pub(crate) catalog_sequence: CatalogSequenceNumber, // Made fields pub(crate) for WriteBufferImpl
    pub(crate) db_id: DbId,
    pub(crate) db_name: Arc<str>,
    pub(crate) lines: Vec<QualifiedLine>,
    pub(crate) bytes: u64,
    pub(crate) errors: Vec<WriteLineError>,
    pub(crate) shard_id: Option<ShardId>,
}

use influxdb3_catalog::shard::ShardId;
use influxdb3_catalog::shard::ShardId;
use influxdb3_id::NodeId;
use murmur3::murmur3_32; // For hashing
use std::io::Cursor; // For hashing writable
use std::collections::BTreeMap; // For sorted shard key string

impl CatalogChangesCommitted {
    /// Convert this set of parsed and qualified lines into a set of rows
    ///
    /// This is useful for testing when you need to use the write validator to parse line protocol
    /// and get the raw row data for the WAL.
    pub fn to_rows(self) -> Vec<Row> {
        self.lines.into_iter().map(|line| line.row).collect()
    }
}


/// A state machine for validating v1 or v3 line protocol and updating
/// the [`Catalog`] with new tables or schema changes.
#[derive(Debug)]
pub struct WriteValidator<State> {
    state: State,
}

// Generic impl block for WriteValidator<T>
impl<T> WriteValidator<T> {
    /// Convert this into the inner state.
    pub fn into_inner(self) -> T {
        self.state
    }

    /// Access the inner state.
    pub fn inner(&self) -> &T {
        &self.state
    }
}

// Specific impl for WriteValidator<CatalogChangesCommitted>
impl WriteValidator<CatalogChangesCommitted> {
    /// Allows reconstruction from state, useful if state needs to be modified externally
    /// (e.g., injecting shard_id determined by WriteBufferImpl).
    pub fn from(state: CatalogChangesCommitted) -> Self {
        Self { state }
    }
}

/// Creates a WriteBatch from a slice of QualifiedLines that are assumed to be
/// for the same target time shard and hash partition.
///
/// This function is responsible for the final assembly of rows into the
/// WriteBatch structure. It assumes that individual lines have already been
/// schema-validated during the `validate_and_qualify_v1_line` stage.
/// The primary role is data aggregation into TableChunks.
/// Errors returned would typically be for inconsistencies found during batch assembly,
/// if any, rather than individual line parsing/validation errors which are handled earlier.
pub(crate) fn lines_to_write_batch(
    db_id: DbId,
    db_name: Arc<str>,
    catalog_sequence: CatalogSequenceNumber,
    lines: &[QualifiedLine], // Slice of qualified lines for this specific partition
    time_shard_id: ShardId, // The time-based shard_id for this batch
    hash_partition_index: Option<u32>, // The hash_partition_index for this batch
    gen1_duration: Gen1Duration,
) -> Result<WriteBatch, Vec<WriteLineError>> { // Returning Vec<WriteLineError> for now, though less likely to be used
    let mut table_chunks_map: IndexMap<TableId, TableChunks> = IndexMap::new();

    if lines.is_empty() {
        // Return minimal WriteBatch if no lines.
        return Ok(WriteBatch::new(
            catalog_sequence.get(),
            db_id,
            db_name,
            table_chunks_map, // empty
            Some(time_shard_id),
            hash_partition_index,
        ));
    }

    for line in lines.iter() {
        // Logic adapted from the old `convert_qualified_line`
        let chunk_time = gen1_duration.chunk_time_for_timestamp(Timestamp::new(line.row.time));
        let table_entry = table_chunks_map.entry(line.table_id).or_default();
        table_entry.push_row(chunk_time, line.row.clone()); // Clone row as it's borrowed
    }

    // Currently, this function is not expected to produce WriteLineErrors if individual lines
    // were already validated. If batch-level validation that could produce such errors
    // is added here in the future, those errors should be collected and returned in Err().
    // For now, assume success if lines are provided and processed.
    Ok(WriteBatch::new(
        catalog_sequence.get(),
        db_id,
        db_name,
        table_chunks_map,
        Some(time_shard_id),
        hash_partition_index,
    ))
}


impl WriteValidator<Initialized> {
    /// Initialize the [`WriteValidator`] by starting a catalog transaction on the given database
    /// with name `db_name`. This initializes the database if it does not already exist.
    pub fn initialize(db_name: NamespaceName<'static>, catalog: Arc<Catalog>) -> Result<Self> {
        let txn = catalog.begin(db_name.as_str())?;
        trace!(transaction = ?txn, "initialize write validator");
        Ok(WriteValidator {
            state: Initialized { catalog, txn },
        })
    }

    /// Parse the incoming lines of line protocol using the v1 parser and update the transaction
    /// to the catalog if:
    ///
    /// * A new table is being added
    /// * New fields or tags are being added to an existing table
    ///
    /// # Implementation Note
    ///
    /// This does not apply the changes to the catalog, it only modifies the database copy that
    /// is held on the catalog transaction.
    pub fn v1_parse_lines_and_catalog_updates(
        mut self,
        lp: &str,
        accept_partial: bool,
        ingest_time: Time,
        precision: Precision,
    ) -> Result<WriteValidator<LinesParsed>> {
        let mut errors = vec![];
        let mut lp_lines = lp.lines();
        let mut lines = vec![];
        let mut bytes = 0;

        for (line_idx, maybe_line) in parse_lines(lp).enumerate() {
            let qualified_line = match maybe_line
                .map_err(|e| WriteLineError {
                    original_line: lp_lines.next().unwrap().to_string(),
                    line_number: line_idx + 1,
                    error_message: e.to_string(),
                })
                .and_then(|l| {
                    let raw_line = lp_lines.next().unwrap();
                    validate_and_qualify_v1_line(
                        &mut self.state.txn,
                        line_idx,
                        l,
                        ingest_time,
                        precision,
                        raw_line.to_string(), // Pass original line string
                    )
                    .inspect(|_| bytes += raw_line.len() as u64)
                }) {
                Ok(qualified_line) => qualified_line,
                Err(e) => {
                    if !accept_partial {
                        return Err(Error::ParseError(e));
                    } else {
                        errors.push(e);
                    }
                    continue;
                }
            };
            lines.push(qualified_line);
        }

        Ok(WriteValidator {
            state: LinesParsed {
                catalog: self.state.catalog,
                txn: self.state.txn,
                lines,
                errors,
                bytes,
            },
        })
    }
}

fn validate_and_qualify_v1_line(
    txn: &mut DatabaseCatalogTransaction,
    line_number: usize,
    line: ParsedLine<'_>,
    ingest_time: Time,
    precision: Precision,
    original_lp_string: String, // Added parameter
) -> Result<QualifiedLine, WriteLineError> {
    let table_name = line.series.measurement.as_str();
    let mut fields = Vec::with_capacity(line.column_count());
    let mut index_count = 0;
    let mut field_count = 0;
    let table_def = txn
        .table_or_create(table_name)
        .map_err(|error| WriteLineError {
            original_line: line.to_string(),
            line_number: line_number + 1,
            error_message: error.to_string(),
        })?;

    if let Some(tag_set) = &line.series.tag_set {
        for (tag_key, tag_val) in tag_set {
            let col_id = txn
                .column_or_create(table_name, tag_key.as_str(), FieldDataType::Tag)
                .map_err(|error| WriteLineError {
                    original_line: line.to_string(),
                    line_number: line_number + 1,
                    error_message: error.to_string(),
                })?;
            fields.push(Field::new(col_id, FieldData::Tag(tag_val.to_string())));
            index_count += 1;
        }
    }

    for (field_name, field_val) in line.field_set.iter() {
        let col_id = txn
            .column_or_create(table_name, field_name, field_val.into())
            .map_err(|error| WriteLineError {
                original_line: line.to_string(),
                line_number: line_number + 1,
                error_message: error.to_string(),
            })?;
        fields.push(Field::new(col_id, field_val));
        field_count += 1;
    }

    let time_col_id = txn
        .column_or_create(table_name, TIME_COLUMN_NAME, FieldDataType::Timestamp)
        .map_err(|error| WriteLineError {
            original_line: line.to_string(),
            line_number: line_number + 1,
            error_message: error.to_string(),
        })?;
    let timestamp_ns = line
        .timestamp
        .map(|ts| apply_precision_to_timestamp(precision, ts))
        .unwrap_or(ingest_time.timestamp_nanos());
    fields.push(Field::new(time_col_id, FieldData::Timestamp(timestamp_ns)));

    // --- Hashing Logic ---
    // Get shard keys from table_def
    let shard_keys_from_def = &table_def.shard_keys;
    let num_partitions = table_def.num_hash_partitions;
    let mut calculated_hash_partition_index: Option<u32> = None;

    if num_partitions > 1 && !shard_keys_from_def.is_empty() {
        // Collect tags from the current line
        let mut line_tags = BTreeMap::new(); // Use BTreeMap for sorted keys
        if let Some(tag_set) = &line.series.tag_set {
            for (k, v) in tag_set.iter() {
                line_tags.insert(k.as_str().to_string(), v.as_str().to_string());
            }
        }

        let mut shard_key_parts = Vec::with_capacity(shard_keys_from_def.len());
        for key_name in shard_keys_from_def {
            if let Some(value) = line_tags.get(key_name) {
                shard_key_parts.push(format!("{}={}", key_name, value));
            } else {
                // A defined shard key is missing from the line's tags
                return Err(WriteLineError {
                    original_line: line.to_string(),
                    line_number: line_number + 1,
                    error_message: format!("Missing required shard key tag: {}", key_name),
                });
            }
        }
        // Canonical string: already sorted by BTreeMap iteration if shard_keys_from_def was sorted,
        // or sort shard_key_parts now if shard_keys_from_def order isn't guaranteed/canonical.
        // Assuming shard_keys_from_def is the canonical order.
        let canonical_shard_key_string = shard_key_parts.join(",");

        let hash_value = murmur3_32(&mut Cursor::new(canonical_shard_key_string.as_bytes()), 0)
            .map_err(|e| WriteLineError {
                original_line: line.to_string(),
                line_number: line_number + 1,
                error_message: format!("Failed to compute shard key hash: {}", e),
            })?;

        calculated_hash_partition_index = Some(hash_value % num_partitions);
    }
    // --- End Hashing Logic ---

    Ok(QualifiedLine {
        table_id: table_def.table_id,
        row: Row {
            time: timestamp_ns,
            fields,
        },
        index_count,
        field_count,
        hash_partition_index: calculated_hash_partition_index,
        target_node_id_for_hash_partition: None, // To be filled in later by WriteBufferImpl
        original_lp_string, // Store it
    })
}

impl WriteValidator<LinesParsed> {
    pub async fn commit_catalog_changes(
        self,
    ) -> Result<Prompt<WriteValidator<CatalogChangesCommitted>>> {
        let db_schema = self.state.txn.db_schema();
        let db_id = db_schema.id;
        let db_name = Arc::clone(&db_schema.name);
        let determined_shard_id: Option<ShardId> = None; // Placeholder

        match self.state.catalog.commit(self.state.txn).await? {
            Prompt::Success(catalog_sequence) => Ok(Prompt::Success(WriteValidator {
                state: CatalogChangesCommitted {
                    catalog_sequence,
                    db_id,
                    db_name,
                    lines: self.state.lines,
                    bytes: self.state.bytes,
                    errors: self.state.errors,
                    shard_id: determined_shard_id,
                },
            })),
            Prompt::Retry(_) => Ok(Prompt::Retry(())),
        }
    }

    pub fn ignore_catalog_changes_and_convert_lines_to_buffer(
        self,
        gen1_duration: Gen1Duration,
        shard_id: Option<ShardId>,
    ) -> ValidatedLines {
        let db_schema = self.state.txn.db_schema();
        let ignored = WriteValidator {
            state: CatalogChangesCommitted {
                catalog_sequence: self.state.txn.sequence_number(),
                db_id: db_schema.id,
                db_name: Arc::clone(&db_schema.name),
                lines: self.state.lines,
                bytes: self.state.bytes,
                errors: self.state.errors,
                shard_id,
            },
        };
        ignored.convert_lines_to_buffer(gen1_duration)
    }
}

#[derive(Debug)]
pub struct ValidatedLines {
    pub(crate) line_count: usize,
    pub(crate) valid_bytes_count: u64,
    pub(crate) field_count: usize,
    pub(crate) index_count: usize,
    pub errors: Vec<WriteLineError>,
    pub valid_data: WriteBatch,
    pub shard_id: Option<ShardId>,
}

impl From<ValidatedLines> for WriteBatch {
    fn from(value: ValidatedLines) -> Self {
        value.valid_data
    }
}

impl WriteValidator<CatalogChangesCommitted> {
    #[deprecated(note = "This function is being phased out. Logic is moving to WriteBufferImpl::write_lp and lines_to_write_batch.")]
    pub fn convert_lines_to_buffer(self, _gen1_duration: Gen1Duration) -> ValidatedLines {
        // The new logic in `WriteBufferImpl::write_lp` will group lines and call
        // `lines_to_write_batch` for each local partition. This function, which
        // processed all lines into a single batch, is no longer the primary path.
        //
        // For now, to ensure compilability if any old paths still call this,
        // return a default/empty ValidatedLines. Ideally, all callers are updated.
        panic!("convert_lines_to_buffer is deprecated and should not be called in the new write path.");
        // ValidatedLines {
        //     line_count: 0,
        //     valid_bytes_count: 0,
        //     field_count: 0,
        //     index_count: 0,
        //     errors: self.state.errors, // Preserve errors if any were collected
        //     valid_data: WriteBatch::new(
        //         self.state.catalog_sequence.get(),
        //         self.state.db_id,
        //         self.state.db_name,
        //         IndexMap::new(), // Empty table chunks
        //         self.state.shard_id,
        //         None, // No single hash partition index for a potentially mixed batch
        //     ),
        //     shard_id: self.state.shard_id,
        // }
    }
}

// fn convert_qualified_line( // This function's logic is now effectively in lines_to_write_batch
//     line: QualifiedLine,
//     table_chunk_map: &mut IndexMap<TableId, TableChunks>,
//     gen1_duration: Gen1Duration,
// ) {
//     let chunk_time = gen1_duration.chunk_time_for_timestamp(Timestamp::new(line.row.time));
//     let table_chunks = table_chunk_map.entry(line.table_id).or_default();
//     table_chunks.push_row(chunk_time, line.row);
// }

#[derive(Debug, Clone)] // Added Clone
pub(crate) struct QualifiedLine { // Made pub(crate) for mod.rs access
    pub(crate) table_id: TableId,
    pub(crate) row: Row,
    pub(crate) index_count: usize,
    pub(crate) field_count: usize,
    pub(crate) hash_partition_index: Option<u32>,
    pub(crate) target_node_id_for_hash_partition: Option<NodeId>,
    pub(crate) original_lp_string: String, // Added for error reporting and forwarding
}

fn apply_precision_to_timestamp(precision: Precision, ts: i64) -> i64 {
    let multiplier = match precision {
        Precision::Auto => match crate::guess_precision(ts) {
            Precision::Second => 1_000_000_000,
            Precision::Millisecond => 1_000_000,
            Precision::Microsecond => 1_000,
            Precision::Nanosecond => 1,
            Precision::Auto => unreachable!(),
        },
        Precision::Second => 1_000_000_000,
        Precision::Millisecond => 1_000_000,
        Precision::Microsecond => 1_000,
        Precision::Nanosecond => 1,
    };
    ts * multiplier
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use super::WriteValidator;
    use crate::{Precision, write_buffer::Error};
    use data_types::NamespaceName;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_id::{TableId, NodeId}; // Added NodeId for tests
    use influxdb3_wal::{Gen1Duration, WriteBatch}; // Added WriteBatch for inspection
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use crate::write_buffer::validator::QualifiedLine; // To inspect QualifiedLine

    #[tokio::test]
    async fn write_validator_v1() -> Result<(), Error> {
        let node_id = Arc::from("sample-host-id");
        let obj_store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let namespace = NamespaceName::new("test").unwrap();
        let catalog = Arc::new(
            Catalog::new(node_id, obj_store, time_provider, Default::default())
                .await
                .unwrap(),
        );
        let expected_sequence = catalog.sequence_number().next();
        let result = WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog))?
            .v1_parse_lines_and_catalog_updates(
                "cpu,tag1=foo val1=\"bar\" 1234",
                false,
                Time::from_timestamp_nanos(0),
                Precision::Auto,
            )?
            .commit_catalog_changes()
            .await?
            .unwrap_success()
            .convert_lines_to_buffer(Gen1Duration::new_5m());

        println!("result: {result:?}");
        assert_eq!(result.line_count, 1);
        assert_eq!(result.field_count, 1);
        assert_eq!(result.index_count, 1);
        assert!(result.errors.is_empty());
        assert_eq!(expected_sequence, catalog.sequence_number());
        assert_eq!(result.valid_data.database_name.as_ref(), namespace.as_str());
        let batch = result
            .valid_data
            .table_chunks
            .get(&TableId::from(0))
            .unwrap();
        assert_eq!(batch.row_count(), 1);

        let expected_sequence = catalog.sequence_number();
        let result = WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog))?
            .v1_parse_lines_and_catalog_updates(
                "cpu,tag1=foo val1=\"bar\" 1235",
                false,
                Time::from_timestamp_nanos(0),
                Precision::Auto,
            )?
            .commit_catalog_changes()
            .await?
            .unwrap_success()
            .convert_lines_to_buffer(Gen1Duration::new_5m());

        println!("result: {result:?}");
        assert_eq!(result.line_count, 1);
        assert_eq!(result.field_count, 1);
        assert_eq!(result.index_count, 1);
        assert_eq!(expected_sequence, catalog.sequence_number());
        assert!(result.errors.is_empty());

        let expected_sequence = catalog.sequence_number().next();
        let result = WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog))?
            .v1_parse_lines_and_catalog_updates(
                "cpu,tag1=foo val1=\"bar\",val2=false 1236",
                false,
                Time::from_timestamp_nanos(0),
                Precision::Auto,
            )?
            .commit_catalog_changes()
            .await?
            .unwrap_success()
            .convert_lines_to_buffer(Gen1Duration::new_5m());

        println!("result: {result:?}");
        assert_eq!(result.line_count, 1);
        assert_eq!(result.field_count, 2);
        assert_eq!(result.index_count, 1);
        assert!(result.errors.is_empty());
        assert_eq!(expected_sequence, catalog.sequence_number());

        Ok(())
    }

    #[tokio::test]
    async fn test_validate_and_qualify_line_with_hashing() -> Result<(), Error> {
        let node_id_arc = Arc::from("test_node_hashing");
        let obj_store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let catalog = Arc::new(
            Catalog::new(node_id_arc, obj_store, time_provider, Default::default())
                .await
                .unwrap(),
        );

        let db_name_str = "hash_db";
        let table_name_str = "hash_table";
        let db_name_ns = NamespaceName::new(db_name_str).unwrap();

        // Setup table with shard_keys and num_hash_partitions
        catalog.create_database(db_name_str).await.unwrap();
        // For this test, we don't need to create columns via API, as validate_and_qualify_v1_line will add them.
        // We manually update the table's sharding metadata after it's implicitly created by first write.

        // Initial write to create the table implicitly
        let initial_lp = format!("{},tag1=valA,tag2=valX field1=1i 100", table_name_str);
        let mut validator = WriteValidator::initialize(db_name_ns.clone(), Arc::clone(&catalog))?;
        let lines_parsed_state = validator.v1_parse_lines_and_catalog_updates(
            &initial_lp, false, Time::from_timestamp_nanos(0), Precision::Nanosecond
        )?.into_inner();

        let db_id = lines_parsed_state.txn.db_schema().id;
        let table_id = lines_parsed_state.txn.db_schema().table_name_to_id(table_name_str).unwrap();

        // Now, apply the catalog changes from the initial write
        let _ = catalog.commit(lines_parsed_state.txn).await.unwrap();


        // Update table metadata for sharding
        let shard_keys = vec!["tag1".to_string(), "tag2".to_string()];
        let num_hash_partitions = 4u32;
        catalog.update_table_sharding_metadata(db_name_str, table_name_str, Some(shard_keys.clone()), Some(num_hash_partitions)).await.unwrap();

        // Test line that should be hashed
        let lp_to_hash = format!("{},tag1=valB,tag2=valY field2=2i 200", table_name_str);
        let mut validator_for_hash = WriteValidator::initialize(db_name_ns.clone(), Arc::clone(&catalog))?;
        let lines_parsed_for_hash = validator_for_hash.v1_parse_lines_and_catalog_updates(
            &lp_to_hash, false, Time::from_timestamp_nanos(0), Precision::Nanosecond
        )?.into_inner();

        assert_eq!(lines_parsed_for_hash.lines.len(), 1);
        let qualified_line_hashed = &lines_parsed_for_hash.lines[0];

        // Verify hash_partition_index calculation
        let expected_key_string = "tag1=valB,tag2=valY"; // Order based on shard_keys vec
        let expected_hash = murmur3_32(&mut Cursor::new(expected_key_string.as_bytes()), 0).unwrap();
        let expected_index = expected_hash % num_hash_partitions;

        assert_eq!(qualified_line_hashed.hash_partition_index, Some(expected_index));
        assert!(qualified_line_hashed.target_node_id_for_hash_partition.is_none()); // Still None at this stage

        // Test line missing a shard key tag (should error)
        let lp_missing_key = format!("{},tag1=valC field3=3i 300", table_name_str);
        let mut validator_missing_key = WriteValidator::initialize(db_name_ns.clone(), Arc::clone(&catalog))?;
        let result_missing_key = validator_missing_key.v1_parse_lines_and_catalog_updates(
            &lp_missing_key, false, Time::from_timestamp_nanos(0), Precision::Nanosecond
        );
        assert!(result_missing_key.is_err());
        if let Err(Error::ParseError(write_line_error)) = result_missing_key {
            assert!(write_line_error.error_message.contains("Missing required shard key tag: tag2"));
        } else {
            panic!("Expected a ParseError for missing shard key tag, got {:?}", result_missing_key);
        }

        // Test line for a table with num_hash_partitions = 1 (should result in None for hash_partition_index)
        let table_no_hash_str = "no_hash_table";
        let lp_no_hash = format!("{},tag1=valD field4=4i 400", table_no_hash_str);
        // Write to implicitly create table, default num_hash_partitions will be 1
        let mut validator_no_hash_create = WriteValidator::initialize(db_name_ns.clone(), Arc::clone(&catalog))?;
        let _ = validator_no_hash_create.v1_parse_lines_and_catalog_updates(
            &lp_no_hash, false, Time::from_timestamp_nanos(0), Precision::Nanosecond
        )?.into_inner(); // Consumed to create table in txn

        // Now parse again to check QualifiedLine
        let mut validator_no_hash = WriteValidator::initialize(db_name_ns.clone(), Arc::clone(&catalog))?;
        let lines_parsed_no_hash = validator_no_hash.v1_parse_lines_and_catalog_updates(
            &lp_no_hash, false, Time::from_timestamp_nanos(0), Precision::Nanosecond
        )?.into_inner();
        assert_eq!(lines_parsed_no_hash.lines.len(), 1);
        let qualified_line_no_hash = &lines_parsed_no_hash.lines[0];
        assert!(qualified_line_no_hash.hash_partition_index.is_none());
        assert!(qualified_line_no_hash.target_node_id_for_hash_partition.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_convert_lines_to_buffer_sets_hash_partition_index() -> Result<(), Error> {
        let node_id_arc = Arc::from("test_node_convert");
        let obj_store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let catalog = Arc::new(
            Catalog::new(node_id_arc, obj_store, time_provider, Default::default())
                .await
                .unwrap(),
        );
        let db_name_str = "hash_convert_db";
        let db_name_ns = NamespaceName::new(db_name_str).unwrap();
        catalog.create_database(db_name_str).await.unwrap();

        // Lines for the same table, time shard, but potentially different hash partitions
        let ql1 = QualifiedLine {
            table_id: TableId::new(1), row: Row { time: 100, fields: vec![] },
            index_count: 0, field_count: 0,
            hash_partition_index: Some(0), target_node_id_for_hash_partition: None,
        };
        let ql2 = QualifiedLine {
            table_id: TableId::new(1), row: Row { time: 101, fields: vec![] },
            index_count: 0, field_count: 0,
            hash_partition_index: Some(0), target_node_id_for_hash_partition: None,
        };
        let ql_different_hpi = QualifiedLine {
            table_id: TableId::new(1), row: Row { time: 102, fields: vec![] },
            index_count: 0, field_count: 0,
            hash_partition_index: Some(1), target_node_id_for_hash_partition: None,
        };
        let ql_none_hpi = QualifiedLine {
            table_id: TableId::new(1), row: Row { time: 103, fields: vec![] },
            index_count: 0, field_count: 0,
            hash_partition_index: None, target_node_id_for_hash_partition: None,
        };

        let time_shard_id = Some(ShardId::new(100));

        // Case 1: All lines have the same Some(hash_partition_index)
        let state1 = CatalogChangesCommitted {
            catalog_sequence: CatalogSequenceNumber::new(1), db_id: DbId::new(1), db_name: Arc::from(db_name_str),
            lines: vec![ql1.clone(), ql2.clone()], bytes: 10, errors: vec![], shard_id: time_shard_id,
        };
        let validator1 = WriteValidator::from(state1);
        let validated_lines1 = validator1.convert_lines_to_buffer(Gen1Duration::new_5m());
        assert_eq!(validated_lines1.valid_data.hash_partition_index, Some(0));

        // Case 2: Lines have different hash_partition_index values
        let state2 = CatalogChangesCommitted {
            catalog_sequence: CatalogSequenceNumber::new(2), db_id: DbId::new(1), db_name: Arc::from(db_name_str),
            lines: vec![ql1.clone(), ql_different_hpi.clone()], bytes: 10, errors: vec![], shard_id: time_shard_id,
        };
        let validator2 = WriteValidator::from(state2);
        let validated_lines2 = validator2.convert_lines_to_buffer(Gen1Duration::new_5m());
        assert_eq!(validated_lines2.valid_data.hash_partition_index, None);

        // Case 3: Some lines have None, others Some
        let state3 = CatalogChangesCommitted {
            catalog_sequence: CatalogSequenceNumber::new(3), db_id: DbId::new(1), db_name: Arc::from(db_name_str),
            lines: vec![ql1.clone(), ql_none_hpi.clone()], bytes: 10, errors: vec![], shard_id: time_shard_id,
        };
        let validator3 = WriteValidator::from(state3);
        let validated_lines3 = validator3.convert_lines_to_buffer(Gen1Duration::new_5m());
        assert_eq!(validated_lines3.valid_data.hash_partition_index, None);

        // Case 4: All lines have None for hash_partition_index
        let state4 = CatalogChangesCommitted {
            catalog_sequence: CatalogSequenceNumber::new(4), db_id: DbId::new(1), db_name: Arc::from(db_name_str),
            lines: vec![ql_none_hpi.clone(), ql_none_hpi.clone()], bytes: 10, errors: vec![], shard_id: time_shard_id,
        };
        let validator4 = WriteValidator::from(state4);
        let validated_lines4 = validator4.convert_lines_to_buffer(Gen1Duration::new_5m());
        assert_eq!(validated_lines4.valid_data.hash_partition_index, None); // Still None as filter checks for Some(first_hpi)

        // Case 5: Empty lines vector
        let state5 = CatalogChangesCommitted {
            catalog_sequence: CatalogSequenceNumber::new(5), db_id: DbId::new(1), db_name: Arc::from(db_name_str),
            lines: vec![], bytes: 0, errors: vec![], shard_id: time_shard_id,
        };
        let validator5 = WriteValidator::from(state5);
        let validated_lines5 = validator5.convert_lines_to_buffer(Gen1Duration::new_5m());
        assert_eq!(validated_lines5.valid_data.hash_partition_index, None);

        Ok(())
    }
}
