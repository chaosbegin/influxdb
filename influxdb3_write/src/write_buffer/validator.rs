use std::sync::Arc;

use crate::{Precision, WriteLineError, write_buffer::Result};
use data_types::{NamespaceName, Timestamp};
use indexmap::IndexMap;
use influxdb3_catalog::{
    catalog::{Catalog, CatalogSequenceNumber, DatabaseCatalogTransaction, Prompt},
    shard::ShardingStrategy, // Import ShardingStrategy
};

use influxdb_line_protocol::{parse_lines, ParsedLine};
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
    pub(crate) lines: Vec<QualifiedLine>, // This will now contain QualifiedLine with shard key values
    pub(crate) bytes: u64,
    pub(crate) errors: Vec<WriteLineError>,
    pub(crate) shard_id: Option<ShardId>,
}

use influxdb3_catalog::shard::ShardId; // Keep this import as it's used

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
                    original_line: lp_lines.next().unwrap_or_default().to_string(), // Added unwrap_or_default
                    line_number: line_idx + 1,
                    error_message: e.to_string(),
                })
                .and_then(|l| {
                    let raw_line = lp_lines.next().unwrap_or_default(); // Added unwrap_or_default
                    validate_and_qualify_v1_line(
                        &mut self.state.txn,
                        line_idx,
                        l,
                        ingest_time,
                        precision,
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
) -> Result<QualifiedLine, WriteLineError> {
    let table_name = line.series.measurement.as_str();
    let mut fields = Vec::with_capacity(line.column_count());
    let mut index_count = 0;
    let mut field_count = 0;
    let mut parsed_shard_key_values: Option<Vec<String>> = None;

    let table_def = txn
        .table_or_create(table_name)
        .map_err(|error| WriteLineError {
            original_line: line.to_string(),
            line_number: line_number + 1,
            error_message: error.to_string(),
        })?;

    // Extract shard key values if strategy is TimeAndKey
    if table_def.sharding_strategy == ShardingStrategy::TimeAndKey {
        if let Some(ref key_columns) = table_def.shard_key_columns {
            if !key_columns.is_empty() {
                let mut extracted_values = Vec::with_capacity(key_columns.len());
                for key_col_name in key_columns {
                    let found_value = line.series.tag_set.as_ref().and_then(|tags| {
                        tags.iter().find(|(k, _)| k.as_str() == key_col_name)
                    });

                    if let Some((_, tag_val)) = found_value {
                        extracted_values.push(tag_val.to_string());
                    } else {
                        return Err(WriteLineError {
                            original_line: line.to_string(),
                            line_number: line_number + 1,
                            error_message: format!(
                                "Missing shard key column '{}' for table '{}' with TimeAndKey sharding strategy.",
                                key_col_name, table_name
                            ),
                        });
                    }
                }
                parsed_shard_key_values = Some(extracted_values);
            } else {
                 return Err(WriteLineError {
                    original_line: line.to_string(),
                    line_number: line_number + 1,
                    error_message: format!(
                        "Table '{}' has TimeAndKey sharding strategy but an empty shard_key_columns list defined.",
                        table_name
                    ),
                });
            }
        } else {
            return Err(WriteLineError {
                original_line: line.to_string(),
                line_number: line_number + 1,
                error_message: format!(
                    "Table '{}' has TimeAndKey sharding strategy but no shard key columns defined.",
                    table_name
                ),
            });
        }
    }

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

    Ok(QualifiedLine {
        table_id: table_def.table_id,
        row: Row {
            time: timestamp_ns,
            fields,
        },
        index_count,
        field_count,
        parsed_shard_key_values,
    })
}

impl WriteValidator<LinesParsed> {
    pub async fn commit_catalog_changes(
        self,
    ) -> Result<Prompt<WriteValidator<CatalogChangesCommitted>>> {
        let db_schema = self.state.txn.db_schema();
        let db_id = db_schema.id;
        let db_name = Arc::clone(&db_schema.name);
        let determined_shard_id: Option<ShardId> = None; // Placeholder, to be set by WriteBufferImpl

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
    pub fn convert_lines_to_buffer(self, gen1_duration: Gen1Duration) -> ValidatedLines {
        let mut table_chunks = IndexMap::new();
        let line_count = self.state.lines.len();
        let mut field_count = 0;
        let mut index_count = 0;

        for line in self.state.lines.into_iter() {
            field_count += line.field_count;
            index_count += line.index_count;
            // Note: line.parsed_shard_key_values is available here if needed for further logic
            // before converting to WriteBatch, but WriteBatch itself doesn't store per-line keys.
            // The primary purpose of extracting them was for shard determination, which happens
            // in WriteBufferImpl before this conversion step (by passing the lines from
            // CatalogChangesCommitted to a shard determination function).
            convert_qualified_line(line, &mut table_chunks, gen1_duration);
        }

        let write_batch = WriteBatch::new(
            self.state.catalog_sequence.get(),
            self.state.db_id,
            self.state.db_name,
            table_chunks,
            self.state.shard_id,
        );

        ValidatedLines {
            line_count,
            valid_bytes_count: self.state.bytes,
            field_count,
            index_count,
            errors: self.state.errors,
            valid_data: write_batch,
            shard_id: self.state.shard_id,
        }
    }
}

fn convert_qualified_line(
    line: QualifiedLine,
    table_chunk_map: &mut IndexMap<TableId, TableChunks>,
    gen1_duration: Gen1Duration,
) {
    let chunk_time = gen1_duration.chunk_time_for_timestamp(Timestamp::new(line.row.time));
    let table_chunks = table_chunk_map.entry(line.table_id).or_default();
    table_chunks.push_row(chunk_time, line.row);
}

#[derive(Debug)]
struct QualifiedLine {
    table_id: TableId,
    row: Row,
    index_count: usize,
    field_count: usize,
    parsed_shard_key_values: Option<Vec<String>>, // Added for shard key values
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
    use influxdb3_id::TableId;
    use influxdb3_wal::Gen1Duration;
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    // Added for shard key tests
    use influxdb3_catalog::shard::ShardingStrategy;


    #[tokio::test]
    async fn write_validator_v1() -> Result<(), Error> {
        let node_id = Arc::from("sample-host-id");
        let obj_store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let namespace = NamespaceName::new("test").unwrap();
        let catalog = Arc::new(
            Catalog::new_with_args( // Use new_with_args to provide default limits
                node_id,
                obj_store,
                time_provider,
                Default::default(),
                Default::default(),
                Default::default()
            )
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
            .get(&TableId::from(0)) // Assuming first table gets ID 0
            .unwrap();
        assert_eq!(batch.row_count(), 1);

        let expected_sequence = catalog.sequence_number();
        let result = WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog))?
            .v1_parse_lines_and_catalog_updates(
                "cpu,tag1=foo val1=\"bar\" 1235", // Different timestamp to avoid duplicate points if WAL is replayed by same test instance
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
        assert_eq!(expected_sequence, catalog.sequence_number()); // No schema change, so sequence shouldn't advance
        assert!(result.errors.is_empty());

        let expected_sequence = catalog.sequence_number().next(); // Expect sequence to advance due to new field
        let result = WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog))?
            .v1_parse_lines_and_catalog_updates(
                "cpu,tag1=foo val1=\"bar\",val2=false 1236", // New field val2
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
    async fn test_shard_key_extraction_success() {
        let catalog = Arc::new(Catalog::new_in_memory("test_host_sk_succ").await.unwrap());
        let db_name = NamespaceName::new("test_db_sk_succ").unwrap();
        catalog.create_database(db_name.as_str()).await.unwrap();
        catalog.create_table(db_name.as_str(), "my_table", &["tag_a", "tag_b", "tag_c"], &[("value", FieldDataType::Integer)]).await.unwrap();
        catalog.update_table_sharding_strategy(
            db_name.as_str(),
            "my_table",
            ShardingStrategy::TimeAndKey,
            Some(vec!["tag_a".to_string(), "tag_c".to_string()])
        ).await.unwrap();

        let lp = "my_table,tag_a=val_a,tag_b=val_b,tag_c=val_c value=100 1000";
        let validator_init = WriteValidator::initialize(db_name.clone(), Arc::clone(&catalog)).unwrap();
        let lines_parsed_result = validator_init.v1_parse_lines_and_catalog_updates(
            lp,
            false,
            Time::from_timestamp_nanos(0),
            Precision::Nanosecond
        );

        assert!(lines_parsed_result.is_ok(), "Parsing failed: {:?}", lines_parsed_result.err());
        let lines_parsed = lines_parsed_result.unwrap().into_inner();
        assert_eq!(lines_parsed.lines.len(), 1);
        assert!(lines_parsed.errors.is_empty());

        let qualified_line = &lines_parsed.lines[0];
        assert_eq!(
            qualified_line.parsed_shard_key_values,
            Some(vec!["val_a".to_string(), "val_c".to_string()])
        );
    }

    #[tokio::test]
    async fn test_shard_key_extraction_missing_key() {
        let catalog = Arc::new(Catalog::new_in_memory("test_host_sk_miss").await.unwrap());
        let db_name = NamespaceName::new("test_db_sk_miss").unwrap();
        catalog.create_database(db_name.as_str()).await.unwrap();
        catalog.create_table(db_name.as_str(), "my_table", &["tag_a", "tag_b"], &[("value", FieldDataType::Integer)]).await.unwrap();
        catalog.update_table_sharding_strategy(
            db_name.as_str(),
            "my_table",
            ShardingStrategy::TimeAndKey,
            Some(vec!["tag_a".to_string(), "tag_c".to_string()]) // tag_c is the shard key but not in table create
        ).await.unwrap(); // This setup itself is problematic if tag_c doesn't exist, but let's assume it passes for now for the test's purpose.
                         // The update_table_sharding_strategy should ideally validate columns exist.

        // To make the test more focused on line parsing, ensure tag_c column is known to the catalog for "my_table"
        let mut txn = catalog.begin(db_name.as_str()).unwrap();
        txn.column_or_create("my_table", "tag_c", FieldDataType::Tag).unwrap();
        catalog.commit(txn).await.unwrap();


        let lp = "my_table,tag_a=val_a,tag_b=val_b value=100 1000"; // tag_c is missing from line
        let validator_init = WriteValidator::initialize(db_name.clone(), Arc::clone(&catalog)).unwrap();
        let lines_parsed_result = validator_init.v1_parse_lines_and_catalog_updates(
            lp,
            false,
            Time::from_timestamp_nanos(0),
            Precision::Nanosecond
        );

        assert!(lines_parsed_result.is_err(), "Parsing should fail due to missing shard key");
        if let Err(Error::ParseError(e)) = lines_parsed_result {
            assert!(e.error_message.contains("Missing shard key column 'tag_c'"));
        } else {
            panic!("Expected ParseError with specific message, got {:?}", lines_parsed_result);
        }
    }

     #[tokio::test]
    async fn test_shard_key_extraction_strategy_time_only() {
        let catalog = Arc::new(Catalog::new_in_memory("test_host_sk_time").await.unwrap());
        let db_name = NamespaceName::new("test_db_sk_time").unwrap();
        catalog.create_database(db_name.as_str()).await.unwrap();
        catalog.create_table(db_name.as_str(), "my_table", &["tag_a", "tag_b"], &[("value", FieldDataType::Integer)]).await.unwrap();
        // ShardingStrategy is Time by default, no explicit shard_key_columns

        let lp = "my_table,tag_a=val_a,tag_b=val_b value=100 1000";
        let validator_init = WriteValidator::initialize(db_name.clone(), Arc::clone(&catalog)).unwrap();
        let lines_parsed_result = validator_init.v1_parse_lines_and_catalog_updates(
            lp,
            false,
            Time::from_timestamp_nanos(0),
            Precision::Nanosecond
        );

        assert!(lines_parsed_result.is_ok(), "Parsing failed: {:?}", lines_parsed_result.err());
        let lines_parsed = lines_parsed_result.unwrap().into_inner();
        assert_eq!(lines_parsed.lines.len(), 1);
        assert!(lines_parsed.errors.is_empty());

        let qualified_line = &lines_parsed.lines[0];
        assert_eq!(qualified_line.parsed_shard_key_values, None);
    }
}
