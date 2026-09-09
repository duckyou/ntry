use std::collections::BTreeMap;

use crate::model::StoredRecord;

pub mod ddtrace;
pub mod otlp;
pub mod sentry;

#[derive(Debug)]
pub struct IngestBatch {
    pub records: Vec<StoredRecord>,
    pub skipped: BTreeMap<String, u64>,
}
