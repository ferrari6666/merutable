/// Opaque scan iterator returned by `MeruDB::scan`.
/// Wraps the engine's `MergingIterator` as a `futures::Stream`.
pub struct ScanIterator;
