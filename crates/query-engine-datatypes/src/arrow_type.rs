use arrow::datatypes::DataType;

/// Central definition of all Arrow data types used in the query engine.
/// Each variant maps to a corresponding Apache Arrow DataType.
/// This avoids scattering raw Arrow type constructors across the codebase.
/// Everything else we build will refer to ArrowType rather than Arrow's DataType directly.
// We need PartialEq and Debug for assert_eq! since:
// We need to compare 2 values for equality, which requires they implement PartialEq.
// If the assertion fails, Rust needs to print both values to show you what went wrong, so the type must have Debug.
#[derive(Clone, PartialEq, Debug)]
pub enum ArrowType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
}

/// Converts an ArrowType variant into its corresponding Apache Arrow DataType.
/// Allows using `.into()` anywhere an Arrow DataType is expected.
impl From<ArrowType> for DataType {
    fn from(t: ArrowType) -> Self {
        match t {
            ArrowType::Boolean => DataType::Boolean,
            ArrowType::Int8 => DataType::Int8,
            ArrowType::Int16 => DataType::Int16,
            ArrowType::Int32 => DataType::Int32,
            ArrowType::Int64 => DataType::Int64,
            ArrowType::UInt8 => DataType::UInt8,
            ArrowType::UInt16 => DataType::UInt16,
            ArrowType::UInt32 => DataType::UInt32,
            ArrowType::UInt64 => DataType::UInt64,
            ArrowType::Float32 => DataType::Float32,
            ArrowType::Float64 => DataType::Float64,
            ArrowType::String => DataType::Utf8,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn test_boolean_into_arrow() {
        let dt: DataType = ArrowType::Boolean.into();
        assert_eq!(dt, DataType::Boolean);
    }

    #[test]
    fn test_int32_into_arrow() {
        let dt: DataType = ArrowType::Int32.into();
        assert_eq!(dt, DataType::Int32);
    }
}
