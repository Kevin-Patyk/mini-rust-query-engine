use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};

use crate::arrow_type::ArrowType;

/// Summary:
/// Schema represents the structure of a dataset as a collection of named, typed columns.
/// Field represents a single column definition with a name and data type.
/// Field::to_arrow() converts a Field into an Apache Arrow Field.
/// Schema::to_arrow() converts the entire Schema into an Apache Arrow Schema.
/// Schema::project() returns a new schema with only the fields at the given indices.
/// Schema::select() returns a new Schema with only the fields matching the given names.

/// Represents a single column definition in a schema.
/// Contains the column name and its data type.
/// Does not hold any actual data - just describes the structure.
#[derive(Clone)]
pub struct Field {
    pub name: String,
    pub data_type: ArrowType,
    // nullable_flag: bool, <-- In more complex systems, we would have this field as well.
}

/// Converts this Field into an Apache Arrow Field.
/// The nullable flag is hardcoded true for now.
/// Uses the From implementation on ArrowType to convert the data type.
impl Field {
    pub fn to_arrow(self) -> ArrowField {
        ArrowField::new(self.name, self.data_type.into(), true)
    }
}

/// Represents the structure of a dataset of a collection of named, typed columns.
/// A schema describes the shape of the data without holding any actual values.
/// fields is a vector of Field structs (containing the column name and its data type).
#[derive(Clone)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    /// Converts this Schema into an Apache Arrow Schema.
    pub fn to_arrow(self) -> ArrowSchema {
        // ArrowSchema::new expects a Vec<arrow::datatypes::Field>.
        // We need to convert all of the Fields in our vector to this before passing it in.
        ArrowSchema::new(
            self.fields
                .into_iter()
                .map(|f| f.to_arrow())
                .collect::<Vec<_>>(),
        )
    }

    /// This takes a list of indices and returns a new Schema with only those fields.
    /// For example, if you have fields [id, name, age, salary] and pass in [0, 3]
    /// you get back a schema with just [id, salary].
    pub fn project(&self, indices: &[usize]) -> Self {
        Schema {
            // map is a general purpose transformation - you give it something and transform it into something else.
            // In this function, we are transforming indices into fields.
            fields: indices.iter().map(|i| self.fields[*i].clone()).collect(),
        }
    }

    /// This takes a list of column names and returns a new Schema with only those fields.
    /// So if you pass in ["age", "salary"], you would get back a schema with just those fields.
    pub fn select(&self, names: &[&str]) -> Result<Self, String> {
        // We iterate over names, transforming each name into a Result<Field, String>.
        // find searches self.fields for a field whose name matches n, returning Some(field).
        // cloned converts Option<&Field> to Option<Field> and ok_or converts None into an Err.
        let fields = names
            .iter()
            .map(|n| {
                self.fields
                    .iter()
                    .find(|f| f.name == *n)
                    .cloned()
                    .ok_or(format!("Field {n} does not exist in the schema"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Schema { fields })
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field as ArrowField;

    use super::*;

    fn test_fields() -> Vec<Field> {
        vec![
            Field {
                name: "id".to_string(),
                data_type: ArrowType::Int32,
            },
            Field {
                name: "name".to_string(),
                data_type: ArrowType::String,
            },
            Field {
                name: "age".to_string(),
                data_type: ArrowType::Int32,
            },
        ]
    }

    #[test]
    fn test_field_to_arrow() {
        let field = test_fields()[0].clone();

        let out = field.to_arrow();
        assert_eq!(
            out,
            ArrowField::new("id", arrow::datatypes::DataType::Int32, true)
        )
    }

    #[test]
    fn test_schema_to_arrow() {
        let schema = Schema {
            fields: test_fields(),
        };

        let out = schema.to_arrow();
        assert_eq!(
            out,
            ArrowSchema::new(
                test_fields()
                    .into_iter()
                    .map(|f| f.to_arrow())
                    .collect::<Vec<_>>()
            )
        )
    }

    #[test]
    fn test_schema_project() {
        let schema = Schema {
            fields: test_fields(),
        };

        let out = schema.project(&[0]);
        assert_eq!(out.fields[0].name, "id");
        assert_eq!(out.fields[0].data_type, ArrowType::Int32)
    }

    #[test]
    fn test_schema_select() {
        let schema = Schema {
            fields: test_fields(),
        };

        let out = schema.select(&["age"]);
        // We need to clone out since select() returns Result<Schema, String>.
        // When you call unwrap() on a Result, it consumes it, so the second assert_eq! can't use out anymore.
        // Thus, we need to clone() the first unwrap() so it is available for the second assertion.
        assert_eq!(out.clone().unwrap().fields[0].name, "age");
        assert_eq!(out.unwrap().fields[0].data_type, ArrowType::Int32)
    }
}
