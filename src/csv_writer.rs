use datafusion::prelude::*;
use std::fs::File;
use csv::Writer;
use std::path::{Path, PathBuf};
use std::fs;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::array::{
    Array, StringArray, Int64Array, Float64Array, Int32Array,
    TimestampNanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray,
};

pub async fn write_to_csv(ctx: &SessionContext, query: &str, directory_path: &str, file_name: &str) -> datafusion::error::Result<()> {
    println!("\n\nwrite_to_csv");

    let df = ctx.sql(query).await?;
    let batches = df.collect().await?;

    // Create full file path
    let file_path = PathBuf::from(directory_path).join(file_name);
    println!("file_path: {}", file_path.display());

    // Ensure directory exists
    if let Some(parent_dir) = file_path.parent() {
        if !parent_dir.exists() {
            fs::create_dir_all(parent_dir)?;
        }
    } else {
        return Err(datafusion::error::DataFusionError::Execution("Invalid file path".to_string()));
    }

    let file = File::create(&file_path)?;
    let mut wtr = Writer::from_writer(file);

    // Write headers
    if let Some(batch) = batches.first() {
        wtr.write_record(batch.schema().fields().iter().map(|f| f.name()))
            .map_err(|err| datafusion::error::DataFusionError::Execution(err.to_string()))?;
    }

    // Write rows
    for batch in batches {
        for row in 0..batch.num_rows() {
            let record: Vec<String> = (0..batch.num_columns())
                .map(|col| {
                    let array = batch.column(col);
                    array_value_to_string(array.as_ref(), row)
                })
                .collect::<Result<_, datafusion::error::DataFusionError>>()?;
            wtr.write_record(record.iter())
                .map_err(|err| datafusion::error::DataFusionError::Execution(err.to_string()))?;
        }
    }

    wtr.flush()
        .map_err(|err| datafusion::error::DataFusionError::Execution(err.to_string()))?;

    Ok(())
}

fn array_value_to_string(array: &dyn Array, row: usize) -> Result<String, datafusion::error::DataFusionError> {
    if array.is_null(row) {
        return Ok(String::from(""));
    }

    match array.data_type() {
        DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(string_array.value(row).to_string())
        }
        DataType::Int32 => {
            let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(int_array.value(row).to_string())
        }
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(int_array.value(row).to_string())
        }
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(float_array.value(row).to_string())
        }
        DataType::Timestamp(time_unit, _) => match time_unit {
            TimeUnit::Second => {
                let timestamp_array = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                Ok(timestamp_array.value(row).to_string())
            }
            TimeUnit::Millisecond => {
                let timestamp_array = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                Ok(timestamp_array.value(row).to_string())
            }
            TimeUnit::Microsecond => {
                let timestamp_array = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                Ok(timestamp_array.value(row).to_string())
            }
            TimeUnit::Nanosecond => {
                let timestamp_array = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                Ok(timestamp_array.value(row).to_string())
            }
        },
        _ => Err(datafusion::error::DataFusionError::NotImplemented(
            format!("Unsupported data type: {:?}", array.data_type()),
        )),
    }
}
