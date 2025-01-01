# DataFusion CSV Export Utility

This project provides a Rust utility function to execute SQL queries on parquet data using [DataFusion](https://arrow.apache.org/datafusion/), and export the results to a CSV file. It supports a variety of Arrow data types and ensures compatibility with common timestamp formats.

## Features

- Execute SQL queries using the DataFusion library.
- Collect query results and write them to a CSV file.
- Supports various Arrow data types, including:
  - Strings
  - Integers
  - Floats
  - Timestamps (with different granularities: seconds, milliseconds, microseconds, nanoseconds).
- Ensures the output directory exists before writing the CSV file.
- Provides robust error handling for file creation, directory validation, and data processing.

## Requirements

- Rust (latest stable version recommended)
- Tokio (for asynchronous runtime)
- DataFusion library
- CSV library for writing CSV files

## How It Works

1. **Query Execution**: The function accepts a SQL query string and executes it on a registered data source.
2. **Result Collection**: The query results are collected into Arrow record batches.
3. **CSV Export**:
   - The utility ensures the target directory exists.
   - It writes the header row and corresponding data rows to the specified CSV file.


## Function Documentation

### `write_to_csv`

#### Arguments

- `ctx: &SessionContext`: A reference to the DataFusion session context.
- `query: &str`: The SQL query to execute.
- `directory_path: &str`: The target directory where the CSV file will be saved.
- `file_name: &str`: The name of the output CSV file.

#### Returns

- `datafusion::error::Result<()>`: Returns `Ok(())` if successful, or an error if any step fails.

### `array_value_to_string`

#### Arguments

- `array: &dyn Array`: A reference to an Arrow array.
- `row: usize`: The row index to retrieve the value.

#### Returns

- `Result<String, datafusion::error::DataFusionError>`: The value as a string, or an error if the conversion fails.
