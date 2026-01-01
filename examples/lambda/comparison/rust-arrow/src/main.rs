use arrow::array::{Array, RecordBatchReader, StringArray, UInt32Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_select::take::take;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

// 25 columns for realistic cost analysis query (verified to exist in file)
const SELECTED_COLUMNS: &[&str] = &[
    "bill_payer_account_id",
    "bill_payer_account_name",
    "identity_line_item_id",
    "identity_time_interval",
    "line_item_availability_zone",
    "line_item_blended_cost",
    "line_item_blended_rate",
    "line_item_currency_code",
    "line_item_line_item_description",
    "line_item_line_item_type",
    "line_item_operation",
    "line_item_product_code",
    "line_item_resource_id",
    "line_item_unblended_cost",
    "line_item_unblended_rate",
    "line_item_usage_account_id",
    "line_item_usage_amount",
    "line_item_usage_end_date",
    "line_item_usage_start_date",
    "line_item_usage_type",
    "product_product_family",
    "product_region_code",
    "product_servicecode",
    "pricing_public_on_demand_cost",
    "pricing_public_on_demand_rate",
];

#[derive(Deserialize)]
struct Request {
    input_path: String,
    output_path: String,
    filter_column: String,
    filter_value: String,
}

#[derive(Serialize)]
struct Response {
    status: String,
    input_rows: usize,
    output_rows: usize,
    output_size_bytes: usize,
    download_time_ms: f64,
    filter_time_ms: f64,
    upload_time_ms: f64,
    total_time_ms: f64,
    output_path: String,
}

fn parse_s3_path(path: &str) -> Option<(String, String)> {
    let path = path.strip_prefix("s3://")?;
    let (bucket, key) = path.split_once('/')?;
    Some((bucket.to_string(), key.to_string()))
}

async fn handler(event: LambdaEvent<Request>) -> Result<Response, Error> {
    let total_start = Instant::now();
    let req = event.payload;

    let (in_bucket, in_key) =
        parse_s3_path(&req.input_path).ok_or_else(|| Error::from("Invalid input_path"))?;
    let (out_bucket, out_key) =
        parse_s3_path(&req.output_path).ok_or_else(|| Error::from("Invalid output_path"))?;

    // Initialize S3 client
    let config = aws_config::load_from_env().await;
    let s3 = S3Client::new(&config);

    // Download input file
    let download_start = Instant::now();
    let resp = s3
        .get_object()
        .bucket(&in_bucket)
        .key(&in_key)
        .send()
        .await?;
    let data = resp.body.collect().await?.into_bytes();
    let download_time = download_start.elapsed().as_secs_f64() * 1000.0;

    // Parse and filter
    let filter_start = Instant::now();

    // Build set of selected columns (including filter column)
    let selected_set: HashSet<&str> = SELECTED_COLUMNS.iter().copied().collect();

    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data.to_vec()))?
        .with_batch_size(8192)
        .build()?;

    let full_schema = reader.schema().clone();

    // Find indices of columns we want to keep
    let mut keep_indices: Vec<usize> = Vec::new();
    let mut output_fields: Vec<Arc<Field>> = Vec::new();
    let mut filter_col_idx: Option<usize> = None;

    for (idx, field) in full_schema.fields().iter().enumerate() {
        if field.name() == &req.filter_column {
            filter_col_idx = Some(idx);
        }
        if selected_set.contains(field.name().as_str()) {
            keep_indices.push(idx);
            output_fields.push(field.clone());
        }
    }

    let filter_col_idx = filter_col_idx
        .ok_or_else(|| Error::from(format!("Column '{}' not found", req.filter_column)))?;

    let output_schema = Arc::new(Schema::new(output_fields));

    let mut input_rows = 0usize;
    let mut filtered_batches: Vec<RecordBatch> = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        input_rows += batch.num_rows();

        // Get filter column as string array
        let col = batch.column(filter_col_idx);
        let string_col = col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::from("Filter column must be string type"))?;

        // Build selection mask
        let mut indices: Vec<usize> = Vec::new();
        for i in 0..string_col.len() {
            if string_col.is_valid(i) && string_col.value(i) == req.filter_value {
                indices.push(i);
            }
        }

        if !indices.is_empty() {
            // Select matching rows for only the columns we want
            let indices_array = UInt32Array::from_iter(indices.iter().map(|&i| i as u32));
            let filtered_columns: Vec<Arc<dyn Array>> = keep_indices
                .iter()
                .map(|&idx| take(batch.column(idx).as_ref(), &indices_array, None).unwrap())
                .collect();

            let filtered_batch = RecordBatch::try_new(output_schema.clone(), filtered_columns)?;
            filtered_batches.push(filtered_batch);
        }
    }

    let output_rows: usize = filtered_batches.iter().map(|b| b.num_rows()).sum();
    let filter_time = filter_start.elapsed().as_secs_f64() * 1000.0;

    // Write output parquet
    let mut output_buf = Vec::new();
    {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(&mut output_buf, output_schema, Some(props))?;
        for batch in &filtered_batches {
            writer.write(batch)?;
        }
        writer.close()?;
    }
    let output_size = output_buf.len();

    // Upload to S3
    let upload_start = Instant::now();
    s3.put_object()
        .bucket(&out_bucket)
        .key(&out_key)
        .body(output_buf.into())
        .send()
        .await?;
    let upload_time = upload_start.elapsed().as_secs_f64() * 1000.0;

    let total_time = total_start.elapsed().as_secs_f64() * 1000.0;

    Ok(Response {
        status: "success".to_string(),
        input_rows,
        output_rows,
        output_size_bytes: output_size,
        download_time_ms: download_time,
        filter_time_ms: filter_time,
        upload_time_ms: upload_time,
        total_time_ms: total_time,
        output_path: req.output_path,
    })
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda_runtime::run(service_fn(handler)).await
}
