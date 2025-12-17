use std::sync::Arc;
use std::time::Instant;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use futures::StreamExt;

#[tokio::main]
async fn main() {
    let bucket = "skyway-diat-staging-data";
    let key = "raw/cccis-duckbill/skyway/skyway-export/data/BILLING_PERIOD=2025-04/skyway-export-00001.snappy.parquet";
    
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
    
    println!("Benchmarking Rust (object_store + parquet) against: s3://{}/{}", bucket, key);

    let s3 = AmazonS3Builder::from_env()
        .with_region(region)
        .with_bucket_name(bucket)
        .build()
        .expect("Failed to build S3 client");
    
    let store = Arc::new(s3);
    let path = ObjPath::from(key);
    let object_meta = store.head(&path).await.expect("Failed to HEAD object");

    // 1. Metadata Read (Footer)
    let start = Instant::now();
    let reader = ParquetObjectReader::new(store.clone(), object_meta.clone());
    let _builder = ParquetRecordBatchStreamBuilder::new(reader).await.expect("Failed to create builder");
    let duration = start.elapsed();
    println!("[Metadata] Time: {:.4}s", duration.as_secs_f64());

    // 2. Full Scan
    let start = Instant::now();
    let reader = ParquetObjectReader::new(store.clone(), object_meta.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await.expect("Failed to create builder");
    let mut stream = builder.with_batch_size(8192).build().expect("Failed to build stream");
    
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.expect("Error reading batch").num_rows();
    }
    let duration = start.elapsed();
    println!("[Full Scan] Time: {:.4}s ({} rows)", duration.as_secs_f64(), rows);

    // 3. Single Column Read
    let start = Instant::now();
    let reader = ParquetObjectReader::new(store.clone(), object_meta.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await.expect("Failed to create builder");
    let schema = builder.schema().clone();
    let col_idx = 0; // First column
    let builder = builder.with_projection(parquet::arrow::ProjectionMask::roots(&schema, vec![col_idx]));
    let mut stream = builder.with_batch_size(8192).build().expect("Failed to build stream");
    
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.expect("Error reading batch").num_rows();
    }
    let duration = start.elapsed();
    println!("[Single Col] Time: {:.4}s ({} rows)", duration.as_secs_f64(), rows);
}

