output "BQ_DATASET_STAGING" {
  value = var.bq_dataset_staging
}

output "BQ_DATASET_WAREHOUSE" {
  value = var.bq_dataset_warehouse
}

output "data_lake_bucket_name" {
  value = google_storage_bucket.data-lake-bucket.name
}