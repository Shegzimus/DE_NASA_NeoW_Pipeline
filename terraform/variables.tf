locals {
  data_lake_bucket = "de_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "nasa-neows"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west3"
  type = string
}

variable "credentials" {
  description = "Path to the credential json file"
  default = "nasa-neows-c12d19292fad.json"
  type = string
}


variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "bq_dataset_staging" {
  description = "BigQuery Dataset that serves as staging layer where data tables are injested into"
  type = string
  default = "de_dataset_staging"
}

variable "bq_dataset_warehouse" {
  description = "BigQuery Dataset that serves as final layer where aggregates are injested into"
  type = string
  default = "de_dataset_warehouse"
}
