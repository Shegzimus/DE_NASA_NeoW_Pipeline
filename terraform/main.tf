terraform {
  required_version = ">= 1.0"
  backend "gcs" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

# Instantiate the google cloud provider
provider "google" {
  project = var.project
  region = var.region
  credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Enable Scheduler and Pub/Sub APIs
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/cloud_scheduler_job
resource "google_project_service" "composer_api" {
  project = var.project
  service = "composer.googleapis.com"
}

resource "google_project_service" "storage_api" {
  project = var.project
  service = "storage.googleapis.com"
}

resource "google_project_service" "cloud_sql_api" {
  project = var.project
  service = "sqladmin.googleapis.com"
}

resource "google_project_service" "pubsub_api" {
  project = var.project
  service = "pubsub.googleapis.com"
}



# Provision Cloud SQL Instance (Metadata Database)
resource "google_sql_database_instance" "composer_sql_instance" {
  name             = "composer-sql-instance"
  region           = var.region
  database_version = "POSTGRES_13"

  settings {
    tier = "db-n1-standard-2"
  }
}


# Provision Cloud Composer Environment
resource "google_composer_environment" "composer_environment" {
  name   = "Job_Orchestrator_Environment"
  region = var.region
  project = var.project

  config {
    # Airflow version (set to 2.x to use the latest version of Airflow)
    airflow_version = "2.x"

    # Environment configuration
    node_count      = 3  # Number of nodes in the cluster
    machine_type    = "n1-standard-2"  # Machine type for Airflow workers

    # Optional: You can define Cloud SQL configuration for Airflow metadata DB
    private_environment_config {
      cloud_sql_instance = google_sql_database_instance.composer_sql_instance.name
    }

    # DAGs GCS bucket where DAG files are stored
    dag_gcs_prefix = "gs://${local.data_lake_bucket}_${var.project}/dags"

    # GCS bucket for storing logs
    gcs_bucket = "${local.data_lake_bucket}_${var.project}"
  }

  depends_on = [
    google_project_service.composer_api,
    google_project_service.storage_api,
    google_project_service.cloud_sql_api
  ]
}



# resource "google_composer_environment" "example" {
#   name     = "example-environment"
#   region   = "us-central1"
#   project  = "your-project-id"

#   config {
#     node_count = 3

#     software_config {
#       airflow_version = "2.x"
#       python_version = "3.x"
#     }
#   }
# }


# Provision GCS Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

# Provision BigQuery Datasets
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "staging_layer" {
  dataset_id = var.bq_dataset_staging
  project    = var.project
  location   = var.region
}

resource "google_bigquery_dataset" "warehouse_layer" {
  dataset_id = var.bq_dataset_warehouse
  project    = var.project
  location   = var.region
}



# Set up a Pubsub Topic
resource "google_pubsub_topic" "scheduler_topic" {
  name = "weekly-task-trigger"
}


# Create the Cloud Scheduler Job
resource "google_cloud_scheduler_job" "pubsub_job" {
  name             = "weekly-NASA-task-job"
  description      = "Trigger Cloud Function weekly via Pub/Sub"
  schedule         = "0 9 * * 1" # Every Monday at 9:00 AM UTC
  time_zone        = "UTC"

  pubsub_target {
    topic_name = google_pubsub_topic.scheduler_topic.id
    data       = base64encode(jsonencode({ "message": "Run weekly NASA task" }))
  }
}

# Grant Permission
resource "google_pubsub_subscription" "function_subscription" {
  name  = "function-subscription"
  topic = google_pubsub_topic.scheduler_topic.id
  push_config {
    push_endpoint = "https://${var.region}-${var.project}.cloudfunctions.net/FUNCTION_NAME"
    
    oidc_token {
      service_account_email = "your-service-account@${var.project}.iam.gserviceaccount.com"
    }
  }
}

resource "google_cloudfunctions_function" "my_function" {
  name        = "my-cloud-function"
  runtime     = "python310"  # Can be any supported runtime like nodejs, python, etc.
  entry_point = "main"  # Entry point function in your code
  region      = "us-central1"
  project     = "your-project-id"

  source_archive_bucket = "your-bucket-name"
  source_archive_object = "function-source.zip"

  trigger_http = true

  # Optional: Allow unauthenticated access
  https_trigger {
    insecure = false
  }
}


