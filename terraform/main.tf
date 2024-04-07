terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.18.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

# Enable necessary APIs for the project.
resource "google_project_service" "gcp_services" {
  for_each                   = toset(var.gcp_service_apis)
  project                    = var.project
  service                    = each.key
  disable_dependent_services = true
}

# Create a service account to operates task on the cloud.
resource "google_service_account" "my_service_account" {
  account_id                   = "chess-batch-pipeline-worker"
  display_name                 = "Main service account which enables all operations in the cloud."
  create_ignore_already_exists = true
}

# Grant role to service account
resource "google_project_iam_member" "example" {
  for_each = toset(var.service_account_roles)
  project  = var.project
  role     = each.key
  member   = "serviceAccount:${google_service_account.my_service_account.email}"
}

# Enable a key for main service account.
resource "google_service_account_key" "my_service_account_key" {
  service_account_id = google_service_account.my_service_account.name
  public_key_type    = "TYPE_X509_PEM_FILE"
}

# Download the JSON key of service account to local machine.
resource "local_sensitive_file" "google_application_credentials" {
  content_base64 = google_service_account_key.my_service_account_key.private_key
  filename       = "../keys/google_application_credentials.json"
}

resource "google_storage_bucket" "production_bucket" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket_object" "empty_folder" {
  name       = var.standard_games_folder
  content    = " " # content is ignored but should be non-empty
  bucket     = var.gcs_bucket_name
  depends_on = [google_storage_bucket.production_bucket]
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bigquery_dataset
  project    = var.project
  location   = var.region
  depends_on = [google_project_service.gcp_services]
}

