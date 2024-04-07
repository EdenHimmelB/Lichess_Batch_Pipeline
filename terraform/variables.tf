variable "project" {
  description = "Your GCP Project ID"
  default     = "data-engineering-starter"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "asia-east2"
  type        = string
}

variable "gcp_service_apis" {
  description = "The list of apis necessary for the project"
  type        = list(string)
  default = [
    "cloudresourcemanager.googleapis.com",
    "serviceusage.googleapis.com",
    "iam.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com"
  ]
}

variable "service_account_roles" {
  description = "The list of roles necessary for your service account"
  type        = list(string)
  default = [
    "roles/storage.admin",
    "roles/bigquery.admin"
  ]
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "Your GCS bucket name."
  default     = "chess_object_storage_769413"
}

variable "standard_games_folder" {
  description = "GCS folder for chess standard games."
  default     = "standard_games/"
}

variable "bigquery_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to."
  type        = string
  default     = "chess_datawarehouse"
}
