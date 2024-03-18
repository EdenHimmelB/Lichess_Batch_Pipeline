variable "project" {
  description = "Your GCP Project ID"
  default = "data-engineering-starter"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "US"
  type = string
}

variable "gcp_service_apis" {
  description ="The list of apis necessary for the project"
  type = list(string)
  default = [
    "cloudresourcemanager.googleapis.com",
    "serviceusage.googleapis.com",
    "iam.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com"
  ]
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "gcs_bucket_name" {
    description = "Your GCS bucket name."
    default = "lichess_batch_pipeline_1"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to."
  type = string
  default = "tennis_data"
}
variable "BQ_STAGING_DATASET" {
  description = "BigQuery Dataset that will serve as staging."
  type = string
  default = "staging"
}

variable "BQ_PRODUCTION_DATASET" {
  description = "BigQuery Dataset that will serve as production for final tables."
  type = string
  default = "production"
}