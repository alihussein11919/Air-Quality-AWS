variable "aws_region" {
  type    = string
  default = "us-east-1" # Keeps it co-located with NOAA GFS bucket
}

variable "project_prefix" {
  type    = string
  default = "air-quality-weather"
}

variable "openaq_api_key" {
  type        = string
  sensitive   = true
  description = "OpenAQ API key for live data polling"
}

variable "alert_email" {
  type        = string
  description = "Email address for pipeline alerts"
}

variable "key_pair_name" {
  type        = string
  description = "EC2 key pair name for SSH access to Grafana instance"
}

# Fetch your AWS Account ID dynamically or pass it in
data "aws_caller_identity" "current" {}

# Data block to reference the pre-existing Learner Lab Role
data "aws_iam_role" "lab_role" {
  name = "LabRole"
}