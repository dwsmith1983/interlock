variable "project_name" {
  description = "Project name used for state bucket and lock table naming"
  type        = string
  default     = "interlock"
}

variable "aws_region" {
  description = "AWS region for the state resources"
  type        = string
  default     = "ap-southeast-1"
}
