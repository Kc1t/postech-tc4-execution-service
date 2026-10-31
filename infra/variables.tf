variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "postech-tc4"
}

variable "environment" {
  type = string
}

variable "table_name" {
  type        = string
  default     = "executions"
  description = "O mesmo valor de DYNAMODB_TABLE no configmap do serviço"
}
