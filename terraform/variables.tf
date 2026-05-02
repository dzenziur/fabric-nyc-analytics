variable "tenant_id" {
  description = "Microsoft Entra ID tenant ID"
  type        = string
}

variable "client_id" {
  description = "Service Principal application (client) ID"
  type        = string
}

variable "client_secret" {
  description = "Service Principal client secret"
  type        = string
  sensitive   = true
}

variable "workspace_name" {
  description = "Fabric workspace display name"
  type        = string
  default     = "Fabric NYC Analytics"
}

variable "capacity_id" {
  description = "Fabric Trial capacity ID assigned to the workspace"
  type        = string
}
