terraform {
  required_version = ">= 1.5"

  required_providers {
    fabric = {
      source  = "microsoft/fabric"
      version = "~> 1.0"
    }
  }
}

provider "fabric" {
  # Service Principal auth (default — works on Fabric Trial and paid SKUs).
  # Run `make login` once to verify credentials, then `make plan/apply`.
  tenant_id     = var.tenant_id
  client_id     = var.client_id
  client_secret = var.client_secret

  # Fallback: Azure CLI auth — uncomment and comment out the three lines above
  # if you don't have a Service Principal (e.g. quick local test).
  # use_cli = true
}
