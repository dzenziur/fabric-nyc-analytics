output "workspace_id" {
  description = "Fabric workspace ID"
  value       = fabric_workspace.main.id
}

output "workspace_name" {
  description = "Fabric workspace display name"
  value       = fabric_workspace.main.display_name
}

output "bronze_lakehouse_id" {
  description = "Bronze lakehouse ID"
  value       = fabric_lakehouse.bronze.id
}

output "silver_lakehouse_id" {
  description = "Silver lakehouse ID"
  value       = fabric_lakehouse.silver.id
}

output "gold_warehouse_id" {
  description = "Gold warehouse ID"
  value       = fabric_warehouse.gold.id
}
