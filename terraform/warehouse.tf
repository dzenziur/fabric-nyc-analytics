resource "fabric_warehouse" "gold" {
  display_name = "gold_warehouse"
  workspace_id = fabric_workspace.main.id
}
