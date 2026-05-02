resource "fabric_lakehouse" "bronze" {
  display_name = "bronze_lakehouse"
  workspace_id = fabric_workspace.main.id
}

resource "fabric_lakehouse" "silver" {
  display_name = "silver_lakehouse"
  workspace_id = fabric_workspace.main.id
}
