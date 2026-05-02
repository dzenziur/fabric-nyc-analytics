# Terraform — Fabric Infrastructure

Manages the Microsoft Fabric workspace and storage layers (Bronze/Silver Lakehouses, Gold Warehouse) declaratively.

## Prerequisites

- Terraform >= 1.5
- Azure CLI
- A Service Principal with Workspace Admin role (see Auth modes below)

## First-time setup

```bash
# 1. Copy and fill in variables
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars: tenant_id, client_id, client_secret, capacity_id

# 2. Authenticate
make login

# 3. Initialize
make init

# 4. Apply
make plan
make apply
```

## Common commands

```bash
make help          # list all targets
make login         # verify Service Principal credentials
make plan          # preview changes
make apply         # apply changes
make output        # show resource IDs
make fmt           # format .tf files
make validate      # check syntax
make clean         # remove .terraform/ cache
```

## Auth modes

### Default — Service Principal

`main.tf` uses `tenant_id` / `client_id` / `client_secret` from `terraform.tfvars`.
Works on both Fabric Trial capacity and paid F-SKU.

Setup:
1. Create an App Registration in Azure Entra ID
2. Add the SP as **Admin** directly on the Fabric workspace (Manage Access → Add people or groups)
3. Fill `tenant_id`, `client_id`, `client_secret` in `terraform.tfvars`
4. Run `make login` to verify, then `make apply`

### Fallback — Azure CLI

If you don't have a Service Principal, you can use `az login` as the workspace admin user.
To switch: comment out the SP lines in `main.tf` and uncomment `use_cli = true`.

## What's managed

- `fabric_workspace.main` — workspace `Fabric NYC Analytics`
- `fabric_lakehouse.bronze` — raw data landing
- `fabric_lakehouse.silver` — cleaned Delta tables
- `fabric_warehouse.gold` — analytical star schema

## What's NOT managed (provider limitations)

- Dataflow Gen2 definitions
- Data Factory Pipeline definitions
- Notebook content
- Power BI reports

These are synced via **Fabric Git integration** (Workspace settings → Git) as JSON definitions in this repo.
