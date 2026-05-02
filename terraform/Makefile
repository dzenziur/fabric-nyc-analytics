# Terraform Makefile for Fabric NYC Analytics
#
# Run from terraform/ directory, or use `make -C terraform <target>` from repo root.

TF      ?= terraform
TFVARS  ?= terraform.tfvars

.PHONY: help login init fmt validate plan apply apply-auto destroy show output clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-28s\033[0m %s\n", $$1, $$2}'

ifeq ($(OS),Windows_NT)

login:  ## Verify Service Principal credentials (reads from terraform.tfvars)
	powershell -NoProfile -Command \
		"$$c=Get-Content '$(TFVARS)'; \
		$$t=($$c|Where-Object{$$_ -match '^\s*tenant_id\s*='}) -replace '.*\"(.*)\".*','$$1'; \
		$$u=($$c|Where-Object{$$_ -match '^\s*client_id\s*='}) -replace '.*\"(.*)\".*','$$1'; \
		$$p=($$c|Where-Object{$$_ -match '^\s*client_secret\s*='}) -replace '.*\"(.*)\".*','$$1'; \
		az login --service-principal --tenant $$t --username $$u --password $$p --allow-no-subscriptions"

clean:  ## Remove local Terraform cache (.terraform/)
	powershell -NoProfile -Command "Remove-Item -Recurse -Force .terraform -ErrorAction SilentlyContinue"

else

login:  ## Verify Service Principal credentials (reads from terraform.tfvars)
	@TENANT=$$(grep -m1 '^\s*tenant_id\s*=' $(TFVARS)     | sed 's/.*"\(.*\)".*/\1/'); \
	 CLIENT=$$(grep -m1 '^\s*client_id\s*=' $(TFVARS)     | sed 's/.*"\(.*\)".*/\1/'); \
	 SECRET=$$(grep -m1 '^\s*client_secret\s*=' $(TFVARS)  | sed 's/.*"\(.*\)".*/\1/'); \
	 az login --service-principal --tenant "$$TENANT" --username "$$CLIENT" --password "$$SECRET" --allow-no-subscriptions

clean:  ## Remove local Terraform cache (.terraform/)
	rm -rf .terraform/

endif

init:  ## Initialize Terraform (download providers)
	$(TF) init

fmt:  ## Format all .tf files
	$(TF) fmt -recursive

validate:  ## Validate configuration
	$(TF) validate

plan:  ## Show what changes will be applied
	$(TF) plan -var-file=$(TFVARS)

apply:  ## Apply changes
	$(TF) apply -var-file=$(TFVARS)

apply-auto:  ## Apply changes without confirmation prompt
	$(TF) apply -var-file=$(TFVARS) -auto-approve

destroy:  ## Destroy all managed resources (DANGER)
	$(TF) destroy -var-file=$(TFVARS)

show:  ## Show current state
	$(TF) show

output:  ## Show output values
	$(TF) output
