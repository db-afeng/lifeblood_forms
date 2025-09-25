# Lifeblood Equipment Check App

This bundle deploys a Databricks App that replaces manual Lifeblood/Red Cross equipment inspection forms with a Streamlit UI backed by Unity Catalog tables.

## Components
- Streamlit Databricks App (`resources/apps/lifeblood_equipment_checks.app.yml`) for nurses to enter inspections.
- Serverless setup job (`resources/jobs/lifeblood_table_setup.job.yml`) that creates the managed Delta table.
- Unity Catalog schema definition (`resources/schema/lifeblood_checks.schema.yml`).

## Prerequisites
1. [Databricks CLI v0.270 or later](https://docs.databricks.com/dev-tools/cli/index.html).
2. Workspace authentication (e.g. `databricks auth login`).
3. A SQL warehouse the app can use.

## Configuration
Export the SQL warehouse HTTP path so the bundle can deliver it to the app:

```bash
export BUNDLE_VAR_warehouse_http_path=sql/protocolv1/o/<workspace-id>/<warehouse-id>
```
(Optional) Override the catalog or schema by exporting environment variables before deploying:

```bash
export BUNDLE_VAR_catalog_name=<catalog_name> # Needs to exist already
```

## Deployment & Execution
1. Deploy the bundle (dev is the default target):
   ```bash
   databricks bundle deploy --target dev
   ```
2. Run the setup job once to create or verify the Delta table:
   ```bash
   databricks bundle run lifeblood_table_setup_job --target dev
   ```
3. Launch the Streamlit app from the CLI to verify it starts:
   ```bash
   databricks bundle run lifeblood_equipment_checks_app --target dev
   ```
   (You can also open the app from the Databricks workspace Apps UI.)

## Usage
1. Navigate to the deployed Databricks App (`lifeblood-equipment-checks`).
2. Complete the inspection form and submit; each record writes to `<catalog_name>.lifeblood_checks.lifeblood_equipment_checks` with the logged-in email.
3. Use the "Recent submissions" table for quick confirmation, or query the Delta table directly in SQL.
