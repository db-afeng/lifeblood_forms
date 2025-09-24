"""Create the Lifeblood equipment checks table inside the configured Unity Catalog schema."""

import os
from pyspark.sql import SparkSession

CATALOG_NAME = os.getenv("CHECKS_CATALOG", "alex_feng")
SCHEMA_NAME = os.getenv("CHECKS_SCHEMA", "lifeblood_checks")
TABLE_NAME = os.getenv("CHECKS_TABLE", "lifeblood_equipment_checks")


def qualify(identifier: str) -> str:
    return f"`{identifier}`"


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    catalog_identifier = qualify(CATALOG_NAME)
    schema_identifier = f"{catalog_identifier}.{qualify(SCHEMA_NAME)}"
    table_identifier = f"{schema_identifier}.{qualify(TABLE_NAME)}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_identifier}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_identifier} (
            inspection_date DATE COMMENT 'Date the routine inspection took place',
            facility_name STRING COMMENT 'Facility or donor centre where the equipment resides',
            nurse_name STRING COMMENT 'Nurse or technician performing the inspection',
            machine_type STRING COMMENT 'Type of device or instrument inspected',
            machine_id STRING COMMENT 'Unique identifier or serial number for the device',
            room_location STRING COMMENT 'Location of the equipment within the facility',
            power_status STRING COMMENT 'Power supply check result (e.g., OK, Needs Attention)',
            alarms_functional BOOLEAN COMMENT 'Indicates whether alarms passed functional testing',
            calibration_due_date DATE COMMENT 'Next calibration due date noted on the device',
            calibration_confirmed BOOLEAN COMMENT 'Whether calibration records were verified during the inspection',
            temperature_celsius DOUBLE COMMENT 'Operating temperature recorded in Celsius',
            pressure_kpa DOUBLE COMMENT 'Operating pressure recorded in kilopascals',
            cleaning_status STRING COMMENT 'Cleanliness and disinfection status of the equipment',
            issues_noted STRING COMMENT 'Summary of issues or observations captured during the inspection',
            follow_up_required BOOLEAN COMMENT 'Indicates if follow-up action is required',
            follow_up_actions STRING COMMENT 'Details of planned follow-up actions or responsible parties',
            user_email STRING COMMENT 'Email address of the Databricks user submitting the form',
            submitted_at TIMESTAMP COMMENT 'Timestamp recorded when the submission was stored'
        )
        USING DELTA
        COMMENT 'Routine equipment inspection submissions from Lifeblood nursing teams.'
        TBLPROPERTIES (
            delta.autoOptimize.optimizeWrite = true,
            delta.autoOptimize.autoCompact = true
        )
        """
    )

    print(f"Verified table exists: {table_identifier}")


if __name__ == "__main__":
    main()
