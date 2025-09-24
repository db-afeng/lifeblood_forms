import os
import time
from datetime import date
from typing import Dict, List, Optional, Sequence, Tuple
from databricks.sdk import WorkspaceClient


import pandas as pd
import streamlit as st
from databricks.sdk.service.sql import (
    StatementParameterListItem,
    StatementResponse,
    StatementState,
)

CATALOG_NAME = os.getenv("CHECKS_CATALOG", "alex_feng")
SCHEMA_NAME = os.getenv("CHECKS_SCHEMA", "lifeblood_checks")
TABLE_NAME = os.getenv("CHECKS_TABLE", "lifeblood_equipment_checks")
WAREHOUSE_HTTP_PATH = os.getenv("DATABRICKS_WAREHOUSE_HTTP_PATH", "").strip()

st.set_page_config(
    page_title="Lifeblood Equipment Checks",
    page_icon="🩸",
    layout="wide",
)

if not WAREHOUSE_HTTP_PATH:
    st.error(
        "Missing SQL warehouse URL. Set the DATABRICKS_WAREHOUSE_HTTP_PATH bundle "
        "variable before deploying the app."
    )
    st.stop()

WAREHOUSE_ID = WAREHOUSE_HTTP_PATH.rstrip("/").split("/")[-1]


@st.cache_resource
def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


WORKSPACE_CLIENT = get_workspace_client()


def get_current_user_email() -> Optional[str]:
    """Attempt to resolve the email for the currently authenticated user."""

    try:
        profile = WORKSPACE_CLIENT.current_user.me()
        if profile and getattr(profile, "emails", None):
            emails = profile.emails
            try:
                for entry in emails:
                    value = getattr(entry, "value", None) or entry.get("value")  # type: ignore[arg-type]
                    primary = getattr(entry, "primary", None) or entry.get("primary")  # type: ignore[arg-type]
                    if value and (primary or primary is None):
                        return value
            except AttributeError:
                pass
            first = emails[0]
            return getattr(first, "value", None) or first.get("value")  # type: ignore[arg-type]
    except Exception:
        pass

    try:
        from databricks.sdk.runtime import dbutils  # type: ignore

        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        for accessor in (
            ctx.userName,
            lambda: ctx.tags().get("userEmail"),
            lambda: ctx.tags().get("user"),
        ):
            try:
                opt = accessor()
                if opt and opt.isDefined():
                    return opt.get()
            except Exception:
                continue
    except Exception:
        pass

    return os.getenv("DATABRICKS_USER") or os.getenv("USER")


def bool_to_str(value: bool) -> str:
    return "true" if value else "false"


def execute_sql(statement: str, parameters: Sequence[StatementParameterListItem]) -> Tuple[str, StatementResponse]:
    response = WORKSPACE_CLIENT.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=WAREHOUSE_ID,
        catalog=CATALOG_NAME,
        schema=SCHEMA_NAME,
        parameters=list(parameters),
        wait_timeout="5s",
    )

    current = response
    status = current.status

    # Poll until the SQL warehouse finishes executing the statement.
    while status and status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(0.2)
        current = WORKSPACE_CLIENT.statement_execution.get_statement(
            statement_id=current.statement_id  # type: ignore[arg-type]
        )
        status = current.status

    if not status or status.state != StatementState.SUCCEEDED:
        message = None
        if status and status.error:
            message = status.error.message
        raise RuntimeError(message or "SQL execution failed")

    return current.statement_id, current


def fetch_recent_submissions(limit: int = 20) -> Optional[pd.DataFrame]:
    query = f"""
        SELECT
            inspection_date,
            facility_name,
            machine_type,
            machine_id,
            power_status,
            follow_up_required,
            submitted_at,
            user_email
        FROM {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}
        ORDER BY submitted_at DESC
        LIMIT {limit}
    """

    try:
        statement_id, response = execute_sql(query, [])
    except Exception:
        return None

    manifest = response.manifest
    columns: List[str] = []
    if manifest and manifest.schema and manifest.schema.columns:
        columns = [col.name or "" for col in manifest.schema.columns]

    data = []
    if response.result and response.result.data_array:
        data = response.result.data_array
    else:
        chunk = WORKSPACE_CLIENT.statement_execution.get_statement_result_chunk_n(
            statement_id=statement_id,
            chunk_index=0,
        )
        data = chunk.data_array or []

    return pd.DataFrame(data, columns=columns)


def insert_submission(payload: Dict[str, str]) -> None:
    statement = f"""
        INSERT INTO {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME} (
            inspection_date,
            facility_name,
            nurse_name,
            machine_type,
            machine_id,
            room_location,
            power_status,
            alarms_functional,
            calibration_due_date,
            calibration_confirmed,
            temperature_celsius,
            pressure_kpa,
            cleaning_status,
            issues_noted,
            follow_up_required,
            follow_up_actions,
            user_email,
            submitted_at
        ) VALUES (
            to_date(:inspection_date),
            :facility_name,
            :nurse_name,
            :machine_type,
            :machine_id,
            :room_location,
            :power_status,
            CASE WHEN lower(:alarms_functional) = 'true' THEN TRUE ELSE FALSE END,
            CASE WHEN :calibration_due_date = '' THEN NULL ELSE to_date(:calibration_due_date) END,
            CASE WHEN lower(:calibration_confirmed) = 'true' THEN TRUE ELSE FALSE END,
            TRY_CAST(NULLIF(:temperature_celsius, '') AS DOUBLE),
            TRY_CAST(NULLIF(:pressure_kpa, '') AS DOUBLE),
            :cleaning_status,
            :issues_noted,
            CASE WHEN lower(:follow_up_required) = 'true' THEN TRUE ELSE FALSE END,
            :follow_up_actions,
            :user_email,
            current_timestamp()
        )
    """

    params = [
        StatementParameterListItem(name=name, value=value)
        for name, value in payload.items()
    ]

    execute_sql(statement, params)


user_email = get_current_user_email()

st.title("Lifeblood Equipment Check Form")
st.caption(
    "Use this form to replace the paper checklist and capture machine and instrument "
    "inspections directly in Databricks."
)

with st.expander("Inspection guidance", expanded=False):
    st.markdown(
        "- Verify alarms and safety features before recording environmental readings.\n"
        "- Use the follow-up fields to document any remediation or escalation actions.\n"
        "- Leave numeric readings blank if not applicable to the device."
    )

if user_email:
    st.info(f"Logged in as: {user_email}")
else:
    st.warning(
        "Unable to resolve the current user email automatically; submissions will still be "
        "recorded but the email column may be empty."
    )

machine_types = [
    "Apheresis Machine",
    "Blood Fridge",
    "Blood Pressure Monitor",
    "Centrifuge",
    "Defibrillator",
    "Infusion Pump",
    "Scale",
    "Other",
]

power_status_options = ["OK", "Needs Attention", "Out of Service"]
cleaning_status_options = ["Sanitized", "Needs Cleaning", "Not Applicable"]

with st.form("lifeblood-equipment-check-form"):
    col1, col2 = st.columns(2)

    with col1:
        inspection_date = st.date_input("Inspection date", value=date.today())
        facility_name = st.text_input("Facility / donor centre*", placeholder="e.g. Sydney Processing Centre")
        nurse_name = st.text_input("Inspector name*", placeholder="e.g. Alex Smith")
        machine_type = st.selectbox("Machine or instrument*", options=machine_types)
        machine_id = st.text_input("Equipment ID / serial*", placeholder="e.g. FX-2041")
        room_location = st.text_input("Location / room*", placeholder="e.g. Donor Room 3")
        power_status = st.selectbox("Power status*", options=power_status_options)
        alarms_functional = st.toggle("Alarms functional?", value=True)
        calibration_confirmed = st.toggle("Calibration sticker verified?", value=True)

    with col2:
        calibration_due_raw = st.text_input(
            "Next calibration due (YYYY-MM-DD)",
            placeholder="Leave blank if not recorded",
        )
        temperature_raw = st.text_input(
            "Operating temperature (°C)",
            placeholder="e.g. 4.5",
        )
        pressure_raw = st.text_input(
            "Operating pressure (kPa)",
            placeholder="e.g. 101.3",
        )
        cleaning_status = st.selectbox("Cleaning status", options=cleaning_status_options)
        issues_noted = st.text_area(
            "Issues / observations",
            placeholder="Describe any defects, alarms, or maintenance performed",
            height=120,
        )
        follow_up_required = st.toggle("Follow-up required?")
        follow_up_actions = st.text_area(
            "Follow-up owner / actions",
            placeholder="Assign actions, due dates, or escalation details",
            height=120,
        )

    submitted = st.form_submit_button("Submit inspection", use_container_width=True)

if submitted:
    errors: List[str] = []

    required_fields = {
        "Facility name": facility_name,
        "Inspector name": nurse_name,
        "Equipment ID": machine_id,
        "Location": room_location,
    }

    for label, value in required_fields.items():
        if not value.strip():
            errors.append(f"{label} is required.")

    calibration_due_value: Optional[str] = None
    if calibration_due_raw.strip():
        try:
            calibration_due_value = date.fromisoformat(calibration_due_raw.strip()).isoformat()
        except ValueError:
            errors.append("Calibration due date must follow YYYY-MM-DD format.")

    temperature_value = ""
    if temperature_raw.strip():
        try:
            temperature_value = str(float(temperature_raw.strip()))
        except ValueError:
            errors.append("Temperature must be recorded as a number (e.g. 4.5).")

    pressure_value = ""
    if pressure_raw.strip():
        try:
            pressure_value = str(float(pressure_raw.strip()))
        except ValueError:
            errors.append("Pressure must be recorded as a number (e.g. 101.3).")

    if follow_up_required and not follow_up_actions.strip():
        errors.append("Provide follow-up actions when follow-up is marked as required.")

    if errors:
        for message in errors:
            st.error(message)
    else:
        payload = {
            "inspection_date": inspection_date.isoformat(),
            "facility_name": facility_name.strip(),
            "nurse_name": nurse_name.strip(),
            "machine_type": machine_type,
            "machine_id": machine_id.strip(),
            "room_location": room_location.strip(),
            "power_status": power_status,
            "alarms_functional": bool_to_str(alarms_functional),
            "calibration_due_date": calibration_due_value or "",
            "calibration_confirmed": bool_to_str(calibration_confirmed),
            "temperature_celsius": temperature_value,
            "pressure_kpa": pressure_value,
            "cleaning_status": cleaning_status,
            "issues_noted": issues_noted.strip(),
            "follow_up_required": bool_to_str(follow_up_required),
            "follow_up_actions": follow_up_actions.strip(),
            "user_email": (user_email or ""),
        }

        with st.spinner("Recording inspection..."):
            try:
                insert_submission(payload)
            except Exception as exc:
                st.error(f"Unable to save inspection: {exc}")
            else:
                st.success("Inspection submitted successfully.")
                st.session_state["submitted_at"] = time.time()

st.divider()

if st.session_state.get("submitted_at"):
    st.toast("Latest inspection saved to the warehouse.", icon="✅")
    st.session_state.pop("submitted_at", None)

recent = fetch_recent_submissions()

if recent is None or recent.empty:
    st.info("No equipment inspections have been recorded yet.")
else:
    st.subheader("Recent submissions")
    st.dataframe(recent, use_container_width=True, hide_index=True)
