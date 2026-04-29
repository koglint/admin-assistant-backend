from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
import io
import json
import os
import traceback


import firebase_admin
import pandas as pd
from firebase_admin import auth as firebase_auth
from firebase_admin import credentials, firestore
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "https://koglint.github.io"}})
app.config["CORS_HEADERS"] = "Content-Type"

cred_dict = json.loads(os.environ["FIREBASE_KEY_JSON"])
cred = credentials.Certificate(cred_dict)
firebase_admin.initialize_app(cred)
db = firestore.client()

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
ALLOWED_ADMIN_USERNAMES = {
    "troy.koglin1",
    "gordon.nolan2",
    "david.boscoscuro",
    "peter.hales",
    "janine.neden",
    "jennifer.lynne.lawrence",
    "carly.johnston7",
    "kylie.cutajar4",
    "louise.oneill6",
    "david.baldwin12",
    "nathan.ralstonbryce",
}
ALLOWED_ADMIN_DOMAINS = {
    "det.nsw.edu.au",
    "education.nsw.gov.au",
}
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD")
ADMIN_PURGE_ENABLED = os.environ.get("ADMIN_PURGE_ENABLED", "false").lower() == "true"
@app.route("/")
def home():
    return "Admin Assistant Flask backend is live!"


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"status": "error", "message": "No file part"}), 400

    uploaded_file = request.files["file"]
    if uploaded_file.filename == "":
        return jsonify({"status": "error", "message": "No selected file"}), 400

    try:
        workbook = io.BytesIO(uploaded_file.read())
        try:
            df = pd.read_excel(workbook, engine="xlrd")
        except Exception as xlrd_error:
            workbook.seek(0)
            try:
                df = pd.read_excel(workbook, engine="openpyxl")
            except Exception as openpyxl_error:
                raise ValueError(f"xlrd error: {xlrd_error}; openpyxl error: {openpyxl_error}")
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Excel read failed: {exc}"}), 500

    try:
        normalized_df = normalize_dataframe(df)
        report_rows = build_report_rows(normalized_df)
        report_date = get_report_date(report_rows)
        report_context = build_report_context(report_rows, report_date)
        students_by_id = load_existing_students()
        summary = process_upload(report_rows, students_by_id, report_context)
        return jsonify({"status": "success", **summary})
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Upload processing failed: {exc}"}), 500


@app.route("/admin/purge", methods=["POST"])
def admin_purge():
    payload, error_response = verify_admin_request(require_purge_enabled=True)
    if error_response:
        return error_response

    if payload.get("confirmation", "") != "DELETE":
        return jsonify({"status": "error", "message": "Deletion confirmation text was incorrect."}), 400

    deleted = purge_all_students()
    return jsonify({"status": "success", "deleted": deleted})


@app.route("/admin/authorize", methods=["POST"])
def admin_authorize():
    _, error_response = verify_admin_request(require_purge_enabled=False)
    if error_response:
        return error_response

    return jsonify({
        "status": "success",
        "message": "Admin access granted.",
        "purgeEnabled": ADMIN_PURGE_ENABLED,
    })


def normalize_dataframe(df):
    normalized = df.copy()
    normalized["Date"] = pd.to_datetime(normalized.get("Date"), errors="coerce")
    normalized["Student ID"] = normalized.get("Student ID", "").astype(str).str.strip()
    normalized["Given Name(s)"] = normalized.get("Given Name(s)", "").fillna("").astype(str).str.strip()
    normalized["Surname"] = normalized.get("Surname", "").fillna("").astype(str).str.strip()
    normalized["Roll Class"] = normalized.get("Roll Class", "").fillna("").astype(str).str.strip()
    year_series = get_column_series(normalized, ["School Year", "Year"])
    if year_series is None:
        year_series = ""
    normalized["Year"] = year_series.fillna("").astype(str).str.strip()
    normalized["Shorthand"] = normalized.get("Shorthand", "").fillna("").astype(str).str.strip()
    normalized["Description"] = normalized.get("Description", "").fillna("").astype(str).str.strip()
    normalized["Time"] = normalized.get("Time", "").fillna("").astype(str).str.strip()
    normalized["Comment"] = normalized.get("Comment", "").fillna("").astype(str).str.strip()
    normalized["Explainer"] = normalized.get("Explainer", "").fillna("").astype(str).str.strip()
    normalized["Explainer Source"] = normalized.get("Explainer Source", "").fillna("").astype(str).str.strip()
    return normalized


def build_report_rows(df):
    rows = []
    for _, row in df.iterrows():
        if pd.isna(row["Date"]) or not row["Student ID"]:
            continue

        date_string = row["Date"].strftime("%Y-%m-%d")
        time_range = row["Time"]
        start_text, end_text = split_time_range(time_range)
        rows.append({
            "studentId": row["Student ID"],
            "givenName": row["Given Name(s)"],
            "surname": row["Surname"],
            "rollClass": row["Roll Class"],
            "yearGroup": normalize_year_group_text(row["Year"]),
            "date": date_string,
            "shorthand": row["Shorthand"],
            "description": row["Description"],
            "timeRange": time_range,
            "timeStart": start_text,
            "timeEnd": end_text,
            "comment": row["Comment"],
            "explainer": row["Explainer"],
            "explainerSource": row["Explainer Source"],
        })
    return rows


def load_existing_students():
    return {
        doc_snapshot.id: doc_snapshot.to_dict()
        for doc_snapshot in db.collection("students").stream()
    }


def process_upload(report_rows, students_by_id, report_context):
    report_date = report_context.get("reportDate")
    late_rows = [row for row in report_rows if is_roll_call_late(row)]
    attendance_day_records = build_attendance_day_records(report_rows)

    write_attendance_day_records(attendance_day_records)

    new_late_count = 0
    detention_assigned_count = 0
    detention_checks_completed = 0

    late_rows_by_student = {}
    for late_row in late_rows:
        late_rows_by_student.setdefault(late_row["studentId"], []).append(late_row)

    for student_id, rows_for_student in late_rows_by_student.items():
        late_added, detentions_assigned = apply_late_rows_transaction(
            student_id,
            rows_for_student
        )
        new_late_count += late_added
        detention_assigned_count += detentions_assigned

    if report_date and report_context.get("coversFullDay"):
        for student_id in students_by_id.keys():
            if apply_pending_detention_transaction(
                student_id,
                attendance_day_records.get((student_id, report_date)),
                report_date
            ):
                detention_checks_completed += 1

    pending_detention_checks = count_pending_detention_checks(report_date) if report_date else 0

    return {
        "added": new_late_count,
        "detentionsAssigned": detention_assigned_count,
        "detentionChecksCompleted": detention_checks_completed,
        "pendingDetentionChecks": pending_detention_checks,
        "reportDate": report_date,
        "coversFullDay": report_context.get("coversFullDay", False),
        "latestObservedTime": report_context.get("latestObservedTime"),
    }


def clone_student(existing_student, source_row=None):
    if existing_student:
        student = dict(existing_student)
        if source_row:
            if source_row.get("givenName"):
                student["givenName"] = source_row.get("givenName", student.get("givenName", ""))
            if source_row.get("surname"):
                student["surname"] = source_row.get("surname", student.get("surname", ""))
            if source_row.get("rollClass"):
                student["rollClass"] = source_row.get("rollClass", student.get("rollClass", ""))
            if source_row.get("yearGroup"):
                student["yearGroup"] = source_row.get("yearGroup", student.get("yearGroup", ""))
        student["lateArrivals"] = list(existing_student.get("lateArrivals", existing_student.get("truancies", [])))
        student["truancies"] = list(student["lateArrivals"])
        student["detentionHistory"] = list(existing_student.get("detentionHistory", []))
        student["escalationReasons"] = list(existing_student.get("escalationReasons", []))
        student["lastEscalationReasons"] = list(existing_student.get("lastEscalationReasons", existing_student.get("escalationReasons", [])))
        student["escalationCause"] = existing_student.get("escalationCause", "")
        student["lastEscalationCause"] = existing_student.get("lastEscalationCause", existing_student.get("escalationCause", ""))
        student["activeDetention"] = dict(existing_student.get("activeDetention", {})) if existing_student.get("activeDetention") else None
        student["escalationSuppression"] = dict(existing_student.get("escalationSuppression", {}))
        if not student["activeDetention"] and existing_student.get("truancyResolved") is False and student["lateArrivals"]:
            latest_late = sorted(student["lateArrivals"], key=lambda item: item.get("date", ""), reverse=True)[0]
            student["activeDetention"] = {
                "status": "open",
                "createdFromLateDate": latest_late.get("date"),
                "scheduledForDate": latest_late.get("date"),
                "sourceContext": "legacy_migration",
                "createdAt": datetime.now(SYDNEY_TZ).isoformat(),
                "lastRollMark": None,
                "lastRollMarkedAt": None,
                "pendingAttendanceCheckDate": None,
                "missedWhilePresentCount": 0,
            }
        return student

    return {
        "givenName": source_row.get("givenName", "") if source_row else "",
        "surname": source_row.get("surname", "") if source_row else "",
        "rollClass": source_row.get("rollClass", "") if source_row else "",
        "yearGroup": source_row.get("yearGroup", "") if source_row else "",
        "lateArrivals": [],
        "truancies": [],
        "lateCount": 0,
        "truancyCount": 0,
        "detentionsServed": 0,
        "detentionHistory": [],
        "activeDetention": None,
        "escalated": False,
        "escalationReasons": [],
        "lastEscalationReasons": [],
        "escalationCause": "",
        "lastEscalationCause": "",
        "manualEscalation": False,
        "escalationSuppression": {
            "lateCountUntil": 0,
            "missedCountUntil": 0,
        },
        "notes": "",
    }


def add_late_arrival(student, late_row):
    existing_late_arrivals = student.get("lateArrivals", [])
    if any(entry.get("date") == late_row["date"] for entry in existing_late_arrivals):
        return False

    arrival_time_text = late_row["timeEnd"] or ""
    arrival_dt = parse_time_value(arrival_time_text)
    minutes_late = None
    if arrival_dt:
        scheduled = datetime.combine(datetime.today(), time(8, 35))
        minutes_late = max(0, int((arrival_dt - scheduled).total_seconds() // 60))

    existing_late_arrivals.append({
        "date": late_row["date"],
        "description": late_row["description"],
        "comment": late_row["comment"],
        "justified": False,
        "resolved": False,
        "explainer": late_row["explainer"],
        "explainerSource": late_row["explainerSource"],
        "detentionIssued": False,
        "arrivalTime": arrival_time_text or None,
        "minutesLate": minutes_late,
        "shorthand": late_row["shorthand"],
        "timeRange": late_row["timeRange"],
        "yearGroup": late_row.get("yearGroup"),
    })
    existing_late_arrivals.sort(key=lambda item: item["date"], reverse=True)

    student["lateArrivals"] = existing_late_arrivals
    student["truancies"] = existing_late_arrivals
    student["lateCount"] = len(existing_late_arrivals)
    student["truancyCount"] = len(existing_late_arrivals)
    return True


def apply_late_rows_transaction(student_id, late_rows):
    transaction = db.transaction()
    student_ref = db.collection("students").document(student_id)

    @firestore.transactional
    def _apply(transaction_obj):
        snapshot = next(transaction_obj.get(student_ref), None)
        student = clone_student(snapshot.to_dict() if snapshot and snapshot.exists else None, late_rows[0])
        original_student = snapshot.to_dict() if snapshot and snapshot.exists else None
        late_added = 0
        detentions_assigned = 0

        for late_row in sorted(late_rows, key=lambda row: (row["date"], row.get("timeEnd") or "")):
            if add_late_arrival(student, late_row):
                late_added += 1
                if should_assign_detention(student):
                    scheduled_date = determine_detention_date(late_row)
                    assign_detention(student, late_row, scheduled_date)
                    detentions_assigned += 1

        update_student_status(student)
        if late_added == 0 and detentions_assigned == 0 and not student_identity_changed(original_student, student):
            return 0, 0

        apply_audit_fields(student, "backend_upload_sync", "backend_upload")
        transaction_obj.set(student_ref, student)
        return late_added, detentions_assigned

    return _apply(transaction)


def apply_pending_detention_transaction(student_id, attendance_day_record, report_date):
    transaction = db.transaction()
    student_ref = db.collection("students").document(student_id)

    @firestore.transactional
    def _apply(transaction_obj):
        snapshot = next(transaction_obj.get(student_ref), None)
        if not snapshot or not snapshot.exists:
            return False

        student = clone_student(snapshot.to_dict())
        if not evaluate_pending_detention(student, attendance_day_record, report_date):
            return False

        update_student_status(student)
        apply_audit_fields(student, "backend_detention_attendance_evaluated", "backend_upload")
        transaction_obj.set(student_ref, student)
        return True

    return _apply(transaction)


def count_pending_detention_checks(report_date):
    pending_count = 0
    for student_snapshot in db.collection("students").stream():
        active_detention = student_snapshot.to_dict().get("activeDetention") or {}
        if active_detention.get("pendingAttendanceCheckDate") == report_date:
            pending_count += 1
    return pending_count


def apply_audit_fields(student, action, actor):
    student["updatedAt"] = firestore.SERVER_TIMESTAMP
    student["updatedBy"] = actor
    student["lastAction"] = action


def student_identity_changed(original_student, updated_student):
    if not original_student:
        return True

    for key in ("givenName", "surname", "rollClass", "yearGroup"):
        if (original_student.get(key) or "") != (updated_student.get(key) or ""):
            return True

    return False


def should_assign_detention(student):
    active_detention = student.get("activeDetention")
    return not active_detention or active_detention.get("status") != "open"


def assign_detention(student, late_row, scheduled_date):
    student["activeDetention"] = {
        "status": "open",
        "createdFromLateDate": late_row["date"],
        "scheduledForDate": scheduled_date,
        "sourceContext": build_detention_source_context(late_row, scheduled_date),
        "createdAt": datetime.now(SYDNEY_TZ).isoformat(),
        "lastRollMark": None,
        "lastRollMarkedAt": None,
        "pendingAttendanceCheckDate": None,
        "missedWhilePresentCount": 0,
    }


def evaluate_pending_detention(student, attendance_day_record, report_date):
    active_detention = student.get("activeDetention")
    if not active_detention or active_detention.get("status") != "open":
        return False

    if active_detention.get("pendingAttendanceCheckDate") != report_date:
        return False

    if not attendance_day_record or not attendance_day_record.get("hasFullDayCoverage"):
        return False

    present_at_school = attendance_day_record.get("presentAtSchool", True)
    active_detention["lastEvaluatedDate"] = report_date
    active_detention["pendingAttendanceCheckDate"] = None

    if present_at_school:
        active_detention["missedWhilePresentCount"] = active_detention.get("missedWhilePresentCount", 0) + 1
        student.setdefault("detentionHistory", []).append({
            "date": report_date,
            "scheduledForDate": report_date,
            "outcome": "missed_while_present",
        })
    else:
        student.setdefault("detentionHistory", []).append({
            "date": report_date,
            "scheduledForDate": report_date,
            "outcome": "absent_from_school",
        })

    active_detention["scheduledForDate"] = next_school_day(report_date)
    student["activeDetention"] = active_detention
    return True


def update_student_status(student):
    active_detention = student.get("activeDetention") or {}
    active_detention_open = active_detention.get("status") == "open"
    late_count = student.get("lateCount", len(student.get("lateArrivals", [])))
    missed_count = active_detention.get("missedWhilePresentCount", 0)
    suppression = student.get("escalationSuppression", {})

    reasons = []
    if student.get("manualEscalation"):
        reasons.append("manual_escalation")
    if late_count > 5 and late_count > suppression.get("lateCountUntil", 0):
        reasons.append("late_count_over_five")
    if active_detention_open and missed_count >= 2 and missed_count > suppression.get("missedCountUntil", 0):
        reasons.append("missed_detention_twice")

    cause_text = format_escalation_reasons(reasons)
    student["escalationReasons"] = reasons
    student["escalationCause"] = cause_text
    student["escalated"] = bool(reasons)
    if reasons:
        student["lastEscalationReasons"] = list(reasons)
        student["lastEscalationCause"] = cause_text
    student["truancyResolved"] = not active_detention_open
    student["activeDetention"] = active_detention if active_detention_open else None


def format_escalation_reasons(reasons):
    labels = {
        "manual_escalation": "Manual escalation",
        "late_count_over_five": "More than five late arrivals",
        "missed_detention_twice": "Missed detention twice while present at school",
    }

    if not reasons:
        return ""

    return ", ".join(labels.get(reason, reason) for reason in reasons)


def write_students(changed_students):
    if not changed_students:
        return

    batch = db.batch()
    batch_size = 0
    for student_id, student in changed_students.items():
        doc_ref = db.collection("students").document(student_id)
        batch.set(doc_ref, student)
        batch_size += 1
        if batch_size >= 450:
            batch.commit()
            batch = db.batch()
            batch_size = 0

    if batch_size:
        batch.commit()


def group_rows_by_student_and_date(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault((row["studentId"], row["date"]), []).append(row)
    return grouped


def build_attendance_day_records(rows):
    records = {}
    for (student_id, date_string), student_rows in group_rows_by_student_and_date(rows).items():
        latest_observed = get_latest_observed_time(student_rows)
        records[(student_id, date_string)] = {
            "studentId": student_id,
            "date": date_string,
            "presentAtSchool": determine_present_at_school(student_rows),
            "hasFullDayCoverage": latest_observed is not None and latest_observed >= time(14, 45),
            "latestObservedTime": latest_observed.strftime("%I:%M%p") if latest_observed else None,
            "rowCount": len(student_rows),
        }
    return records


def write_attendance_day_records(attendance_day_records):
    if not attendance_day_records:
        return

    batch = db.batch()
    batch_size = 0

    for record in attendance_day_records.values():
        doc_id = build_attendance_day_doc_id(record["studentId"], record["date"])
        doc_ref = db.collection("attendance_days").document(doc_id)
        batch.set(doc_ref, {
            **record,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }, merge=True)
        batch_size += 1

        if batch_size >= 450:
            batch.commit()
            batch = db.batch()
            batch_size = 0

    if batch_size:
        batch.commit()


def build_attendance_day_doc_id(student_id, date_string):
    return f"{student_id}_{date_string}"


def determine_present_at_school(student_rows_for_date):
    if not student_rows_for_date:
        return True

    for row in student_rows_for_date:
        if is_roll_call_late(row):
            return True
        if not is_full_day_absence_row(row):
            return True

    return False


def is_full_day_absence_row(row):
    start_text, end_text = row["timeStart"], row["timeEnd"]
    if not row["timeRange"]:
        return True

    start_is_morning = start_text in {"8:00AM", "8:25AM"}
    end_is_end_of_day = end_text in {"2:45PM", "02:45PM", "14:45"}
    return start_is_morning and end_is_end_of_day


def is_roll_call_late(row):
    shorthand = row["shorthand"].strip().upper()
    description = row["description"].strip().upper()
    time_start = row["timeStart"]
    if time_start not in {"8:00AM", "8:25AM"}:
        return False

    return (
        (shorthand == "U" and description == "UNJUSTIFIED")
        or (shorthand == "?" and description == "ABSENT")
    )


def split_time_range(time_range):
    if not time_range or "-" not in time_range:
        return "", ""

    left, right = time_range.split("-", 1)
    return normalize_time_text(left), normalize_time_text(right)


def normalize_time_text(value):
    return str(value or "").strip().replace(" ", "").upper()


def normalize_year_group_text(value):
    text = str(value or "").strip()
    if not text:
        return ""

    if text.endswith(".0"):
        text = text[:-2]

    digits = "".join(ch for ch in text if ch.isdigit())
    return digits or text


def get_column_series(df, column_names):
    normalized_lookup = {
        normalize_column_name(column): column
        for column in df.columns
    }

    for candidate in column_names:
        actual_name = normalized_lookup.get(normalize_column_name(candidate))
        if actual_name is not None:
            return df[actual_name]

    return None


def normalize_column_name(value):
    return "".join(str(value or "").strip().lower().split())


def parse_time_value(value):
    cleaned = normalize_time_text(value)
    if not cleaned:
        return None

    for fmt in ("%I:%M%p", "%H:%M"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            return datetime.combine(datetime.today(), parsed.time())
        except ValueError:
            continue
    return None


def next_school_day(date_string):
    date_value = datetime.strptime(date_string, "%Y-%m-%d").date()
    next_day = date_value + timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    return next_day.strftime("%Y-%m-%d")


def get_report_date(report_rows):
    dates = sorted({row["date"] for row in report_rows})
    return dates[-1] if dates else None


def build_report_context(report_rows, report_date):
    latest_observed = None
    if report_date:
        latest_observed = get_latest_observed_time(
            [row for row in report_rows if row["date"] == report_date]
        )

    covers_full_day = latest_observed is not None and latest_observed >= time(14, 45)

    return {
        "reportDate": report_date,
        "coversFullDay": covers_full_day,
        "latestObservedTime": latest_observed.strftime("%I:%M%p") if latest_observed else None,
    }


def get_latest_observed_time(rows):
    latest_observed = None
    for row in rows:
        for value in (row["timeStart"], row["timeEnd"]):
            parsed = parse_time_value(value)
            if parsed and (latest_observed is None or parsed.time() > latest_observed):
                latest_observed = parsed.time()
    return latest_observed


def determine_detention_date(late_row):
    arrival_dt = parse_time_value(late_row.get("timeEnd"))
    if not arrival_dt:
        return next_school_day(late_row["date"])

    if arrival_dt.time() < first_break_start_for_date(late_row["date"]):
        return late_row["date"]

    return next_school_day(late_row["date"])


def first_break_start_for_date(date_string):
    date_value = datetime.strptime(date_string, "%Y-%m-%d").date()
    if date_value.weekday() in {1, 3}:
        return time(10, 25)
    return time(10, 35)


def build_detention_source_context(late_row, scheduled_date):
    if scheduled_date == late_row["date"]:
        return "auto_same_day_before_first_break"
    return "auto_next_school_day_after_first_break"


def verify_admin_request(require_purge_enabled):
    if not ADMIN_PASSWORD:
        return None, (jsonify({"status": "error", "message": "ADMIN_PASSWORD is not configured on the server."}), 500)

    if require_purge_enabled and not ADMIN_PURGE_ENABLED:
        return None, (jsonify({"status": "error", "message": "Purge is disabled on the server."}), 403)

    token = extract_bearer_token(request.headers.get("Authorization"))
    if not token:
        return None, (jsonify({"status": "error", "message": "Missing authorization token."}), 401)

    try:
        decoded_token = firebase_auth.verify_id_token(token)
    except Exception:
        traceback.print_exc()
        return None, (jsonify({"status": "error", "message": "Invalid authentication token."}), 401)

    email = decoded_token.get("email", "").lower()
    if not decoded_token.get("email_verified", False):
        return None, (jsonify({"status": "error", "message": "Email address is not verified."}), 403)

    if not is_allowed_admin_email(email):
        return None, (jsonify({"status": "error", "message": "This account is not allowed to use admin controls."}), 403)

    payload = request.get_json(silent=True) or {}
    if payload.get("password", "") != ADMIN_PASSWORD:
        return None, (jsonify({"status": "error", "message": "Admin password was incorrect."}), 403)

    return payload, None


def is_allowed_admin_email(email):
    parts = email.split("@", 1)
    if len(parts) != 2:
        return False

    username, domain = parts
    return username in ALLOWED_ADMIN_USERNAMES and domain in ALLOWED_ADMIN_DOMAINS


def extract_bearer_token(header_value):
    if not header_value:
        return None

    parts = header_value.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None

    return parts[1].strip()


def purge_all_students():
    students = list(db.collection("students").stream())
    deleted = 0
    batch = db.batch()
    batch_size = 0

    for student_doc in students:
        batch.delete(student_doc.reference)
        batch_size += 1
        deleted += 1

        if batch_size >= 450:
            batch.commit()
            batch = db.batch()
            batch_size = 0

    if batch_size:
        batch.commit()

    return deleted


@app.errorhandler(Exception)
def handle_exception(exc):
    traceback.print_exc()
    return jsonify({"status": "error", "message": str(exc)}), 500
