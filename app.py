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
ALLOWED_ADMIN_EMAILS = {
    "troy.koglin1@det.nsw.edu.au",
    "troy.koglin1@education.nsw.gov.au",
}
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD")
ADMIN_PURGE_ENABLED = os.environ.get("ADMIN_PURGE_ENABLED", "false").lower() == "true"
UPLOAD_TYPES = {"midday", "end_of_day"}


@app.route("/")
def home():
    return "Admin Assistant Flask backend is live!"


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"status": "error", "message": "No file part"}), 400

    upload_type = request.form.get("uploadType", "").strip().lower()
    if upload_type not in UPLOAD_TYPES:
        return jsonify({"status": "error", "message": "Upload type must be midday or end_of_day."}), 400

    uploaded_file = request.files["file"]
    if uploaded_file.filename == "":
        return jsonify({"status": "error", "message": "No selected file"}), 400

    warning = get_upload_time_warning(upload_type)

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
        students_by_id = load_existing_students()
        summary = process_upload(report_rows, students_by_id, upload_type)
        if warning:
            summary["warning"] = warning
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


def process_upload(report_rows, students_by_id, upload_type):
    latest_rows_by_student_date = {}
    for row in report_rows:
        latest_rows_by_student_date[(row["studentId"], row["date"])] = row

    late_rows = [row for row in report_rows if is_roll_call_late(row)]
    grouped_absences = group_rows_by_student_and_date(report_rows)

    changed_students = {}
    new_late_count = 0
    detention_assigned_count = 0

    for late_row in late_rows:
        student_id = late_row["studentId"]
        student = changed_students.get(student_id) or clone_student(students_by_id.get(student_id), late_row)

        if add_late_arrival(student, late_row, upload_type):
            new_late_count += 1

            if should_assign_detention(student):
                scheduled_date = late_row["date"] if upload_type == "midday" else next_school_day(late_row["date"])
                assign_detention(student, late_row, upload_type, scheduled_date)
                detention_assigned_count += 1

        update_student_status(student)
        changed_students[student_id] = student

    if upload_type == "end_of_day":
        report_date = get_report_date(report_rows)
        if report_date:
            for student_id, existing_student in students_by_id.items():
                student = changed_students.get(student_id) or clone_student(existing_student)
                if evaluate_end_of_day_detention(student, grouped_absences.get((student_id, report_date), []), report_date):
                    update_student_status(student)
                    changed_students[student_id] = student

    write_students(changed_students)

    return {
        "added": new_late_count,
        "detentionsAssigned": detention_assigned_count,
        "uploadType": upload_type,
    }


def clone_student(existing_student, source_row=None):
    if existing_student:
        student = dict(existing_student)
        student["lateArrivals"] = list(existing_student.get("lateArrivals", existing_student.get("truancies", [])))
        student["truancies"] = list(student["lateArrivals"])
        student["detentionHistory"] = list(existing_student.get("detentionHistory", []))
        student["escalationReasons"] = list(existing_student.get("escalationReasons", []))
        student["activeDetention"] = dict(existing_student.get("activeDetention", {})) if existing_student.get("activeDetention") else None
        student["escalationSuppression"] = dict(existing_student.get("escalationSuppression", {}))
        if not student["activeDetention"] and existing_student.get("truancyResolved") is False and student["lateArrivals"]:
            latest_late = sorted(student["lateArrivals"], key=lambda item: item.get("date", ""), reverse=True)[0]
            student["activeDetention"] = {
                "status": "open",
                "createdFromLateDate": latest_late.get("date"),
                "scheduledForDate": latest_late.get("date"),
                "sourceUploadType": "legacy_migration",
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
        "lateArrivals": [],
        "truancies": [],
        "lateCount": 0,
        "truancyCount": 0,
        "detentionsServed": 0,
        "detentionHistory": [],
        "activeDetention": None,
        "escalated": False,
        "escalationReasons": [],
        "manualEscalation": False,
        "escalationSuppression": {
            "lateCountUntil": 0,
            "missedCountUntil": 0,
        },
        "notes": "",
    }


def add_late_arrival(student, late_row, upload_type):
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
        "uploadType": upload_type,
    })
    existing_late_arrivals.sort(key=lambda item: item["date"], reverse=True)

    student["lateArrivals"] = existing_late_arrivals
    student["truancies"] = existing_late_arrivals
    student["lateCount"] = len(existing_late_arrivals)
    student["truancyCount"] = len(existing_late_arrivals)
    return True


def should_assign_detention(student):
    active_detention = student.get("activeDetention")
    return not active_detention or active_detention.get("status") != "open"


def assign_detention(student, late_row, upload_type, scheduled_date):
    student["activeDetention"] = {
        "status": "open",
        "createdFromLateDate": late_row["date"],
        "scheduledForDate": scheduled_date,
        "sourceUploadType": upload_type,
        "createdAt": datetime.now(SYDNEY_TZ).isoformat(),
        "lastRollMark": None,
        "lastRollMarkedAt": None,
        "pendingAttendanceCheckDate": None,
        "missedWhilePresentCount": 0,
    }


def evaluate_end_of_day_detention(student, student_rows_for_date, report_date):
    active_detention = student.get("activeDetention")
    if not active_detention or active_detention.get("status") != "open":
        return False

    if active_detention.get("pendingAttendanceCheckDate") != report_date:
        return False

    present_at_school = determine_present_at_school(student_rows_for_date)
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

    student["escalationReasons"] = reasons
    student["escalated"] = bool(reasons)
    student["truancyResolved"] = not active_detention_open
    student["activeDetention"] = active_detention if active_detention_open else None


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


def get_upload_time_warning(upload_type):
    now = datetime.now(SYDNEY_TZ)
    current_minutes = now.hour * 60 + now.minute

    if upload_type == "midday":
        expected_min = 10 * 60
        expected_max = 12 * 60 + 30
        if not (expected_min <= current_minutes <= expected_max):
            return "This upload was marked as midday, but the upload time is outside the usual midday window."
    else:
        expected_min = 14 * 60
        expected_max = 18 * 60
        if not (expected_min <= current_minutes <= expected_max):
            return "This upload was marked as end of day, but the upload time is outside the usual end-of-day window."

    return None


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

    if email not in ALLOWED_ADMIN_EMAILS:
        return None, (jsonify({"status": "error", "message": "This account is not allowed to use admin controls."}), 403)

    payload = request.get_json(silent=True) or {}
    if payload.get("password", "") != ADMIN_PASSWORD:
        return None, (jsonify({"status": "error", "message": "Admin password was incorrect."}), 403)

    return payload, None


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
