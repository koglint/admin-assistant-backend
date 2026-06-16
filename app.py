from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
import io
import json
import os
import traceback
from zipfile import is_zipfile


import firebase_admin
import pandas as pd
import xlrd
from firebase_admin import auth as firebase_auth
from firebase_admin import credentials, firestore
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(
    app,
    resources={
        r"/*": {
            "origins": ["https://koglint.github.io"],
            "methods": ["GET", "POST", "OPTIONS"],
            "allow_headers": ["Content-Type", "Authorization"],
        }
    },
)
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
DETENTION_DURATION_MINUTES = 15
LATE_ATTENDANCE_CODES = {
    ("U", "UNJUSTIFIED"),
    ("?", "ABSENT"),
}
EXCLUDED_ATTENDANCE_SHORTHANDS = {"S", "F", "E", "B", "L", "M"}
EXCLUDED_ATTENDANCE_DESCRIPTIONS = {
    "SICK",
    "FLEXIBLE",
    "SUSPENDED",
    "SCHOOL BUSINESS",
    "LEAVE",
    "EXEMPT",
}
ROLL_CALL_START_TIMES = {time(8, 0), time(8, 25)}
LATE_ARRIVAL_THRESHOLD = time(8, 35)
UPLOAD_MODE_LATE_ARRIVALS = "late_arrivals"
UPLOAD_MODE_ATTENDANCE_CONFIRMATION = "attendance_confirmation"
UPLOAD_MODES = {
    UPLOAD_MODE_LATE_ARRIVALS,
    UPLOAD_MODE_ATTENDANCE_CONFIRMATION,
}


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
        workbook_bytes = uploaded_file.read()
        df = read_attendance_workbook(workbook_bytes)
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Excel read failed: {exc}"}), 500

    try:
        normalized_df = normalize_dataframe(df)
        report_rows = build_report_rows(normalized_df)
        report_date = get_report_date(report_rows)
        report_context = build_report_context(report_rows, report_date)
        students_by_id = load_existing_students()
        upload_mode = normalize_upload_mode(request.form.get("uploadMode"))
        summary = process_upload(report_rows, students_by_id, report_context, upload_mode)
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

    deleted_by_collection = purge_firestore_collections(["students", "attendance_days", "uploadTracking", "student_exceptions"])
    return jsonify({
        "status": "success",
        "deleted": sum(deleted_by_collection.values()),
        "deletedByCollection": deleted_by_collection,
    })


@app.route("/admin/student-purge", methods=["POST"])
def admin_student_purge():
    payload, error_response = verify_admin_request(require_purge_enabled=False)
    if error_response:
        return error_response

    student_id = normalize_student_id(payload.get("studentId"))
    if not student_id:
        return jsonify({"status": "error", "message": "Student ID is required."}), 400

    if payload.get("confirmation", "") != student_id:
        return jsonify({"status": "error", "message": "Student ID confirmation was incorrect."}), 400

    deleted_by_collection = purge_student_records(student_id)
    return jsonify({
        "status": "success",
        "studentId": student_id,
        "deleted": sum(deleted_by_collection.values()),
        "deletedByCollection": deleted_by_collection,
    })


@app.route("/admin/student-exception", methods=["POST"])
def admin_student_exception():
    payload, error_response = verify_admin_request(require_purge_enabled=False)
    if error_response:
        return error_response

    student_id = normalize_student_id(payload.get("studentId"))
    if not student_id:
        return jsonify({"status": "error", "message": "Student ID is required."}), 400

    if payload.get("confirmation", "") != student_id:
        return jsonify({"status": "error", "message": "Student ID confirmation was incorrect."}), 400

    deleted_by_collection = purge_student_records(student_id, keep_exception=True)
    db.collection("student_exceptions").document(student_id).set({
        "studentId": student_id,
        "reason": str(payload.get("reason", "")).strip(),
        "createdAt": firestore.SERVER_TIMESTAMP,
        "createdBy": "admin_backend",
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "updatedBy": "admin_backend",
        "lastAction": "student_added_to_exception_list",
    }, merge=True)

    return jsonify({
        "status": "success",
        "studentId": student_id,
        "deleted": sum(deleted_by_collection.values()),
        "deletedByCollection": deleted_by_collection,
    })


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


@app.route("/attendance-days/lookup", methods=["POST", "OPTIONS"])
def attendance_days_lookup():
    if request.method == "OPTIONS":
        return jsonify({"status": "ok"})

    payload = request.get_json(silent=True) or {}
    pairs = payload.get("pairs") or []
    if not isinstance(pairs, list):
        return jsonify({"status": "error", "message": "pairs must be a list."}), 400

    results = {}
    full_coverage_by_date = {}
    exception_ids = load_exception_student_ids()
    for pair in pairs[:1000]:
        if not isinstance(pair, dict):
            continue

        student_id = str(pair.get("studentId", "")).strip()
        date_string = str(pair.get("date", "")).strip()
        if not student_id or not date_string:
            continue
        if student_id in exception_ids:
            continue

        doc_id = build_attendance_day_doc_id(student_id, date_string)
        snapshot = db.collection("attendance_days").document(doc_id).get()
        if snapshot.exists:
            record = snapshot.to_dict()
            if record.get("hasFullDayCoverage") is not True:
                if date_string not in full_coverage_by_date:
                    full_coverage_by_date[date_string] = attendance_date_has_full_day_coverage(date_string)
                if full_coverage_by_date[date_string]:
                    record = {**record, "hasFullDayCoverage": True}
            results[doc_id] = record
            continue

        if date_string not in full_coverage_by_date:
            full_coverage_by_date[date_string] = attendance_date_has_full_day_coverage(date_string)

        if full_coverage_by_date[date_string]:
            results[doc_id] = build_present_attendance_day_record(student_id, date_string)

    return jsonify({
        "status": "success",
        "records": results,
    })


def attendance_date_has_full_day_coverage(date_string):
    snapshots = (
        db.collection("attendance_days")
        .where("date", "==", date_string)
        .where("hasFullDayCoverage", "==", True)
        .limit(1)
        .stream()
    )
    return any(True for _ in snapshots)


def build_present_attendance_day_record(student_id, date_string):
    return {
        "studentId": student_id,
        "date": date_string,
        "presentAtSchool": True,
        "presentDuringDetention": True,
        "hasFullDayCoverage": True,
        "latestObservedTime": None,
        "rowCount": 0,
    }


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


def read_attendance_workbook(workbook_bytes):
    errors = []

    if is_zipfile(io.BytesIO(workbook_bytes)):
        try:
            return pd.read_excel(io.BytesIO(workbook_bytes), engine="openpyxl")
        except Exception as exc:
            errors.append(f"openpyxl error: {exc}")
    else:
        errors.append("openpyxl skipped: file is not an .xlsx zip workbook")

    try:
        return pd.read_excel(io.BytesIO(workbook_bytes), engine="xlrd")
    except Exception as exc:
        errors.append(f"xlrd error: {exc}")

    for encoding in ("cp1252", "latin1"):
        try:
            workbook = xlrd.open_workbook(
                file_contents=workbook_bytes,
                encoding_override=encoding,
            )
            return xlrd_first_sheet_to_dataframe(workbook)
        except Exception as exc:
            errors.append(f"xlrd {encoding} fallback error: {exc}")

    raise ValueError("; ".join(errors))


def xlrd_first_sheet_to_dataframe(workbook):
    sheet = workbook.sheet_by_index(0)
    if sheet.nrows == 0:
        return pd.DataFrame()

    headers = [
        str(xlrd_cell_value(sheet.cell(0, col_index), workbook.datemode) or "").strip()
        for col_index in range(sheet.ncols)
    ]

    rows = []
    for row_index in range(1, sheet.nrows):
        rows.append([
            xlrd_cell_value(sheet.cell(row_index, col_index), workbook.datemode)
            for col_index in range(sheet.ncols)
        ])

    return pd.DataFrame(rows, columns=headers)


def xlrd_cell_value(cell, datemode):
    if cell.ctype == xlrd.XL_CELL_DATE:
        return xlrd.xldate.xldate_as_datetime(cell.value, datemode)
    if cell.ctype == xlrd.XL_CELL_EMPTY:
        return ""
    return cell.value


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
    exception_ids = load_exception_student_ids()
    return {
        doc_snapshot.id: doc_snapshot.to_dict()
        for doc_snapshot in db.collection("students").stream()
        if doc_snapshot.id not in exception_ids
    }


def load_exception_student_ids():
    return {
        doc_snapshot.id
        for doc_snapshot in db.collection("student_exceptions").stream()
    }


def normalize_upload_mode(upload_mode):
    upload_mode = str(upload_mode or "").strip()
    if upload_mode in UPLOAD_MODES:
        return upload_mode
    return UPLOAD_MODE_LATE_ARRIVALS


def process_upload(report_rows, students_by_id, report_context, upload_mode=UPLOAD_MODE_LATE_ARRIVALS):
    report_date = report_context.get("reportDate")
    exception_ids = load_exception_student_ids()
    report_rows = [row for row in report_rows if row["studentId"] not in exception_ids]
    process_late_arrivals = upload_mode == UPLOAD_MODE_LATE_ARRIVALS
    late_rows = [row for row in report_rows if is_roll_call_late(row)] if process_late_arrivals else []
    attendance_day_records = build_attendance_day_records(report_rows)

    write_attendance_day_records(attendance_day_records)

    new_late_count = 0
    detention_assigned_count = 0
    detention_stats = {
        "completed": 0,
        "missedWhilePresent": 0,
        "notCountedAbsenceRecorded": 0,
        "rolledForward": 0,
        "markedPending": 0,
        "matched": 0,
        "passes": 0,
    }

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

    if process_late_arrivals:
        for student_id in students_by_id.keys():
            reconcile_student_detention_schedule_transaction(student_id)

    full_coverage_dates = get_full_coverage_dates(attendance_day_records)
    if full_coverage_dates:
        detention_stats = process_detention_attendance_checks(attendance_day_records, full_coverage_dates)

    pending_detention_check_dates = summarize_pending_detention_check_dates()
    pending_detention_checks = sum(
        item["count"] for item in pending_detention_check_dates
    )

    return {
        "added": new_late_count,
        "detentionsAssigned": detention_assigned_count,
        "detentionChecksCompleted": detention_stats["completed"],
        "detentionChecksMatched": detention_stats["matched"],
        "missedDetentionsConfirmed": detention_stats["missedWhilePresent"],
        "detentionAbsencesNotCounted": detention_stats["notCountedAbsenceRecorded"],
        "detentionsRolledForward": detention_stats["rolledForward"],
        "detentionChecksMarkedPending": detention_stats["markedPending"],
        "detentionCheckPasses": detention_stats["passes"],
        "pendingDetentionChecks": pending_detention_checks,
        "pendingDetentionCheckDates": pending_detention_check_dates,
        "reportDate": report_date,
        "coversFullDay": report_context.get("coversFullDay", False),
        "latestObservedTime": report_context.get("latestObservedTime"),
        "attendanceDatesChecked": len(full_coverage_dates),
        "uploadMode": upload_mode,
        "lateProcessingSkipped": not process_late_arrivals,
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
        student["lateArrivals"] = sanitize_late_arrivals(existing_student.get("lateArrivals", []))
        student["detentionHistory"] = list(existing_student.get("detentionHistory", []))
        student["activeDetention"] = dict(existing_student.get("activeDetention", {})) if existing_student.get("activeDetention") else None
        return student

    return {
        "givenName": source_row.get("givenName", "") if source_row else "",
        "surname": source_row.get("surname", "") if source_row else "",
        "rollClass": source_row.get("rollClass", "") if source_row else "",
        "yearGroup": source_row.get("yearGroup", "") if source_row else "",
        "lateArrivals": [],
        "lateCount": 0,
        "detentionsServed": 0,
        "detentionHistory": [],
        "activeDetention": None,
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
        "justified": False,
        "arrivalTime": arrival_time_text or None,
        "minutesLate": minutes_late,
        "shorthand": late_row["shorthand"],
        "yearGroup": late_row.get("yearGroup"),
    })
    existing_late_arrivals.sort(key=lambda item: item["date"], reverse=True)

    student["lateArrivals"] = existing_late_arrivals
    student["lateCount"] = len(existing_late_arrivals)
    return True


def sanitize_late_arrivals(late_arrivals):
    if not isinstance(late_arrivals, list):
        return []

    allowed_fields = {
        "date",
        "description",
        "justified",
        "arrivalTime",
        "minutesLate",
        "shorthand",
        "yearGroup",
    }
    return [
        {key: value for key, value in entry.items() if key in allowed_fields}
        for entry in late_arrivals
        if isinstance(entry, dict)
    ]


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
        detention_reconciled = False

        for late_row in sorted(late_rows, key=lambda row: (row["date"], row.get("timeEnd") or "")):
            if add_late_arrival(student, late_row):
                late_added += 1
                if should_assign_detention(student):
                    scheduled_date = determine_detention_date(late_row)
                    assign_detention(student, late_row, scheduled_date)
                    detentions_assigned += 1
            elif reconcile_active_detention_schedule(student, late_row):
                detention_reconciled = True

        update_student_status(student)
        if late_added == 0 and detentions_assigned == 0 and not detention_reconciled and not student_identity_changed(original_student, student):
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
        result = evaluate_pending_detention(student, attendance_day_record, report_date)
        if not result:
            return None

        update_student_status(student)
        apply_audit_fields(student, "backend_detention_attendance_evaluated", "backend_upload")
        transaction_obj.set(student_ref, student)
        return result

    return _apply(transaction)


def reconcile_student_detention_schedule_transaction(student_id):
    transaction = db.transaction()
    student_ref = db.collection("students").document(student_id)

    @firestore.transactional
    def _apply(transaction_obj):
        snapshot = next(transaction_obj.get(student_ref), None)
        if not snapshot or not snapshot.exists:
            return False

        student = clone_student(snapshot.to_dict())
        if not reconcile_active_detention_from_history(student):
            return False

        update_student_status(student)
        apply_audit_fields(student, "backend_detention_schedule_reconciled", "backend_upload")
        transaction_obj.set(student_ref, student)
        return True

    return _apply(transaction)


def count_pending_detention_checks(report_date=None):
    pending_count = 0
    for student_snapshot in db.collection("students").stream():
        active_detention = student_snapshot.to_dict().get("activeDetention") or {}
        pending_date = active_detention.get("pendingAttendanceCheckDate")
        if pending_date and (report_date is None or pending_date == report_date):
            pending_count += 1
    return pending_count


def summarize_pending_detention_check_dates():
    pending_by_date = {}
    for student_snapshot in db.collection("students").stream():
        active_detention = student_snapshot.to_dict().get("activeDetention") or {}
        pending_date = active_detention.get("pendingAttendanceCheckDate")
        if pending_date:
            pending_by_date[pending_date] = pending_by_date.get(pending_date, 0) + 1

    return [
        {"date": pending_date, "count": pending_by_date[pending_date]}
        for pending_date in sorted(pending_by_date)
    ]


def get_pending_detention_check_candidates(full_coverage_dates):
    full_coverage_date_set = set(full_coverage_dates)
    if not full_coverage_date_set:
        return []

    candidates = []
    for student_snapshot in db.collection("students").stream():
        active_detention = student_snapshot.to_dict().get("activeDetention") or {}
        pending_date = active_detention.get("pendingAttendanceCheckDate")
        scheduled_date = active_detention.get("scheduledForDate")
        if pending_date in full_coverage_date_set:
            candidates.append((student_snapshot.id, pending_date))
        elif scheduled_date in full_coverage_date_set:
            candidates.append((student_snapshot.id, scheduled_date))

    return candidates


def process_detention_attendance_checks(attendance_day_records, full_coverage_dates):
    full_coverage_date_set = set(full_coverage_dates)
    stats = {
        "completed": 0,
        "missedWhilePresent": 0,
        "notCountedAbsenceRecorded": 0,
        "rolledForward": 0,
        "markedPending": 0,
        "matched": 0,
        "passes": 0,
    }
    max_passes = max(1, len(full_coverage_dates) + 5)

    for _ in range(max_passes):
        candidates = get_pending_detention_check_candidates(full_coverage_date_set)
        stats["matched"] += len(candidates)
        stats["passes"] += 1
        changed_this_pass = 0

        for student_id, attendance_date in candidates:
            attendance_day_record = get_attendance_day_record_for_pending_check(
                attendance_day_records,
                full_coverage_date_set,
                student_id,
                attendance_date
            )
            result = apply_pending_detention_transaction(
                student_id,
                attendance_day_record,
                attendance_date
            )
            if not result:
                continue

            changed_this_pass += 1
            if result.get("completed"):
                stats["completed"] += 1
            if result.get("missedWhilePresent"):
                stats["missedWhilePresent"] += 1
            if result.get("notCountedAbsenceRecorded"):
                stats["notCountedAbsenceRecorded"] += 1
            if result.get("rolledForward"):
                stats["rolledForward"] += 1
            if result.get("markedPending"):
                stats["markedPending"] += 1

        if changed_this_pass == 0:
            break

    return stats


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


def reconcile_active_detention_schedule(student, late_row):
    active_detention = student.get("activeDetention")
    if not active_detention or active_detention.get("status") != "open":
        return False

    if active_detention.get("pendingAttendanceCheckDate"):
        return False

    if active_detention.get("createdFromLateDate") != late_row["date"]:
        return False

    corrected_date = determine_detention_date(late_row)
    corrected_context = build_detention_source_context(late_row, corrected_date)
    if (
        active_detention.get("scheduledForDate") == corrected_date
        and active_detention.get("sourceContext") == corrected_context
    ):
        return False

    active_detention["scheduledForDate"] = corrected_date
    active_detention["sourceContext"] = corrected_context
    student["activeDetention"] = active_detention
    return True


def reconcile_active_detention_from_history(student):
    active_detention = student.get("activeDetention")
    if not active_detention or active_detention.get("status") != "open":
        return False

    late_date = active_detention.get("createdFromLateDate")
    if not late_date:
        return False

    matching_late = next(
        (entry for entry in student.get("lateArrivals", []) if entry.get("date") == late_date),
        None
    )
    if not matching_late:
        return False

    corrected_date = determine_detention_date_from_late_record(late_date, matching_late)
    corrected_context = build_detention_source_context({"date": late_date}, corrected_date)
    changed = False

    if active_detention.get("scheduledForDate") != corrected_date:
        active_detention["scheduledForDate"] = corrected_date
        changed = True

    if active_detention.get("sourceContext") != corrected_context:
        active_detention["sourceContext"] = corrected_context
        changed = True

    if changed:
        student["activeDetention"] = active_detention

    return changed


def evaluate_pending_detention(student, attendance_day_record, report_date):
    active_detention = student.get("activeDetention")
    if not active_detention or active_detention.get("status") != "open":
        return None

    pending_date = active_detention.get("pendingAttendanceCheckDate")
    scheduled_date = active_detention.get("scheduledForDate")
    if report_date not in {pending_date, scheduled_date}:
        return None

    if not attendance_day_record or not attendance_day_record.get("hasFullDayCoverage"):
        if scheduled_date == report_date and not pending_date:
            active_detention["pendingAttendanceCheckDate"] = report_date
            student["activeDetention"] = active_detention
            return {
                "completed": False,
                "missedWhilePresent": False,
                "notCountedAbsenceRecorded": False,
                "rolledForward": False,
                "markedPending": True,
            }
        return None

    row_count = attendance_day_record.get("rowCount", 0)
    present_during_detention = row_count == 0
    evidence = (
        "no_absence_row_full_day_coverage"
        if present_during_detention
        else "absence_row_recorded_not_counted"
    )

    active_detention["lastEvaluatedDate"] = report_date
    active_detention["pendingAttendanceCheckDate"] = None

    if present_during_detention:
        student.setdefault("detentionHistory", []).append({
            "date": report_date,
            "lateDate": active_detention.get("createdFromLateDate"),
            "scheduledForDate": active_detention.get("scheduledForDate") or report_date,
            "outcome": "missed_while_present",
            "attendanceEvidence": evidence,
            "attendanceDayRowCount": row_count,
        })
    else:
        student.setdefault("detentionHistory", []).append({
            "date": report_date,
            "lateDate": active_detention.get("createdFromLateDate"),
            "scheduledForDate": active_detention.get("scheduledForDate") or report_date,
            "outcome": "not_counted_absence_recorded",
            "attendanceEvidence": evidence,
            "attendanceDayRowCount": row_count,
        })

    active_detention["scheduledForDate"] = next_school_day(report_date)
    active_detention["missedWhilePresentCount"] = count_current_missed_while_present(student, active_detention)
    student["activeDetention"] = active_detention
    return {
        "completed": True,
        "missedWhilePresent": present_during_detention,
        "notCountedAbsenceRecorded": not present_during_detention,
        "rolledForward": True,
        "markedPending": False,
    }


def update_student_status(student):
    active_detention = student.get("activeDetention") or {}
    active_detention_open = active_detention.get("status") == "open"
    student["activeDetention"] = active_detention if active_detention_open else None


def count_current_missed_while_present(student, active_detention):
    if not active_detention or active_detention.get("status") != "open":
        return 0

    history = student.get("detentionHistory", [])
    if not isinstance(history, list):
        history = []

    most_recent_served_index = -1
    for index in range(len(history) - 1, -1, -1):
        entry = history[index] if isinstance(history[index], dict) else {}
        if entry.get("outcome") == "served":
            most_recent_served_index = index
            break

    unresolved_history = history[most_recent_served_index + 1:]
    active_late_date = active_detention.get("createdFromLateDate") or ""
    history_count = 0
    for entry in unresolved_history:
        if not isinstance(entry, dict):
            continue
        if entry.get("outcome") != "missed_while_present":
            continue
        if active_late_date and entry.get("lateDate") and entry.get("lateDate") != active_late_date:
            continue
        history_count += 1

    return history_count


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
        has_full_day_absence = any(is_full_day_absence_row(row) for row in student_rows)
        has_full_day_coverage = has_full_day_absence or (
            latest_observed is not None and latest_observed >= time(14, 45)
        )
        records[(student_id, date_string)] = {
            "studentId": student_id,
            "date": date_string,
            "presentAtSchool": determine_present_at_school(student_rows),
            "presentDuringDetention": determine_present_during_detention(student_rows, date_string),
            "hasFullDayCoverage": has_full_day_coverage,
            "latestObservedTime": latest_observed.strftime("%I:%M%p") if latest_observed else None,
            "rowCount": len(student_rows),
        }
    return records


def get_full_coverage_dates(attendance_day_records):
    return sorted({
        record["date"]
        for record in attendance_day_records.values()
        if record.get("hasFullDayCoverage")
    })


def get_attendance_day_record_for_pending_check(
    attendance_day_records,
    full_coverage_date_set,
    student_id,
    attendance_date
):
    attendance_day_record = attendance_day_records.get((student_id, attendance_date))
    if attendance_date not in full_coverage_date_set:
        return attendance_day_record

    if attendance_day_record:
        return {
            **attendance_day_record,
            "hasFullDayCoverage": True,
        }

    return build_present_attendance_day_record(student_id, attendance_date)


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


def determine_present_during_detention(student_rows_for_date, date_string):
    return not student_rows_for_date


def time_ranges_overlap(start_a, end_a, start_b, end_b):
    return start_a < end_b and start_b < end_a


def is_full_day_absence_row(row):
    start_text, end_text = row["timeStart"], row["timeEnd"]
    if not row["timeRange"]:
        return True

    start_is_morning = start_text in {"8:00AM", "8:25AM"}
    end_is_end_of_day = end_text in {"2:45PM", "02:45PM", "14:45"}
    return start_is_morning and end_is_end_of_day


def is_roll_call_late(row):
    shorthand = normalize_attendance_text(row["shorthand"])
    description = normalize_attendance_text(row["description"])

    if shorthand in EXCLUDED_ATTENDANCE_SHORTHANDS or description in EXCLUDED_ATTENDANCE_DESCRIPTIONS:
        return False

    if (shorthand, description) not in LATE_ATTENDANCE_CODES:
        return False

    time_start = parse_time_value(row.get("timeStart"))
    time_end = parse_time_value(row.get("timeEnd"))
    if not time_start or not time_end:
        return False

    return time_start.time() in ROLL_CALL_START_TIMES and time_end.time() > LATE_ARRIVAL_THRESHOLD


def split_time_range(time_range):
    if not time_range or "-" not in time_range:
        return "", ""

    left, right = time_range.split("-", 1)
    return normalize_time_text(left), normalize_time_text(right)


def normalize_time_text(value):
    return str(value or "").strip().replace(" ", "").upper()


def normalize_attendance_text(value):
    return " ".join(str(value or "").strip().upper().split())


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
    late_date = late_row["date"]
    if is_tuesday(late_date):
        return next_school_day(late_date)

    arrival_dt = parse_time_value(late_row.get("timeEnd"))
    if not arrival_dt:
        return next_school_day(late_date)

    if arrival_dt.time() < first_break_start_for_date(late_date):
        return late_date

    return next_school_day(late_date)


def determine_detention_date_from_late_record(late_date, late_record):
    if is_tuesday(late_date):
        return next_school_day(late_date)

    arrival_dt = parse_time_value(late_record.get("arrivalTime"))
    if not arrival_dt:
        return next_school_day(late_date)

    if arrival_dt.time() < first_break_start_for_date(late_date):
        return late_date

    return next_school_day(late_date)


def first_break_start_for_date(date_string):
    date_value = datetime.strptime(date_string, "%Y-%m-%d").date()
    if date_value.weekday() in {1, 3}:
        return time(10, 25)
    return time(10, 35)


def is_tuesday(date_string):
    date_value = datetime.strptime(date_string, "%Y-%m-%d").date()
    return date_value.weekday() == 1


def build_detention_source_context(late_row, scheduled_date):
    if is_tuesday(late_row["date"]):
        return "auto_next_school_day_tuesday_detention_runs_first_break"
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


def normalize_student_id(value):
    return str(value or "").strip()


def purge_student_records(student_id, keep_exception=False):
    deleted_by_collection = {
        "students": 0,
        "attendance_days": 0,
        "student_exceptions": 0,
    }

    student_ref = db.collection("students").document(student_id)
    if student_ref.get().exists:
        student_ref.delete()
        deleted_by_collection["students"] = 1

    deleted_by_collection["attendance_days"] = purge_query_documents(
        db.collection("attendance_days").where("studentId", "==", student_id)
    )

    if not keep_exception:
        exception_ref = db.collection("student_exceptions").document(student_id)
        if exception_ref.get().exists:
            exception_ref.delete()
            deleted_by_collection["student_exceptions"] = 1

    return deleted_by_collection


def purge_query_documents(query):
    documents = list(query.stream())
    deleted = 0
    batch = db.batch()
    batch_size = 0

    for document in documents:
        batch.delete(document.reference)
        batch_size += 1
        deleted += 1

        if batch_size >= 450:
            batch.commit()
            batch = db.batch()
            batch_size = 0

    if batch_size:
        batch.commit()

    return deleted


def purge_firestore_collections(collection_names):
    deleted_by_collection = {}

    for collection_name in collection_names:
        deleted_by_collection[collection_name] = purge_firestore_collection(collection_name)

    return deleted_by_collection


def purge_firestore_collection(collection_name):
    documents = list(db.collection(collection_name).stream())
    deleted = 0
    batch = db.batch()
    batch_size = 0

    for document in documents:
        batch.delete(document.reference)
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
