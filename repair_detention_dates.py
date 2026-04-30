import argparse
import json
import os
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import firebase_admin
from firebase_admin import credentials, firestore


SYDNEY_TZ = ZoneInfo("Australia/Sydney")


def init_db():
    if not firebase_admin._apps:
        cred_dict = json.loads(os.environ["FIREBASE_KEY_JSON"])
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Repair stale open-detention scheduled dates using stored late-arrival history."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the repairs to Firestore. Without this flag the script only reports what would change.",
    )
    parser.add_argument(
        "--student-id",
        help="Restrict the repair to a single student ID for safe testing.",
    )
    return parser.parse_args()


def parse_time_value(value):
    cleaned = str(value or "").strip().replace(" ", "").upper()
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


def first_break_start_for_date(date_string):
    date_value = datetime.strptime(date_string, "%Y-%m-%d").date()
    if date_value.weekday() in {1, 3}:
        return time(10, 25)
    return time(10, 35)


def is_tuesday(date_string):
    return datetime.strptime(date_string, "%Y-%m-%d").date().weekday() == 1


def determine_detention_date_from_late_record(late_date, late_record):
    if is_tuesday(late_date):
        return next_school_day(late_date)

    arrival_dt = parse_time_value(late_record.get("arrivalTime"))
    if not arrival_dt:
        return next_school_day(late_date)

    if arrival_dt.time() < first_break_start_for_date(late_date):
        return late_date

    return next_school_day(late_date)


def build_detention_source_context(late_date, scheduled_date):
    if is_tuesday(late_date):
        return "auto_next_school_day_tuesday_detention_runs_first_break"
    if scheduled_date == late_date:
        return "auto_same_day_before_first_break"
    return "auto_next_school_day_after_first_break"


def should_repair_student(student):
    active_detention = student.get("activeDetention") or {}
    if active_detention.get("status") != "open":
        return False, "not_open"

    if active_detention.get("pendingAttendanceCheckDate"):
        return False, "pending_attendance_check"

    if active_detention.get("missedWhilePresentCount", 0):
        return False, "already_missed_and_rescheduled"

    late_date = active_detention.get("createdFromLateDate")
    if not late_date:
        return False, "missing_created_from_late_date"

    late_arrivals = student.get("lateArrivals", student.get("truancies", [])) or []
    matching_late = next((entry for entry in late_arrivals if entry.get("date") == late_date), None)
    if not matching_late:
        return False, "missing_matching_late_record"

    corrected_date = determine_detention_date_from_late_record(late_date, matching_late)
    corrected_context = build_detention_source_context(late_date, corrected_date)

    current_date = active_detention.get("scheduledForDate")
    current_context = active_detention.get("sourceContext")
    if current_date == corrected_date and current_context == corrected_context:
        return False, "already_correct"

    return True, {
        "lateDate": late_date,
        "arrivalTime": matching_late.get("arrivalTime"),
        "currentScheduledForDate": current_date,
        "correctedScheduledForDate": corrected_date,
        "currentSourceContext": current_context,
        "correctedSourceContext": corrected_context,
    }


def repair_students(db, apply_changes=False, student_id_filter=None):
    changed = []
    skipped = {}

    stream = db.collection("students").stream()
    for snapshot in stream:
        if student_id_filter and snapshot.id != student_id_filter:
            continue

        student = snapshot.to_dict() or {}
        should_repair, detail = should_repair_student(student)
        if not should_repair:
            skipped[detail] = skipped.get(detail, 0) + 1
            continue

        changed.append({
            "studentId": snapshot.id,
            "surname": student.get("surname", ""),
            "givenName": student.get("givenName", ""),
            **detail,
        })

        if apply_changes:
            db.collection("students").document(snapshot.id).update({
                "activeDetention.scheduledForDate": detail["correctedScheduledForDate"],
                "activeDetention.sourceContext": detail["correctedSourceContext"],
                "updatedAt": firestore.SERVER_TIMESTAMP,
                "updatedBy": "repair_detention_dates_script",
                "lastAction": "repair_detention_schedule",
            })

    return changed, skipped


def main():
    args = parse_args()
    db = init_db()
    changed, skipped = repair_students(
        db,
        apply_changes=args.apply,
        student_id_filter=args.student_id,
    )

    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"Mode: {mode}")
    print(f"Timezone: {SYDNEY_TZ}")
    print(f"Students to change: {len(changed)}")
    print("Skip summary:", json.dumps(skipped, indent=2, sort_keys=True))

    preview = changed[:50]
    print("Preview:", json.dumps(preview, indent=2))

    if args.apply and changed:
        print("Applied updates successfully.")


if __name__ == "__main__":
    main()
