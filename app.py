from flask import Flask, request, jsonify
from flask_cors import CORS

import io
import json
import os
import traceback
from datetime import datetime, time

import firebase_admin
import pandas as pd
from firebase_admin import auth as firebase_auth
from firebase_admin import credentials, firestore

# Initialize Flask
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "https://koglint.github.io"}})
app.config['CORS_HEADERS'] = 'Content-Type'

# Firebase Admin SDK setup
cred_dict = json.loads(os.environ['FIREBASE_KEY_JSON'])
cred = credentials.Certificate(cred_dict)
firebase_admin.initialize_app(cred)
db = firestore.client()

ALLOWED_ADMIN_EMAILS = {
    "troy.koglin1@det.nsw.edu.au",
    "troy.koglin1@education.nsw.gov.au"
}
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD")
ADMIN_PURGE_ENABLED = os.environ.get("ADMIN_PURGE_ENABLED", "false").lower() == "true"


@app.route('/')
def home():
    return "Admin Assistant Flask backend is live!"


@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "No selected file"}), 400

    try:
        in_memory_file = io.BytesIO(file.read())
        try:
            df = pd.read_excel(in_memory_file, engine='openpyxl')
        except Exception as e_openpyxl:
            in_memory_file.seek(0)
            try:
                df = pd.read_excel(in_memory_file, engine='xlrd')
            except Exception as e_xlrd:
                raise ValueError(f"openpyxl error: {e_openpyxl}; xlrd error: {e_xlrd}")
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Excel read failed: {str(e)}"}), 500

    try:
        df['Comment'] = df.get('Comment', '').astype(str).str.lower()
        df['Description'] = df.get('Description', '').astype(str).str.lower()
        df['date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['Explainer'] = df.get('Explainer', '').fillna('').astype(str)
        df['Explainer Source'] = df.get('Explainer Source', '').fillna('').astype(str)

        added = 0
        batch = db.batch()
        batch_size = 0
        max_batch_size = 500

        existing_students = {student_doc.id: student_doc.to_dict() for student_doc in db.collection('students').stream()}

        for _, row in df.iterrows():
            try:
                student_id = str(row['Student ID'])
                given_name = row['Given Name(s)']
                surname = row['Surname']
                roll_class = row['Roll Class']
                date = row['date'] if pd.notna(row['date']) else datetime.today().strftime('%Y-%m-%d')
                description = row.get('Description', 'unspecified').lower()

                if not ('absent' in description or 'unjustified' in description):
                    continue

                justified_keywords = ['school business', 'leave', 'justified']
                is_justified = any(keyword in description for keyword in justified_keywords)

                comment = row.get('Comment', '')
                explainer = row['Explainer'].strip()
                explainer_source = row['Explainer Source'].strip()

                time_range = str(row.get('Time', ''))
                arrival_time_str = None
                minutes_late = None

                if '-' in time_range:
                    arrival_part = time_range.split('-')[-1].strip()
                    arrival_dt = pd.to_datetime(arrival_part, errors='coerce')
                    if pd.notna(arrival_dt):
                        arrival_time_str = arrival_dt.strftime('%H:%M')
                        scheduled_time = time(8, 35)
                        arrival_time = arrival_dt.time()
                        minutes_late = (
                            datetime.combine(datetime.today(), arrival_time)
                            - datetime.combine(datetime.today(), scheduled_time)
                        ).total_seconds() // 60
                        minutes_late = max(0, int(minutes_late))

                truancy_record = {
                    'date': date,
                    'description': description,
                    'comment': comment,
                    'justified': is_justified,
                    'resolved': False,
                    'explainer': explainer,
                    'explainerSource': explainer_source,
                    'detentionIssued': False,
                    'arrivalTime': arrival_time_str,
                    'minutesLate': minutes_late
                }

                doc_ref = db.collection('students').document(student_id)
                existing_doc = existing_students.get(student_id)

                if existing_doc:
                    existing = existing_doc.get('truancies', [])
                    if not any(pd.to_datetime(t['date']).strftime('%Y-%m-%d') == date for t in existing):
                        existing.append(truancy_record)
                        existing.sort(key=lambda item: item['date'], reverse=True)
                        latest_truancy_date = existing[0]['date']
                        last_served_date = existing_doc.get('lastDetentionServedDate')
                        truancy_resolved = bool(last_served_date and last_served_date >= latest_truancy_date)

                        batch.update(doc_ref, {
                            'truancies': existing,
                            'truancyCount': len(existing),
                            'truancyResolved': truancy_resolved
                        })
                        added += 1
                else:
                    batch.set(doc_ref, {
                        'givenName': given_name,
                        'surname': surname,
                        'rollClass': roll_class,
                        'truancyCount': 1,
                        'truancyResolved': False,
                        'truancies': [truancy_record],
                        'detentionsServed': 0,
                        'notes': '',
                        'escalated': False,
                    })
                    added += 1

                batch_size += 1
                if batch_size >= max_batch_size:
                    batch.commit()
                    batch = db.batch()
                    batch_size = 0

            except Exception:
                traceback.print_exc()
                continue

        if batch_size > 0:
            batch.commit()

        return jsonify({"status": "success", "added": added})

    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Firestore update failed: {str(e)}"}), 500


@app.route('/admin/purge', methods=['POST'])
def admin_purge():
    payload, error_response = verify_admin_request(require_purge_enabled=True)
    if error_response:
        return error_response

    confirmation = payload.get("confirmation", "")

    if confirmation != "DELETE":
        return jsonify({"status": "error", "message": "Deletion confirmation text was incorrect."}), 400

    deleted = purge_all_students()
    return jsonify({"status": "success", "deleted": deleted})


@app.route('/admin/authorize', methods=['POST'])
def admin_authorize():
    _, error_response = verify_admin_request(require_purge_enabled=False)
    if error_response:
        return error_response

    return jsonify({
        "status": "success",
        "message": "Admin access granted.",
        "purgeEnabled": ADMIN_PURGE_ENABLED
    })


def extract_bearer_token(header_value):
    if not header_value:
        return None

    parts = header_value.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None

    return parts[1].strip()


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
    email_verified = decoded_token.get("email_verified", False)

    if not email_verified:
        return None, (jsonify({"status": "error", "message": "Email address is not verified."}), 403)

    if email not in ALLOWED_ADMIN_EMAILS:
        return None, (jsonify({"status": "error", "message": "This account is not allowed to use admin controls."}), 403)

    payload = request.get_json(silent=True) or {}
    password = payload.get("password", "")
    if password != ADMIN_PASSWORD:
        return None, (jsonify({"status": "error", "message": "Admin password was incorrect."}), 403)

    return payload, None


def purge_all_students():
    students = list(db.collection('students').stream())
    deleted = 0
    batch = db.batch()
    batch_size = 0
    max_batch_size = 500

    for student_doc in students:
        batch.delete(student_doc.reference)
        batch_size += 1
        deleted += 1

        if batch_size >= max_batch_size:
            batch.commit()
            batch = db.batch()
            batch_size = 0

    if batch_size > 0:
        batch.commit()

    return deleted


@app.errorhandler(Exception)
def handle_exception(e):
    traceback.print_exc()
    return jsonify({"status": "error", "message": str(e)}), 500
