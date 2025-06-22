from flask import Flask, request, jsonify
from flask_cors import CORS

import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import io
import traceback
import os, json
from datetime import datetime, time

# Initialize Flask
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "https://koglint.github.io"}})
app.config['CORS_HEADERS'] = 'Content-Type'

# Firebase Admin SDK setup
cred_dict = json.loads(os.environ['FIREBASE_KEY_JSON'])
cred = credentials.Certificate(cred_dict)
firebase_admin.initialize_app(cred)
db = firestore.client()

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

        added = 0
        for _, row in df.iterrows():
            try:
                student_id = str(row['Student ID'])
                name = f"{row['Given Name(s)']} {row['Surname']}"
                roll_class = row['Roll Class']
                date_str = row.get('Date', '')
                if date_str:
                    date = pd.to_datetime(date_str).strftime('%Y-%m-%d')
                else:
                    date = datetime.today().strftime('%Y-%m-%d')
                reason = row.get('Description', 'unspecified')
                comment = row.get('Comment', '')

                time_range = str(row.get('Time', ''))
                arrival_time_str = None
                minutes_late = None
                try:
                    if '-' in time_range:
                        arrival_part = time_range.split('-')[-1].strip()
                        arrival_dt = pd.to_datetime(arrival_part)
                        arrival_time_str = arrival_dt.strftime('%H:%M')

                        scheduled_time = time(8, 35)
                        arrival_time = arrival_dt.time()
                        minutes_late = (datetime.combine(datetime.today(), arrival_time) - datetime.combine(datetime.today(), scheduled_time)).total_seconds() // 60
                        minutes_late = max(0, int(minutes_late))
                except:
                    arrival_time_str = None
                    minutes_late = None

                truancy_record = {
                    'date': date,
                    'reason': reason,
                    'comment': comment,
                    'resolved': False,
                    'justified': False,
                    'detentionIssued': False,
                    'arrivalTime': arrival_time_str,
                    'minutesLate': minutes_late
                }

                doc_ref = db.collection('students').document(student_id)
                doc = doc_ref.get()

                if doc.exists:
                    doc_data = doc.to_dict()
                    existing = doc_data.get('truancies', [])
                    if not any(t['date'] == date and t['reason'] == reason for t in existing):
                        existing.append(truancy_record)
                        unresolved_count = sum(1 for t in existing if not t['resolved'] and not t['justified'])
                        doc_ref.update({
                            'truancies': existing,
                            'truancyCount': len(existing),
                            'unresolvedDetentions': unresolved_count
                        })
                        added += 1
                else:
                    doc_ref.set({
                        'fullName': name,
                        'rollClass': roll_class,
                        'truancyCount': 1,
                        'unresolvedDetentions': 1,
                        'truancies': [truancy_record],
                        'detentionsServed': 0,
                        'notes': ''
                    })
                    added += 1

            except Exception as student_error:
                traceback.print_exc()
                continue

        return jsonify({"status": "success", "added": added})

    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Firestore update failed: {str(e)}"}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    traceback.print_exc()
    return jsonify({"status": "error", "message": str(e)}), 500
