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
        df['date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['Explainer'] = df.get('Explainer', '').fillna('').astype(str)
        df['Explainer Source'] = df.get('Explainer Source', '').fillna('').astype(str)


        added = 0
        batch = db.batch()
        batch_size = 0
        max_batch_size = 500

        existing_students = {doc.id: doc.to_dict() for doc in db.collection('students').stream()}

        for _, row in df.iterrows():
            try:
                student_id = str(row['Student ID'])
                name = f"{row['Given Name(s)']} {row['Surname']}"
                roll_class = row['Roll Class']
                date = row['date'] if pd.notna(row['date']) else datetime.today().strftime('%Y-%m-%d')
                description = row.get('Description', 'unspecified')
                # Determine if the truancy is justified based on keywords in description
                justified_keywords = ['School Business', 'Leave', 'Justified']                              # Add more keywords as needed
                is_justified = any(kw in description.lower() for kw in justified_keywords)

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
                        minutes_late = (datetime.combine(datetime.today(), arrival_time) - datetime.combine(datetime.today(), scheduled_time)).total_seconds() // 60
                        minutes_late = max(0, int(minutes_late))
               
   

                truancy_record = {
                    'date': date,
                    'description': description,
                    'comment': comment,
                    'justified': is_justified,
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
                    if not any(t['date'] == date and t['description'] == description for t in existing):
                        existing.append(truancy_record)
                        existing.sort(key=lambda x: x['date'], reverse=True)
                        latest_truancy_date = existing[0]['date']
                        last_served_date = existing_doc.get('lastDetentionServedDate')
                        truancy_resolved = False
                        if last_served_date:
                            truancy_resolved = last_served_date >= latest_truancy_date


                        batch.update(doc_ref, {
                            'truancies': existing,
                            'truancyCount': len(existing),
                            'truancyResolved': truancy_resolved
                        })
                        added += 1
                else:
                    batch.set(doc_ref, {
                        'fullName': name,
                        'rollClass': roll_class,
                        'truancyCount': 1,
                        'truancyResolved': False,
                        'truancies': [truancy_record],
                        'detentionsServed': 0,
                        'notes': ''
                    })
                    added += 1

                batch_size += 1
                if batch_size >= max_batch_size:
                    batch.commit()
                    batch = db.batch()
                    batch_size = 0

            except Exception as student_error:
                traceback.print_exc()
                continue

        if batch_size > 0:
            batch.commit()

        return jsonify({"status": "success", "added": added})

    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Firestore update failed: {str(e)}"}), 500


@app.errorhandler(Exception)
def handle_exception(e):
    traceback.print_exc()
    return jsonify({"status": "error", "message": str(e)}), 500
