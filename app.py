from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin

import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import io
import traceback


# Initialize Flask
app = Flask(__name__)
CORS(app, origins=["https://koglint.github.io"]) # Adjust the origin as needed 

# Firebase Admin SDK setup
import os, json
cred_dict = json.loads(os.environ['FIREBASE_KEY_JSON'])
cred = credentials.Certificate(cred_dict)

firebase_admin.initialize_app(cred)
db = firestore.client()

@app.route('/')
def home():
    return "Admin Assistant Flask backend is live!"

@app.route('/upload', methods=['POST'])
@cross_origin(origins=["https://koglint.github.io"])
def upload():
    # ✅ 1. Ensure a file was uploaded
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "No selected file"}), 400

    try:
        # Read file into memory to allow multiple attempts
        in_memory_file = io.BytesIO(file.read())

        try:
            # Try openpyxl (best for modern .xlsx files, even if misnamed)
            df = pd.read_excel(in_memory_file, engine='openpyxl')
        except Exception as e_openpyxl:
            in_memory_file.seek(0)  # Reset for retry
            try:
                # Fallback to xlrd (for actual .xls files)
                df = pd.read_excel(in_memory_file, engine='xlrd')
            except Exception as e_xlrd:
                raise ValueError(f"openpyxl error: {e_openpyxl}; xlrd error: {e_xlrd}")

    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Excel read failed: {str(e)}"}), 500


    try:
        # ✅ 3. Filter truants: late + absent/unjustified
        df['Comment'] = df['Comment'].astype(str).str.lower()
        df['Description'] = df['Description'].astype(str).str.lower()
        truants = df[
            (df['Comment'] == 'late') &
            (df['Description'].isin(['absent', 'unjustified']))
        ]

        # ✅ 4. Update Firestore
        for _, row in truants.iterrows():
            student_id = str(row['Student ID'])
            name = f"{row['Given Name(s)']} {row['Surname']}"
            roll_class = row['Roll Class']

            doc_ref = db.collection('students').document(student_id)
            doc = doc_ref.get()
            if doc.exists:
                doc_ref.update({'truancyCount': firestore.Increment(1)})
            else:
                doc_ref.set({
                    'fullName': name,
                    'rollClass': roll_class,
                    'truancyCount': 1,
                    'detentionsServed': 0,
                    'notes': ''
                })

        return jsonify({
            "status": "success",
            "updated": len(truants),
            "exampleNames": truants["Given Name(s)"].head(3).tolist()
        })
    
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Firestore update failed: {str(e)}"}), 500


