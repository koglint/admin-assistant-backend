from flask import Flask, request, jsonify
from flask_cors import CORS

import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore


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
def upload():
    file = request.files['file']
    df = pd.read_excel(file, engine='xlrd')

    # Filter truants (late + Absent or Unjustified)
    df['Comment'] = df['Comment'].astype(str).str.lower()
    df['Description'] = df['Description'].astype(str).str.lower()
    truants = df[
        (df['Comment'] == 'late') &
        (df['Description'].isin(['absent', 'unjustified']))
    ]

    # Update Firestore
    for _, row in truants.iterrows():
        student_id = str(row['Student ID'])
        name = f"{row['Given Name(s)']} {row['Surname']}"
        roll_class = row['Roll Class']

        doc_ref = db.collection('students').document(student_id)
        doc = doc_ref.get()
        if doc.exists:
            doc_ref.update({
                'truancyCount': firestore.Increment(1)
            })
        else:
            doc_ref.set({
                'fullName': name,
                'rollClass': roll_class,
                'truancyCount': 1,
                'detentionsServed': 0,
                'notes': ''
            })

    return jsonify({"status": "success", "updated": len(truants)})

