# Admin Assistant Backend

This repository contains the Flask backend for the Attendance Assistant project. Its main purpose is to accept uploaded attendance spreadsheets, extract truancy information, and write the results into Firestore.

This service is designed to run on Render and be called by the static frontend hosted separately.

## What This Service Does

The backend exposes a small API:

- `GET /`
- `POST /upload`
- `POST /admin/purge`

The service is responsible for:

- accepting uploaded `.xls` and `.xlsx` files
- reading the spreadsheet with pandas
- identifying rows that represent lateness or unjustified attendance issues
- converting those rows into truancy records
- creating or updating student documents in Firestore
- securely performing the admin purge action on the server

It does not render HTML templates or serve the frontend.

## System Architecture

The Attendance Assistant project is split into two repos:

- `Admin-assistant`: static frontend UI
- `admin-assistant-backend`: Flask upload and processing API

The end-to-end flow looks like this:

1. A staff user signs into the frontend.
2. The frontend uploads an attendance spreadsheet to `POST /upload`.
3. Flask reads the spreadsheet into memory.
4. The backend extracts student and attendance fields from each row.
5. The backend updates the `students` collection in Firestore.
6. The frontend reloads data directly from Firestore and shows the updated records.

## Runtime Stack

Main technologies:

- Flask
- flask-cors
- pandas
- openpyxl
- xlrd
- firebase-admin
- gunicorn

Dependency list is stored in [`requirements.txt`](./requirements.txt).

## Deployment

This repo includes a Render blueprint file: [`.render.yaml`](./.render.yaml)

Current Render service definition:

- service type: `web`
- runtime: `python`
- build command: `pip install -r requirements.txt`
- start command: `gunicorn app:app -c gunicorn.conf.py`
- auto deploy: enabled

## Environment Variables

The service expects a Firebase service account JSON payload in the environment variable:

- `FIREBASE_KEY_JSON`
- `ADMIN_PASSWORD`
- `ADMIN_PURGE_ENABLED`

At startup, [`app.py`](./app.py) loads that JSON, creates a Firebase Admin credential, and opens a Firestore client.

Expected setup:

```python
cred_dict = json.loads(os.environ['FIREBASE_KEY_JSON'])
cred = credentials.Certificate(cred_dict)
firebase_admin.initialize_app(cred)
db = firestore.client()
```

If `FIREBASE_KEY_JSON` is missing or malformed, the app will fail on startup.

Admin purge configuration:

- `ADMIN_PASSWORD`: the backend-only admin password used by the protected purge endpoint
- `ADMIN_PURGE_ENABLED`: set to `true` to allow purge, or `false` to lock it at the server

## CORS

CORS is enabled for the GitHub Pages frontend origin:

```python
CORS(app, resources={r"/*": {"origins": "https://koglint.github.io"}})
```

If the frontend moves to a different domain, this origin will need to be updated.

## API Endpoints

### `GET /`

Health-style endpoint that returns a plain string confirming the backend is alive.

Use it to quickly verify the Render service is responding.

### `POST /upload`

Accepts a multipart form upload with a file field named `file`.

Expected request:

```http
POST /upload
Content-Type: multipart/form-data
```

Form fields:

- `file`: the attendance spreadsheet

Basic validation:

- returns `400` if no file is attached
- returns `400` if the filename is empty
- returns `500` if spreadsheet parsing fails
- returns `500` if Firestore updates fail

Success response:

```json
{
  "status": "success",
  "added": 12
}
```

### `POST /admin/purge`

This endpoint performs a full delete of the `students` collection.

It is intentionally protected by several checks:

- the server must have `ADMIN_PURGE_ENABLED=true`
- the server must have `ADMIN_PASSWORD` configured
- the request must include a valid Firebase ID token in the `Authorization: Bearer <token>` header
- the signed-in Firebase user must have a verified email
- the email must be one of:
  - `troy.koglin1@det.nsw.edu.au`
  - `troy.koglin1@education.nsw.gov.au`
- the request body must include the correct backend password
- the request body must include `confirmation: "DELETE"`

Example request body:

```json
{
  "password": "your-admin-password",
  "confirmation": "DELETE"
}
```

Success response:

```json
{
  "status": "success",
  "deleted": 123
}
```

## Spreadsheet Processing Logic

The upload route reads the file into memory and then tries two pandas engines:

1. `openpyxl`
2. `xlrd`

This allows the service to support both newer Excel files and older workbook formats.

After loading the spreadsheet, the backend normalizes several columns:

- `Comment`
- `Description`
- `Date`
- `Explainer`
- `Explainer Source`

The code expects spreadsheet columns with names such as:

- `Student ID`
- `Given Name(s)`
- `Surname`
- `Roll Class`
- `Date`
- `Description`
- `Comment`
- `Explainer`
- `Explainer Source`
- `Time`

If the export format changes and these headers move or get renamed, this parser will need updating.

## Truancy Detection Rules

For each row, the backend:

1. Reads the student identity and attendance metadata.
2. Converts the date to `YYYY-MM-DD`.
3. Lowercases the description and comment.
4. Checks whether the description contains:
   - `absent`
   - `unjustified`
5. Skips rows that do not match those keywords.
6. Marks a record as justified if the description contains:
   - `school business`
   - `leave`
   - `justified`

This means the service intentionally filters the spreadsheet down to records considered relevant to truancy/lateness follow-up.

## Arrival Time And Minutes Late

The backend parses the `Time` column when it contains a range like:

```text
08:35 - 09:04
```

The logic:

- takes the value after the hyphen as the arrival time
- converts it to `HH:MM`
- compares it to the scheduled start time of `08:35`
- stores the difference as `minutesLate`

If time parsing fails, `arrivalTime` and `minutesLate` remain `None`.

## Firestore Write Model

Student documents are stored in the `students` collection, keyed by student ID.

When a new student is first seen, the backend creates a document with fields like:

- `givenName`
- `surname`
- `rollClass`
- `truancyCount`
- `truancyResolved`
- `truancies`
- `detentionsServed`
- `notes`
- `escalated`

Each truancy record includes:

```json
{
  "date": "2026-03-31",
  "description": "unjustified late arrival",
  "comment": "",
  "justified": false,
  "resolved": false,
  "explainer": "",
  "explainerSource": "",
  "detentionIssued": false,
  "arrivalTime": "09:04",
  "minutesLate": 29
}
```

## Duplicate Handling

For existing students, the backend checks whether a truancy already exists for the same date before appending a new one.

Current behavior:

- if a matching date already exists, no new truancy is added
- if the date is new, the truancy is appended and the list is re-sorted newest first

This is a simple duplicate rule. If multiple distinct truancies can happen on the same day and need to be preserved separately, the duplicate logic would need to become more specific.

## Resolution Logic

When a new truancy is added to an existing student:

- the backend reads `lastDetentionServedDate`
- compares it with the latest truancy date
- recalculates `truancyResolved`

This allows the frontend detention workflow to drive whether a student appears resolved or unresolved after new uploads.

## Local Development

Create and activate a virtual environment, then install dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Set the Firebase service account JSON in `FIREBASE_KEY_JSON`, then run:

```bash
flask --app app run
```

or

```bash
python app.py
```

If using the second option, make sure your Flask environment is configured the way you expect.

## Testing Uploads Locally

To test locally:

1. Start the Flask server.
2. Make sure `FIREBASE_KEY_JSON` points to a service account with Firestore access.
3. Set `ADMIN_PASSWORD` and `ADMIN_PURGE_ENABLED=true` if you want to test the admin purge locally.
4. Point the frontend upload URL to your local backend if needed.
5. Upload a real attendance export from the frontend or use a REST client.

Example with `curl`:

```bash
curl -X POST http://127.0.0.1:5000/upload -F "file=@attendance.xlsx"
```

## Key Files

- [`app.py`](./app.py): Flask app, upload route, spreadsheet parsing, Firestore writes
- [`requirements.txt`](./requirements.txt): Python dependencies
- [`gunicorn.conf.py`](./gunicorn.conf.py): Gunicorn runtime configuration
- [`.render.yaml`](./.render.yaml): Render deployment blueprint

## Operational Notes

- The frontend and backend must point to the same Firebase project.
- The CORS origin must match the deployed frontend domain.
- Spreadsheet header changes are the most likely reason for parser breakage.
- Firestore document shape changes should be coordinated with the frontend repo.
- The secure purge endpoint is the only place that should delete all student records.

## Maintenance Checklist

- Keep `requirements.txt` current and remove duplicates when cleaning dependencies.
- Verify Render still has the correct `FIREBASE_KEY_JSON` secret after any environment changes.
- Verify Render still has the correct `ADMIN_PASSWORD` and `ADMIN_PURGE_ENABLED` values after any admin-security changes.
- Re-test an upload whenever the school attendance export format changes.
- Re-test the frontend home page, detentions page, and reports page after any Firestore schema change.
- Re-test the Admin page whenever auth, allowed emails, or backend domain settings change.
