# gunicorn.conf.py
bind = "0.0.0.0:10000"
workers = 1
timeout = 120  # Increase from default 30s to 120s
