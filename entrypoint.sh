#!/bin/sh
set -e

# Reset lich su chay truoc khi start Flask
python -c "
from pathlib import Path
f = Path('/app/scripts/job_his.xml')
f.parent.mkdir(parents=True, exist_ok=True)
f.write_text(\"<?xml version='1.0' encoding='utf-8'?>\n<histories />\", encoding='utf-8')
print('[init] Da reset job_his.xml')
"

exec python web/app.py
