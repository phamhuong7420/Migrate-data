# src/generate_sql.py
"""
Generate file.sql từ config đã lưu (sau khi chọn bảng/cột trên giao diện và lưu vào sync_jobs + sync_job_columns).
Cách dùng: python -m src.generate_sql <job_id> [output_dir]
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from src.sql_generator import generate_sql_file


def main():
    if len(sys.argv) < 2:
        print("Cách dùng: python -m src.generate_sql <job_id> [output_dir]")
        sys.exit(1)
    job_id = int(sys.argv[1])
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None
    path = generate_sql_file(job_id, output_dir)
    print(f"Đã ghi file: {path}")


if __name__ == "__main__":
    main()
