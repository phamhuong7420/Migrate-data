# src/sql_generator.py
"""
Đọc config job từ DB, generate file.sql chứa câu lệnh INSERT với bind variables.
Nội dung: INSERT INTO target_table (col1, col2, ...) VALUES (:1, :2, ...)
Python sẽ chạy câu này với từng batch dữ liệu lấy từ source.
"""
from pathlib import Path

from src.config_loader import get_sync_job, update_sql_file_path


def generate_sql_file(job_id: int, output_dir: str | Path | None = None) -> str:
    """
    Generate file.sql cho job_id (đọc bảng/cột từ DB), ghi ra thư mục output_dir.
    Trả về đường dẫn file đã ghi. Cập nhật sync_jobs.sql_file_path.
    """
    job = get_sync_job(job_id)
    target_table = job["target_table"]
    columns = job["columns"]
    if not columns:
        raise ValueError(f"Job {job_id} chưa có danh sách cột. Cần thêm bản ghi vào sync_job_columns.")

    target_cols = [c["target"] for c in columns]
    cols_str = ", ".join(f'"{c}"' for c in target_cols)
    placeholders = ", ".join(f":{i+1}" for i in range(len(target_cols)))
    sql = f"INSERT INTO {target_table} ({cols_str}) VALUES ({placeholders})"

    if output_dir is None:
        output_dir = Path(__file__).resolve().parent.parent / "output"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"sync_job_{job_id}.sql"

    file_path.write_text(sql, encoding="utf-8")
    path_str = str(file_path.resolve())
    update_sql_file_path(job_id, path_str)
    return path_str
