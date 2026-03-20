# src/run_sync.py
"""
Chạy đồng bộ:
- Nếu job có câu lệnh (statements) trong XML: đọc từ XML, thực thi từng câu lệnh trên kết nối đích.
  Câu lệnh theo mẫu: INSERT INTO table(col) SELECT col FROM table.
- Nếu job legacy (columns): SELECT từ source, INSERT batch lên target như trước.
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import oracledb
from src.config_loader import get_connection_by_id, get_sync_job
from src.sql_generator import generate_sql_file


def run_sync(job_id: int, regenerate_sql: bool = True, output_dir: str | Path | None = None) -> dict:
    """
    Chạy sync cho job_id.
    Nếu job có statements: kết nối target, execute từng câu lệnh (INSERT INTO...SELECT).
    Ngược lại: legacy (fetch source, insert target theo batch).
    Trả về: rows_affected hoặc rows_selected/rows_inserted, error (nếu có).
    """
    job = get_sync_job(job_id)
    statements = job.get("statements") or []

    if statements:
        # Chạy từng câu lệnh trên kết nối đích (mẫu INSERT INTO table(col) SELECT col FROM table)
        tgt = get_connection_by_id(job["target_connection_id"])
        total_rows = 0
        try:
            with oracledb.connect(dsn=tgt["dsn"], user=tgt["user"], password=tgt["password"]) as conn:
                with conn.cursor() as cur:
                    for sql in statements:
                        sql = (sql or "").strip()
                        if not sql:
                            continue
                        cur.execute(sql)
                        if cur.rowcount is not None and cur.rowcount >= 0:
                            total_rows += cur.rowcount
                    conn.commit()
        except Exception as e:
            return {"rows_affected": total_rows, "error": str(e)}
        return {"rows_affected": total_rows, "error": None}

    # Legacy: job dùng columns, fetch source + insert target
    sql_file_path = job.get("sql_file_path")
    if regenerate_sql or not sql_file_path or not Path(sql_file_path).exists():
        sql_file_path = generate_sql_file(job_id, output_dir)
    sql_content = Path(sql_file_path).read_text(encoding="utf-8").strip()
    src_conn_info = get_connection_by_id(job["source_connection_id"])
    tgt_conn_info = get_connection_by_id(job["target_connection_id"])
    source_cols = [c["source"] for c in job["columns"]]
    source_table = job["source_table"]
    where = (job.get("where_clause") or "").strip()
   batch_size = job.get("batch_size", 10000)  # ← chuẩn nhất
    select_sql = "SELECT " + ", ".join(f'"{c}"' for c in source_cols) + f" FROM {source_table}"
    if where:
        select_sql += " WHERE " + where
    rows_selected, rows_inserted = 0, 0
    try:
        with oracledb.connect(dsn=src_conn_info["dsn"], user=src_conn_info["user"], password=src_conn_info["password"]) as conn_src:
            with oracledb.connect(dsn=tgt_conn_info["dsn"], user=tgt_conn_info["user"], password=tgt_conn_info["password"]) as conn_tgt:
                cur_src = conn_src.cursor()
                cur_src.execute(select_sql)
                cur_tgt = conn_tgt.cursor()
                while True:
                    batch = cur_src.fetchmany(batch_size)
                    if not batch:
                        break
                    rows_selected += len(batch)
                    cur_tgt.executemany(sql_content, batch)
                    rows_inserted += len(batch)
                    conn_tgt.commit()
    except Exception as e:
        return {"rows_selected": rows_selected, "rows_inserted": rows_inserted, "error": str(e)}
    return {"rows_selected": rows_selected, "rows_inserted": rows_inserted, "error": None}


def main():
    if len(sys.argv) < 2:
        print("Cách dùng: python -m src.run_sync <job_id> [--no-regenerate]")
        sys.exit(1)
    job_id = int(sys.argv[1])
    regenerate = "--no-regenerate" not in sys.argv
    result = run_sync(job_id, regenerate_sql=regenerate)
    if "rows_affected" in result:
        print(f"Đã thực thi: {result.get('rows_affected', 0)} dòng.")
    else:
        print(f"Đã chọn: {result.get('rows_selected', 0)} dòng, đã insert: {result.get('rows_inserted', 0)} dòng.")
    if result.get("error"):
        print("Lỗi:", result["error"])
        sys.exit(2)


if __name__ == "__main__":
    main()
