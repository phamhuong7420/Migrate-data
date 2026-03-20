# src/config_loader.py
"""
Đọc config từ file XML (sync_config.xml).
Trả về thông tin job: nếu job có job_statements thì dùng danh sách câu lệnh (INSERT INTO...SELECT);
không thì dùng job_columns (legacy) để generate INSERT.
"""
from config.file_config import load_config, load_connections_only, save_config, get_job_with_statements


def get_connection_by_id(connection_id: int) -> dict:
    """Lấy thông tin kết nối theo id. Trả về dict dùng cho oracledb.connect."""
    data = load_connections_only()
    cid = int(connection_id)
    for c in data.get("connections", []):
        if c.get("id") == cid:
            return {
                "name": c.get("name", ""),
                "dsn": f"{c.get('host', '')}:{c.get('port', 1521)}/{c.get('service_name', '')}",
                "user": c.get("username", ""),
                "password": c.get("password_enc") or "",
            }
    raise ValueError(f"Connection id {connection_id} không tồn tại")


def get_sync_job(job_id: int) -> dict:
    """
    Lấy job để chạy sync.
    Nếu job có job_statements (câu lệnh trong XML): trả về "statements" = list sql_text.
    Ngược lại trả về "columns", source_table, target_table... (legacy).
    """
    job = get_job_with_statements(job_id)
    if not job:
        raise ValueError(f"Sync job id {job_id} không tồn tại")
    statements = job.get("statements") or []
    if statements:
        return {
            "id": job.get("id"),
            "name": job.get("name", ""),
            "source_connection_id": job.get("source_connection_id"),
            "target_connection_id": job.get("target_connection_id"),
            "statements": statements,
            "columns": [],
        }
    # Legacy: job dùng job_columns
    data = load_config()
    jid = int(job_id)
    for j in data.get("jobs", []):
        if j.get("id") == jid:
            columns = [
                {"source": c["source_column_name"], "target": c["target_column_name"]}
                for c in sorted(
                    [c for c in data.get("job_columns", []) if c.get("sync_job_id") == jid],
                    key=lambda x: x.get("column_order", 0),
                )
            ]
            return {
                "id": j.get("id"),
                "name": j.get("name", ""),
                "source_connection_id": j.get("source_connection_id"),
                "target_connection_id": j.get("target_connection_id"),
                "source_table": j.get("source_table", ""),
                "target_table": j.get("target_table", ""),
                "where_clause": j.get("where_clause") or "",
                "batch_size": j.get("batch_size") or 1000,
                "statements": [],
                "columns": columns,
            }
    raise ValueError(f"Sync job id {job_id} không tồn tại")


def update_sql_file_path(job_id: int, sql_file_path: str) -> None:
    data = load_config()
    for j in data.get("jobs", []):
        if j.get("id") == int(job_id):
            j["sql_file_path"] = sql_file_path
            save_config(data)
            return
    raise ValueError(f"Job id {job_id} không tồn tại")
