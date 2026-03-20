"""
Quản lý danh sách đồng bộ bảng, lưu trong scripts/job_sync.csv.
Mỗi dòng = 1 tác vụ đồng bộ (id duy nhất).
Cột: id, source_connection_name, target_connection_name, source_table, target_table, sql_text
"""
import csv
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
_CSV_FILE = _SCRIPTS_DIR / "job_sync.csv"
_FIELDNAMES = [
    "id", "name",
    "source_connection_name", "target_connection_name",
    "source_table", "target_table",
    "sql_text",
]


def _load_all() -> list:
    if not _CSV_FILE.exists():
        return []
    rows = []
    with open(_CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rows.append({
                    "id": int(row.get("id", 0)),
                    "name": row.get("name", ""),
                    "source_connection_name": row.get("source_connection_name", ""),
                    "target_connection_name": row.get("target_connection_name", ""),
                    "source_table": row.get("source_table", ""),
                    "target_table": row.get("target_table", ""),
                    "sql_text": row.get("sql_text", ""),
                })
            except (ValueError, TypeError):
                continue
    return rows


def _write_all(rows: list):
    _SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(_CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def load_all_statements_csv() -> list:
    return _load_all()


def get_statement_csv(stmt_id: int) -> dict | None:
    for r in _load_all():
        if r["id"] == stmt_id:
            return r
    return None


def add_statement_csv(source_connection_name: str, target_connection_name: str,
                      source_table: str, target_table: str,
                      sql_text: str = "", name: str = "") -> int:
    rows = _load_all()
    new_id = max((r["id"] for r in rows), default=0) + 1
    rows.append({
        "id": new_id,
        "name": name,
        "source_connection_name": source_connection_name,
        "target_connection_name": target_connection_name,
        "source_table": source_table,
        "target_table": target_table,
        "sql_text": sql_text,
    })
    _write_all(rows)
    return new_id


def update_statement_csv(stmt_id: int, sql_text: str):
    """Cập nhật sql_text của dòng có id=stmt_id. Raise nếu không tìm thấy."""
    rows = _load_all()
    for row in rows:
        if row["id"] == stmt_id:
            row["sql_text"] = sql_text
            _write_all(rows)
            return
    raise ValueError(f"Không tìm thấy job id={stmt_id} trong job_sync.csv.")


def delete_statement_csv(stmt_id: int):
    rows = _load_all()
    rows = [r for r in rows if r["id"] != stmt_id]
    _write_all(rows)


def delete_statements_csv_bulk(stmt_ids: list[int]):
    """Xóa nhiều dòng trong 1 lần đọc/ghi CSV."""
    if not stmt_ids:
        return
    id_set = set(stmt_ids)
    rows = _load_all()
    rows = [r for r in rows if r["id"] not in id_set]
    _write_all(rows)
