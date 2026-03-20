# config/file_config.py
"""
Đọc/ghi config từ file XML (sync_config.xml).
Kết nối và job (kèm câu lệnh INSERT INTO...SELECT) được lưu vào file XML, đọc và sửa từ đó.
"""
import xml.etree.ElementTree as ET
from pathlib import Path

CONFIG_XML = Path(__file__).resolve().parent.parent / "scripts" / "sync_config.xml"


def get_config_path() -> Path:
    return CONFIG_XML


def _sanitize_connection_name_for_filename(name: str) -> str:
    """Tên file an toàn theo tên kết nối, bỏ ký tự không hợp lệ."""
    s = (name or "").strip()
    for c in '\\/:*?"<>|':
        s = s.replace(c, "_")
    return s or "connection"


def _connection_tables_txt_path(connection_name: str) -> Path:
    """Đường dẫn file TXT danh sách bảng: <tên_kết_nối>_tables.txt trong scripts."""
    base = _sanitize_connection_name_for_filename(connection_name)
    return get_config_path().resolve().parent / f"{base}_tables.txt"


def _connection_columns_txt_path(connection_name: str) -> Path:
    """Đường dẫn file TXT danh sách cột: <tên_kết_nối>_columns.txt trong scripts."""
    base = _sanitize_connection_name_for_filename(connection_name)
    return get_config_path().resolve().parent / f"{base}_columns.txt"


def load_connection_metadata() -> dict:
    """Đọc từ file TXT theo tên kết nối: <tên>_tables.txt (danh sách bảng) và <tên>_columns.txt (danh sách cột). Trả về { connection_id: [ {table_name, columns}, ... ] }."""
    data = _load_config_raw()
    connections = data.get("connections", [])
    result = {}
    for c in connections:
        cid = c.get("id")
        name = c.get("name") or ""
        if not cid:
            continue
        tables_path = _connection_tables_txt_path(name).resolve()
        columns_path = _connection_columns_txt_path(name).resolve()
        if not tables_path.exists():
            result[cid] = []
            continue
        try:
            table_names = [line.strip() for line in tables_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        except OSError:
            result[cid] = []
            continue
        # Đọc cột: mỗi dòng "table_name: col1, col2, col3"
        cols_by_table = {}
        if columns_path.exists():
            try:
                for line in columns_path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if ":" in line:
                        tname, rest = line.split(":", 1)
                        tname = tname.strip()
                        col_names = [x.strip() for x in rest.split(",") if x.strip()]
                        cols_by_table[tname] = [{"column_name": cn, "data_type": "", "data_length": None, "nullable": ""} for cn in col_names]
            except OSError:
                pass
        tables_list = [
            {"table_name": tname, "columns": cols_by_table.get(tname, [])}
            for tname in table_names
        ]
        result[cid] = tables_list
    return result


def save_connection_metadata(metadata: dict) -> None:
    """Ghi ra 2 file TXT theo tên kết nối: <tên>_tables.txt (một bảng mỗi dòng) và <tên>_columns.txt (mỗi dòng: table_name: col1, col2, ...). Không tạo XML."""
    data = _load_config_raw()
    conn_by_id = {c.get("id"): c for c in data.get("connections", []) if c.get("id")}
    scripts_dir = get_config_path().resolve().parent
    scripts_dir.mkdir(parents=True, exist_ok=True)
    for cid, tables_list in metadata.items():
        c = conn_by_id.get(cid)
        name = (c.get("name") or "").strip() if c else f"conn_{cid}"
        base = _sanitize_connection_name_for_filename(name)
        tables_path = (scripts_dir / f"{base}_tables.txt").resolve()
        columns_path = (scripts_dir / f"{base}_columns.txt").resolve()
        # File danh sách bảng: một tên bảng mỗi dòng
        table_names = [t.get("table_name", "") for t in tables_list if t.get("table_name")]
        tables_path.write_text("\n".join(table_names), encoding="utf-8")
        # File danh sách cột: mỗi dòng "table_name: col1, col2, col3"
        col_lines = []
        for t in tables_list:
            tname = t.get("table_name", "")
            cols = t.get("columns") or []
            col_names = [str(x.get("column_name", "")) for x in cols if x.get("column_name")]
            col_lines.append(f"{tname}: {', '.join(col_names)}")
        columns_path.write_text("\n".join(col_lines), encoding="utf-8")


def _load_config_raw() -> dict:
    """Chỉ đọc sync_config.xml, không đọc file metadata (tránh đệ quy)."""
    path = get_config_path()
    if not path.exists():
        return {"connections": [], "jobs": [], "job_columns": [], "job_statements": []}
    try:
        tree = ET.parse(path)
        root = tree.getroot()
    except (ET.ParseError, OSError):
        return {"connections": [], "jobs": [], "job_columns": [], "job_statements": []}

    connections = []
    for node in root.findall(".//connection"):
        conn = {
            "id": _int_attr(node, "id"),
            "name": _text(node, "name", ""),
            "connection_type": _text(node, "connection_type", "source"),
            "host": _text(node, "host", ""),
            "port": _int_text(node, "port", 1521),
            "service_name": _text(node, "service_name", ""),
            "username": _text(node, "username", ""),
            "password_enc": _text(node, "password_enc") or None,
        }
        connections.append(conn)

    jobs = []
    for node in root.findall(".//job"):
        st = _text(node, "source_tables", "")
        tt = _text(node, "target_tables", "")
        jobs.append({
            "id": _int_attr(node, "id"),
            "name": _text(node, "name", ""),
            "source_connection_id": _int_text(node, "source_connection_id", 0),
            "target_connection_id": _int_text(node, "target_connection_id", 0),
            "source_table": _text(node, "source_table", ""),
            "target_table": _text(node, "target_table", ""),
            "source_tables": [x.strip() for x in st.split(",") if x.strip()] if st else [],
            "target_tables": [x.strip() for x in tt.split(",") if x.strip()] if tt else [],
            "sql_file_path": _text(node, "sql_file_path") or "",
            "where_clause": _text(node, "where_clause") or "",
            "batch_size": _int_text(node, "batch_size", 1000),
        })

    job_columns = []
    for node in root.findall(".//job_column"):
        job_columns.append({
            "id": _int_attr(node, "id"),
            "sync_job_id": _int_attr(node, "sync_job_id"),
            "column_order": _int_attr(node, "column_order"),
            "source_column_name": _text(node, "source_column_name", ""),
            "target_column_name": _text(node, "target_column_name", ""),
        })

    job_statements = []
    for node in root.findall(".//job_statement"):
        job_statements.append({
            "id": _int_attr(node, "id"),
            "job_id": _int_attr(node, "job_id"),
            "order": _int_attr(node, "order"),
            "sql_text": _text(node, "sql_text", ""),
        })

    return {"connections": connections, "jobs": jobs, "job_columns": job_columns, "job_statements": job_statements}


def load_connections_only() -> dict:
    """Đọc nhanh config chỉ từ XML (bỏ qua file TXT metadata). Dùng khi không cần c['tables']."""
    return _load_config_raw()


def load_config() -> dict:
    """
    Đọc config từ sync_config.xml và file TXT theo từng kết nối (<tên>_tables.txt, <tên>_columns.txt).
    Trả về: connections (có c["tables"]), jobs, job_columns, job_statements.
    """
    data = _load_config_raw()
    meta = load_connection_metadata()
    for c in data.get("connections", []):
        c["tables"] = meta.get(c["id"], [])
    return data


def save_config(data: dict) -> None:
    path = get_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    root = ET.Element("config")
    root.set("version", "1.0")

    conns_el = ET.SubElement(root, "connections")
    for c in data.get("connections", []):
        el = ET.SubElement(conns_el, "connection", id=str(c.get("id", "")))
        _el_text(el, "name", c.get("name", ""))
        _el_text(el, "connection_type", c.get("connection_type", "source"))
        _el_text(el, "host", c.get("host", ""))
        _el_text(el, "port", str(c.get("port", 1521)))
        _el_text(el, "service_name", c.get("service_name", ""))
        _el_text(el, "username", c.get("username", ""))
        _el_text(el, "password_enc", c.get("password_enc") or "")
        # Bảng/cột lưu ra file TXT (<tên>_tables.txt, <tên>_columns.txt), không ghi vào sync_config

    jobs_el = ET.SubElement(root, "jobs")
    for j in data.get("jobs", []):
        el = ET.SubElement(jobs_el, "job", id=str(j.get("id", "")))
        _el_text(el, "name", j.get("name", ""))
        _el_text(el, "source_connection_id", str(j.get("source_connection_id", "")))
        _el_text(el, "target_connection_id", str(j.get("target_connection_id", "")))
        _el_text(el, "source_table", j.get("source_table", ""))
        _el_text(el, "target_table", j.get("target_table", ""))
        _el_text(el, "source_tables", ",".join(j.get("source_tables") or []))
        _el_text(el, "target_tables", ",".join(j.get("target_tables") or []))
        _el_text(el, "sql_file_path", j.get("sql_file_path") or "")
        _el_text(el, "where_clause", j.get("where_clause") or "")
        _el_text(el, "batch_size", str(j.get("batch_size", 1000)))

    cols_el = ET.SubElement(root, "job_columns")
    for col in data.get("job_columns", []):
        el = ET.SubElement(cols_el, "job_column", id=str(col.get("id", "")), sync_job_id=str(col.get("sync_job_id", "")), column_order=str(col.get("column_order", "")))
        _el_text(el, "source_column_name", col.get("source_column_name", ""))
        _el_text(el, "target_column_name", col.get("target_column_name", ""))

    stmts_el = ET.SubElement(root, "job_statements")
    for s in data.get("job_statements", []):
        el = ET.SubElement(stmts_el, "job_statement", id=str(s.get("id", "")), job_id=str(s.get("job_id", "")), order=str(s.get("order", "")))
        _el_text(el, "sql_text", s.get("sql_text", ""))

    tree = ET.ElementTree(root)
    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass
    tree.write(path, encoding="utf-8", xml_declaration=True, default_namespace="")


def _text(node: ET.Element, tag: str, default: str = "") -> str:
    child = node.find(tag)
    return (child.text or "").strip() if child is not None else default


def _int_text(node: ET.Element, tag: str, default: int = 0) -> int:
    s = _text(node, tag, "")
    try:
        return int(s) if s else default
    except ValueError:
        return default


def _int_attr(node: ET.Element, attr: str) -> int:
    v = node.get(attr)
    try:
        return int(v) if v else 0
    except (ValueError, TypeError):
        return 0


def _el_text(parent: ET.Element, tag: str, value: str) -> None:
    el = ET.SubElement(parent, tag)
    el.text = (value or "").strip()


def _next_id(items: list, key: str = "id") -> int:
    if not items:
        return 1
    return max(int(x.get(key) or 0) for x in items) + 1


def get_connection(connection_id: int) -> dict | None:
    data = _load_config_raw()
    cid = int(connection_id)
    for c in data.get("connections", []):
        if c.get("id") == cid:
            return dict(c)
    return None


def add_connection(name: str, connection_type: str, host: str, port: int, service_name: str, username: str, password_enc: str | None) -> int:
    data = _load_config_raw()
    conns = data.setdefault("connections", [])
    if any(c.get("name", "").strip().lower() == name.strip().lower() for c in conns):
        raise ValueError(f"Tên kết nối '{name}' đã tồn tại. Vui lòng chọn tên khác.")
    new_id = _next_id(conns)
    conns.append({"id": new_id, "name": name, "connection_type": connection_type, "host": host, "port": port, "service_name": service_name, "username": username, "password_enc": password_enc})
    save_config(data)
    return new_id


def update_connection(connection_id: int, name: str, connection_type: str, host: str, port: int, service_name: str, username: str, password_enc: str | None = None) -> None:
    data = _load_config_raw()
    cid = int(connection_id)
    duplicate = any(
        c.get("name", "").strip().lower() == name.strip().lower() and c.get("id") != cid
        for c in data.get("connections", [])
    )
    if duplicate:
        raise ValueError(f"Tên kết nối '{name}' đã tồn tại. Vui lòng chọn tên khác.")
    for c in data.get("connections", []):
        if c.get("id") == cid:
            c["name"], c["connection_type"], c["host"], c["port"] = name, connection_type, host, port
            c["service_name"], c["username"] = service_name, username
            if password_enc is not None:
                c["password_enc"] = password_enc
            save_config(data)
            return
    raise ValueError(f"Connection id {connection_id} không tồn tại")


def delete_connection(connection_id: int) -> None:
    data = _load_config_raw()
    data["connections"] = [c for c in data.get("connections", []) if c.get("id") != int(connection_id)]
    save_config(data)


# --- Job (theo mẫu INSERT INTO table(col) SELECT col FROM table) ---
def add_job(name: str, source_connection_id: int, target_connection_id: int, source_tables: list | None = None, target_tables: list | None = None) -> int:
    """Thêm job mới (chưa có câu lệnh), lưu vào XML. source_tables/target_tables: danh sách tên bảng đã tích chọn."""
    data = _load_config_raw()
    jobs = data.setdefault("jobs", [])
    job_id = _next_id(jobs)
    jobs.append({
        "id": job_id, "name": name,
        "source_connection_id": source_connection_id, "target_connection_id": target_connection_id,
        "source_table": "", "target_table": "",
        "source_tables": source_tables or [], "target_tables": target_tables or [],
        "sql_file_path": "", "where_clause": "", "batch_size": 1000,
    })
    data.setdefault("job_statements", [])
    save_config(data)
    return job_id


def get_job(job_id: int) -> dict | None:
    data = _load_config_raw()
    jid = int(job_id)
    for j in data.get("jobs", []):
        if j.get("id") == jid:
            return dict(j)
    return None


def get_job_with_statements(job_id: int) -> dict | None:
    """Job kèm danh sách câu lệnh (sql_text), sắp theo order."""
    j = get_job(job_id)
    if not j:
        return None
    data = _load_config_raw()
    stmts = sorted(
        [s for s in data.get("job_statements", []) if s.get("job_id") == int(job_id)],
        key=lambda x: x.get("order", 0),
    )
    j["statements"] = [s.get("sql_text", "") for s in stmts]
    j["statement_list"] = stmts
    return j


def update_job(job_id: int, name: str, source_connection_id: int, target_connection_id: int) -> None:
    data = _load_config_raw()
    jid = int(job_id)
    for j in data.get("jobs", []):
        if j.get("id") == jid:
            j["name"] = name
            j["source_connection_id"] = source_connection_id
            j["target_connection_id"] = target_connection_id
            save_config(data)
            return
    raise ValueError(f"Job id {job_id} không tồn tại")


def delete_job(job_id: int) -> None:
    data = _load_config_raw()
    jid = int(job_id)
    data["jobs"] = [j for j in data.get("jobs", []) if j.get("id") != jid]
    data["job_statements"] = [s for s in data.get("job_statements", []) if s.get("job_id") != jid]
    save_config(data)


def add_statement(job_id: int, sql_text: str) -> int:
    """Thêm một câu lệnh vào job (mẫu: INSERT INTO table(col) SELECT col FROM table). Trả về statement id."""
    data = _load_config_raw()
    stmts = data.setdefault("job_statements", [])
    stmt_id = _next_id(stmts)
    same_job = [s for s in stmts if s.get("job_id") == int(job_id)]
    max_order = max((s.get("order") or 0) for s in same_job) if same_job else 0
    stmts.append({"id": stmt_id, "job_id": int(job_id), "order": max_order + 1, "sql_text": (sql_text or "").strip()})
    save_config(data)
    return stmt_id


def update_statement(statement_id: int, sql_text: str) -> None:
    data = _load_config_raw()
    sid = int(statement_id)
    for s in data.get("job_statements", []):
        if s.get("id") == sid:
            s["sql_text"] = (sql_text or "").strip()
            save_config(data)
            return
    raise ValueError(f"Statement id {statement_id} không tồn tại")


def delete_statement(statement_id: int) -> None:
    data = _load_config_raw()
    data["job_statements"] = [s for s in data.get("job_statements", []) if s.get("id") != int(statement_id)]
    save_config(data)


def add_job_with_columns(name: str, source_connection_id: int, target_connection_id: int, source_table: str, target_table: str, columns: list[dict]) -> int:
    """Legacy: thêm job với cột (giữ cho tương thích)."""
    data = _load_config_raw()
    jobs, job_cols = data.setdefault("jobs", []), data.setdefault("job_columns", [])
    job_id = _next_id(jobs)
    jobs.append({"id": job_id, "name": name, "source_connection_id": source_connection_id, "target_connection_id": target_connection_id, "source_table": source_table, "target_table": target_table, "sql_file_path": "", "where_clause": "", "batch_size": 1000})
    col_id = _next_id(job_cols)
    for ord, p in enumerate(columns, 1):
        job_cols.append({"id": col_id, "sync_job_id": job_id, "column_order": ord, "source_column_name": p["source"], "target_column_name": p["target"]})
        col_id += 1
    save_config(data)
    return job_id
