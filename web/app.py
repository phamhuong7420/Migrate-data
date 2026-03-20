# web/app.py
"""
Ứng dụng web: giao diện khai báo kết nối Oracle. Config đọc/ghi từ file XML (không dùng bảng trong DB).
Chạy: từ thư mục Clone_data, set PYTHONPATH=. rồi chạy python web/app.py
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import json
import os
import re
import queue as _queue
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
import oracledb
from flask import Flask, request, redirect, url_for, render_template, jsonify, Response, stream_with_context

from config.file_config import (
    load_connections_only,
    add_connection as fc_add_connection,
    update_connection as fc_update_connection,
    delete_connection as fc_delete_connection,
    get_connection as fc_get_connection,
)
from config.csv_statements import (
    load_all_statements_csv,
    get_statement_csv,
    add_statement_csv,
    update_statement_csv,
    delete_statement_csv,
    delete_statements_csv_bulk,
)
from config.xml_history import add_history, get_history_by_stmt, get_latest_errors, get_latest_successes
from config.xml_deleted import add_delete_op, get_all_delete_ops
from config.xml_script_history import add_script_run, get_all_script_runs
from src.metadata_loader import get_tables, get_columns

app = Flask(
    __name__,
    template_folder=Path(__file__).parent / "templates",
    static_folder=Path(__file__).parent / "static",
)

# run_id -> {"stop": threading.Event, "conns": dict[int, conn], "lock": Lock}
_active_runs: dict = {}
_history_lock = threading.Lock()   # bảo vệ ghi job_his.xml từ nhiều thread

# script_run_id -> {"stop": Event, "status": str, "result": dict, "lock": Lock}
_script_runs: dict = {}

LARGE_TABLE_THRESHOLD = int(os.environ.get("LARGE_TABLE_THRESHOLD", 100_000))


def _execute_insert(oc, sql_insert: str, progress_cb=None) -> int:
    """
    Thực thi INSERT...SELECT với chiến lược tối ưu cho Oracle:
    - Nhỏ (<=100k dòng) : INSERT thường (conventional path).
    - Lớn (>100k dòng)  : INSERT /*+ APPEND */ (direct-path write).
      Direct-path bỏ qua buffer cache, ghi thẳng vào datafile,
      Oracle đọc source đúng 1 lần — nhanh gấp 3-10× so với ROWNUM chunk.
    progress_cb(total_rows, mode): callback thông báo ('normal' | 'append').
    """
    # Đếm dòng nguồn từ phần SELECT của câu lệnh
    m_sel = re.search(r'\bSELECT\b([\s\S]+)', sql_insert, re.IGNORECASE)
    total_rows = None
    if m_sel:
        try:
            with oc.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM (SELECT{m_sel.group(1)})")
                total_rows = cur.fetchone()[0]
        except Exception:
            total_rows = None  # COUNT thất bại → chạy nguyên câu

    # Kiểm tra đã có hint chưa
    has_hint = bool(re.search(r'INSERT\s*/\*\+', sql_insert, re.IGNORECASE))
    use_append = (total_rows is not None
                  and total_rows > LARGE_TABLE_THRESHOLD
                  and not has_hint)

    if use_append:
        # Chèn APPEND hint sau INSERT: "INSERT /*+ APPEND */ INTO ..."
        sql_to_run = re.sub(
            r'^(INSERT\s+)(INTO\s+)',
            r'\1/*+ APPEND */ \2',
            sql_insert.strip(), count=1, flags=re.IGNORECASE,
        )
    else:
        sql_to_run = sql_insert

    if progress_cb and total_rows is not None:
        progress_cb(total_rows, "append" if use_append else "normal")

    with oc.cursor() as cur:
        cur.execute(sql_to_run)
        return cur.rowcount


def _conn_name_by_id(data: dict, cid: int) -> str:
    for c in data.get("connections", []):
        if c.get("id") == cid:
            return c.get("name") or f"ID {cid}"
    return f"ID {cid}"


def _conn_id_by_name(data: dict, name: str) -> int:
    for c in data.get("connections", []):
        if c.get("name") == name:
            return c.get("id") or 0
    return 0


def _get_connection_info(data: dict, cid: int) -> dict:
    for c in data.get("connections", []):
        if c.get("id") == cid:
            return {
                "dsn": f"{c.get('host', '')}:{c.get('port', 1521)}/{c.get('service_name', '')}",
                "user": c.get("username", ""),
                "password": c.get("password_enc") or "",
            }
    raise ValueError(f"Không tìm thấy kết nối id={cid}")


def _scripts_dir() -> Path:
    """Thư mục chứa các file .sql (mặc định: scripts/sql)."""
    return _root / "scripts" / "sql"


def _notes_file() -> Path:
    """File lưu note theo tên script (scripts/sql_notes.json)."""
    return _root / "scripts" / "sql_notes.json"


def _load_notes() -> dict[str, str]:
    """Đọc notes: { filename: note_text }."""
    path = _notes_file()
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_note(filename: str, note: str) -> None:
    """Lưu note cho 1 file."""
    notes = _load_notes()
    fn = filename.strip()
    if not fn or not fn.endswith(".sql"):
        raise ValueError("Tên file không hợp lệ.")
    if note.strip():
        notes[fn] = note.strip()
    else:
        notes.pop(fn, None)
    path = _notes_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(notes, ensure_ascii=False, indent=2), encoding="utf-8")


def _list_sql_files() -> list[dict]:
    """Liệt kê các file .sql trong thư mục scripts (level 1).
    Loại trừ JOB_DELETE_*.sql (có menu riêng). JOB_PN_LAI_LICHS.sql đứng đầu."""
    scripts_path = _scripts_dir()
    if not scripts_path.is_dir():
        return []
    items: list[dict] = []
    for p in sorted(scripts_path.glob("*.sql")):
        if p.name.startswith("JOB_DELETE_"):
            continue
        try:
            stat = p.stat()
            items.append({
                "name": p.name,
                "size": stat.st_size,
                "mtime": stat.st_mtime,
            })
        except OSError:
            continue
    # JOB_PN_LAI_LICHS.sql đứng đầu
    priority = ["JOB_PN_LAI_LICHS.sql"]
    by_name = {it["name"]: it for it in items}
    ordered: list[dict] = []
    for p in priority:
        if p in by_name:
            ordered.append(by_name.pop(p))
    for name in sorted(by_name.keys()):
        ordered.append(by_name[name])
    return ordered


def _list_delete_scripts() -> list[dict]:
    """Liệt kê các file JOB_DELETE_*.sql trong scripts/sql."""
    scripts_path = _scripts_dir()
    if not scripts_path.is_dir():
        return []
    items: list[dict] = []
    for p in sorted(scripts_path.glob("JOB_DELETE_*.sql")):
        try:
            stat = p.stat()
            items.append({"name": p.name, "size": stat.st_size, "mtime": stat.st_mtime})
        except OSError:
            continue
    return items


# --- Trang chạy script .sql ---
@app.route("/scripts")
def scripts_page():
    message = request.args.get("message")
    message_type = request.args.get("message_type", "success")
    scripts = _list_sql_files()

    connections = []
    try:
        data = load_connections_only()
        for c in sorted(
            [x for x in data.get("connections", []) if x.get("connection_type") == "target"],
            key=lambda x: x.get("id", 0),
        ):
            connections.append(
                {
                    "id": c.get("id"),
                    "name": c.get("name"),
                }
            )
    except Exception as e:
        message = f"Lỗi đọc kết nối: {e}"
        message_type = "error"

    notes_map = _load_notes()
    return render_template(
        "scripts.html",
        active="scripts",
        scripts=scripts,
        connections=connections,
        notes_map=notes_map,
        message=message,
        message_type=message_type,
    )


def _resolve_script_path(filename: str) -> Path | None:
    """Trả về Path nếu file hợp lệ trong scripts/sql, None nếu không."""
    fn = filename.strip()
    if not fn or not fn.endswith(".sql"):
        return None
    scripts_path = _scripts_dir()
    try:
        scripts_path_resolved = scripts_path.resolve()
    except FileNotFoundError:
        return None
    script_file = (scripts_path / fn).resolve()
    if not str(script_file).startswith(str(scripts_path_resolved)):
        return None
    if not script_file.is_file():
        return None
    return script_file


def _validate_new_filename(filename: str) -> tuple[Path | None, str | None]:
    """Trả về (Path, None) nếu tên file hợp lệ để tạo mới, (None, error_msg) nếu không."""
    fn = filename.strip()
    if not fn:
        return (None, "Tên file không được trống.")
    if not fn.lower().endswith(".sql"):
        return (None, "Tên file phải có đuôi .sql")
    if "/" in fn or "\\" in fn or ".." in fn:
        return (None, "Tên file không được chứa đường dẫn.")
    scripts_path = _scripts_dir()
    try:
        scripts_path_resolved = scripts_path.resolve()
    except FileNotFoundError:
        return (None, "Thư mục scripts không tồn tại.")
    script_file = (scripts_path / fn).resolve()
    if not str(script_file).startswith(str(scripts_path_resolved)):
        return (None, "Tên file không hợp lệ.")
    if script_file.exists():
        return (None, "File đã tồn tại.")
    return (script_file, None)


@app.route("/api/scripts/new", methods=["POST"])
def api_script_new():
    """Tạo file script mới."""
    filename = request.form.get("filename", "").strip()
    content = request.form.get("content", "")
    if content is None:
        content = ""
    script_file, err = _validate_new_filename(filename)
    if err:
        return jsonify({"error": err}), 400
    try:
        script_file.write_text(content, encoding="utf-8")
        return jsonify({"ok": True, "filename": filename})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/scripts/<path:filename>", methods=["GET"])
def api_script_get(filename: str):
    """Đọc nội dung file script."""
    script_file = _resolve_script_path(filename)
    if not script_file:
        return jsonify({"error": "File không hợp lệ hoặc không tồn tại."}), 404
    try:
        content = script_file.read_text(encoding="utf-8")
        return jsonify({"content": content})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/scripts/<path:filename>", methods=["DELETE"])
def api_script_delete(filename: str):
    """Xóa file script."""
    script_file = _resolve_script_path(filename)
    if not script_file:
        return jsonify({"error": "File không hợp lệ hoặc không tồn tại."}), 404
    try:
        script_file.unlink()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/scripts/note", methods=["POST"])
def api_script_note():
    """Lưu note cho file script."""
    filename = request.form.get("filename", "").strip()
    note = request.form.get("note", "")
    if note is None:
        note = ""
    if not filename or not filename.endswith(".sql"):
        return jsonify({"error": "Tên file không hợp lệ."}), 400
    if "/" in filename or "\\" in filename or ".." in filename:
        return jsonify({"error": "Tên file không được chứa đường dẫn."}), 400
    try:
        _save_note(filename, note)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _script_run_worker(run_id: str, filename: str, conn_id: int):
    """Worker chạy script trong thread, có thể bị dừng."""
    state = _script_runs.get(run_id)
    if not state:
        return
    stop_evt = state["stop"]
    err_fn, err_msg = _run_one_script_with_stop(filename, conn_id, stop_evt)
    data = load_connections_only()
    targets = [c for c in data.get("connections", []) if c.get("connection_type") == "target"]
    conn_by_id = {c.get("id"): c for c in targets if c.get("id")}
    conn_name = (conn_by_id.get(conn_id) or {}).get("name", "")
    with state["lock"]:
        if err_fn is None:
            add_script_run(filename, conn_id, conn_name, "success", "")
            state["status"] = "success"
            state["result"] = {"ok": True, "filename": filename}
        else:
            status = "stopped" if (err_msg or "").startswith("Đã dừng") else "error"
            if status == "error":
                add_script_run(filename, conn_id, conn_name, "error", err_msg or "Lỗi không xác định.")
            state["status"] = status
            state["result"] = {"ok": False, "error": err_msg or "", "filename": filename}


@app.route("/api/scripts/run-one", methods=["POST"])
def api_script_run_one():
    """Chạy 1 script. Nếu có run_id: chạy async, trả về ngay. Không có: chạy sync như cũ."""
    filename = request.form.get("filename", "").strip()
    conn_id = request.form.get("connection_id", type=int) or 0
    run_id = request.form.get("run_id", "").strip()
    if not filename:
        return jsonify({"ok": False, "error": "Thiếu tên file."}), 400
    if not conn_id:
        return jsonify({"ok": False, "error": "Vui lòng chọn kết nối.", "filename": filename}), 400

    if run_id:
        stop_evt = threading.Event()
        state = {"stop": stop_evt, "status": "running", "result": None, "lock": threading.Lock()}
        _script_runs[run_id] = state
        t = threading.Thread(target=_script_run_worker, args=(run_id, filename, conn_id))
        t.daemon = True
        t.start()
        return jsonify({"run_id": run_id, "status": "running"})

    err_fn, err_msg = _run_one_script(filename, conn_id)
    data = load_connections_only()
    targets = [c for c in data.get("connections", []) if c.get("connection_type") == "target"]
    conn_by_id = {c.get("id"): c for c in targets if c.get("id")}
    conn_name = (conn_by_id.get(conn_id) or {}).get("name", "")
    if err_fn is None:
        add_script_run(filename, conn_id, conn_name, "success", "")
        return jsonify({"ok": True, "filename": filename})
    add_script_run(filename, conn_id, conn_name, "error", err_msg or "Lỗi không xác định.")
    return jsonify({"ok": False, "error": err_msg or "Lỗi không xác định.", "filename": filename}), 500


@app.route("/api/scripts/run-status/<run_id>")
def api_script_run_status(run_id: str):
    """Lấy trạng thái chạy script."""
    state = _script_runs.get(run_id)
    if not state:
        return jsonify({"status": "unknown"}), 404
    with state["lock"]:
        s = state["status"]
        r = state.get("result")
    if s in ("success", "error", "stopped"):
        _script_runs.pop(run_id, None)
        return jsonify({"status": s, "result": r})
    return jsonify({"status": s})


@app.route("/api/scripts/stop/<run_id>", methods=["POST"])
def api_script_stop(run_id: str):
    """Dừng script đang chạy."""
    state = _script_runs.get(run_id)
    if not state:
        return jsonify({"ok": False, "error": "Không tìm thấy run_id."}), 404
    state["stop"].set()
    return jsonify({"ok": True})


@app.route("/api/scripts/<path:filename>", methods=["POST"])
def api_script_save(filename: str):
    """Lưu nội dung file script."""
    script_file = _resolve_script_path(filename)
    if not script_file:
        return jsonify({"error": "File không hợp lệ hoặc không tồn tại."}), 404
    content = request.form.get("content", request.get_data(as_text=True))
    if content is None:
        content = ""
    try:
        script_file.write_text(content, encoding="utf-8")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _run_one_script_with_stop(
    filename: str, conn_id: int, stop_event: threading.Event
) -> tuple[str | None, str | None]:
    """Chạy 1 script, kiểm tra stop_event trước mỗi câu lệnh. Trả về (None, None) nếu thành công."""
    data = load_connections_only()
    targets = [c for c in data.get("connections", []) if c.get("connection_type") == "target"]
    scripts_path = _scripts_dir()
    script_file = (scripts_path / filename).resolve()
    try:
        scripts_path_resolved = scripts_path.resolve()
    except FileNotFoundError:
        return (filename, "Thư mục scripts không tồn tại.")
    if not str(script_file).startswith(str(scripts_path_resolved)):
        return (filename, "Đường dẫn file không hợp lệ.")
    if not script_file.is_file() or script_file.suffix.lower() != ".sql":
        return (filename, "File không tồn tại hoặc không phải .sql.")
    try:
        sql_text = script_file.read_text(encoding="utf-8")
    except Exception as e:
        return (filename, f"Lỗi đọc file: {e}")
    if not sql_text.strip():
        return (filename, "File script trống.")
    statements: list[str] = []
    for raw in re.split(r";\s*(?=$|\n)", sql_text, flags=re.MULTILINE):
        stmt = []
        for line in raw.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            stmt.append(line)
        joined = "\n".join(stmt).strip()
        if joined:
            statements.append(joined)
    if not statements:
        return (filename, "Không tìm thấy câu lệnh SQL hợp lệ.")
    if not any(c.get("id") == conn_id for c in targets):
        return (filename, "Chỉ được phép dùng kết nối loại target.")
    try:
        conn_info = _get_connection_info({"connections": targets}, conn_id)
    except Exception as e:
        return (filename, str(e))
    try:
        with oracledb.connect(
            dsn=conn_info["dsn"],
            user=conn_info["user"],
            password=conn_info["password"],
        ) as oc:
            oc.autocommit = False
            try:
                with oc.cursor() as cur:
                    for stmt in statements:
                        if stop_event.is_set():
                            try:
                                oc.rollback()
                            except Exception:
                                pass
                            return (filename, "Đã dừng theo yêu cầu.")
                        cur.execute(stmt)
                oc.commit()
            except Exception:
                try:
                    oc.rollback()
                except Exception:
                    pass
                raise
        return (None, None)
    except Exception as e:
        return (filename, str(e))


def _run_one_script(filename: str, conn_id: int) -> tuple[str | None, str | None]:
    """Chạy 1 script. Trả về (None, None) nếu thành công, (filename, error_msg) nếu lỗi."""
    data = load_connections_only()
    targets = [c for c in data.get("connections", []) if c.get("connection_type") == "target"]
    scripts_path = _scripts_dir()
    script_file = (scripts_path / filename).resolve()
    try:
        scripts_path_resolved = scripts_path.resolve()
    except FileNotFoundError:
        return (filename, "Thư mục scripts không tồn tại.")
    if not str(script_file).startswith(str(scripts_path_resolved)):
        return (filename, "Đường dẫn file không hợp lệ.")
    if not script_file.is_file() or script_file.suffix.lower() != ".sql":
        return (filename, "File không tồn tại hoặc không phải .sql.")
    try:
        sql_text = script_file.read_text(encoding="utf-8")
    except Exception as e:
        return (filename, f"Lỗi đọc file: {e}")
    if not sql_text.strip():
        return (filename, "File script trống.")
    statements: list[str] = []
    for raw in re.split(r";\s*(?=$|\n)", sql_text, flags=re.MULTILINE):
        stmt = []
        for line in raw.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            stmt.append(line)
        joined = "\n".join(stmt).strip()
        if joined:
            statements.append(joined)
    if not statements:
        return (filename, "Không tìm thấy câu lệnh SQL hợp lệ.")
    if not any(c.get("id") == conn_id for c in targets):
        return (filename, "Chỉ được phép dùng kết nối loại target.")
    try:
        conn_info = _get_connection_info({"connections": targets}, conn_id)
    except Exception as e:
        return (filename, str(e))
    try:
        with oracledb.connect(
            dsn=conn_info["dsn"],
            user=conn_info["user"],
            password=conn_info["password"],
        ) as oc:
            oc.autocommit = False
            try:
                with oc.cursor() as cur:
                    for stmt in statements:
                        cur.execute(stmt)
                oc.commit()
            except Exception:
                try:
                    oc.rollback()
                except Exception:
                    pass
                raise
        return (None, None)
    except Exception as e:
        return (filename, str(e))


@app.route("/scripts/run", methods=["POST"])
def scripts_run():
    filenames = request.form.getlist("filename")
    conn_ids_raw = request.form.getlist("connection_id")
    conn_ids: list[int] = []
    for v in conn_ids_raw:
        try:
            conn_ids.append(int(v))
        except (ValueError, TypeError):
            conn_ids.append(0)

    if not filenames:
        redirect_to = request.form.get("redirect", "").strip()
        target = "scripts_run_delete_pn_lai_lichs" if redirect_to == "delete" else "scripts_page"
        return redirect(url_for(target, message="Thiếu tên file script.", message_type="error"))

    data = load_connections_only()
    targets = [c for c in data.get("connections", []) if c.get("connection_type") == "target"]

    success_count = 0
    first_error: tuple[str, str] | None = None
    conn_by_id = {c.get("id"): c for c in targets if c.get("id")}

    for i, filename in enumerate(filenames):
        fn = filename.strip()
        if not fn:
            continue
        cid = conn_ids[i] if i < len(conn_ids) else (conn_ids[0] if conn_ids else 0)
        if not cid:
            add_script_run(fn, cid, conn_by_id.get(cid, {}).get("name", ""), "error", "Vui lòng chọn kết nối.")
            first_error = (fn, "Vui lòng chọn kết nối.")
            break
        conn_name = (conn_by_id.get(cid) or {}).get("name", "")
        err_fn, err_msg = _run_one_script(fn, cid)
        if err_fn is None:
            add_script_run(fn, cid, conn_name, "success", "")
            success_count += 1
        else:
            add_script_run(fn, cid, conn_name, "error", err_msg or "Lỗi không xác định.")
            first_error = (err_fn, err_msg or "Lỗi không xác định.")
            break

    redirect_to = request.form.get("redirect", "").strip()
    target_route = "scripts_run_delete_pn_lai_lichs" if redirect_to == "delete" else "scripts_page"

    if first_error:
        return redirect(
            url_for(
                target_route,
                message=f"Lỗi khi chạy {first_error[0]}: {first_error[1]}",
                message_type="error",
            )
        )
    total = len([f for f in filenames if f.strip()])
    msg = f"Đã chạy {success_count} script." if total > 1 else f"Đã chạy script: {filenames[0].strip()}"
    return redirect(url_for(target_route, message=msg, message_type="success"))


@app.route("/scripts/history")
def scripts_history():
    rows = get_all_script_runs()
    return render_template("script_history.html", active="scripts_history", rows=rows)


@app.route("/scripts/run-delete-pn-lai-lichs")
def scripts_run_delete_pn_lai_lichs():
    """Danh sách job JOB_DELETE_*.sql, giao diện giống Chạy script SQL."""
    scripts = _list_delete_scripts()
    connections = []
    try:
        data = load_connections_only()
        for c in sorted(
            [x for x in data.get("connections", []) if x.get("connection_type") == "target"],
            key=lambda x: x.get("id", 0),
        ):
            connections.append({"id": c.get("id"), "name": c.get("name")})
    except Exception:
        pass
    notes_map = _load_notes()
    message = request.args.get("message")
    message_type = request.args.get("message_type", "success")
    return render_template(
        "run_delete_scripts.html",
        active="scripts_run_delete_pn_lai_lichs",
        scripts=scripts,
        connections=connections,
        notes_map=notes_map,
        message=message,
        message_type=message_type,
    )


# --- Trang chủ: Danh sách bảng cần đồng bộ ---
@app.route("/")
def index():
    message = request.args.get("message")
    message_type = request.args.get("message_type", "success")
    rows = []
    try:
        rows = load_all_statements_csv()
    except Exception as e:
        message = f"Lỗi đọc config: {e}"
        message_type = "error"
    return render_template("index.html", active="jobs", rows=rows, message=message, message_type=message_type)


# --- Trang Tạo kết nối ---
@app.route("/connection")
def connection_page():
    message = request.args.get("message")
    message_type = request.args.get("message_type", "success")
    connections = []
    try:
        data = load_connections_only()
        for c in sorted(data.get("connections", []), key=lambda x: x.get("id", 0)):
            connections.append({
                "id": c.get("id"), "name": c.get("name"), "connection_type": c.get("connection_type"),
                "host": c.get("host"), "port": c.get("port"), "service_name": c.get("service_name"), "username": c.get("username"),
            })
    except Exception as e:
        message = f"Lỗi: {e}"
        message_type = "error"
    return render_template("connection.html", active="connection", connections=connections, message=message, message_type=message_type)


@app.route("/connection/add", methods=["POST"])
def add_connection():
    name = request.form.get("name", "").strip()
    connection_type = request.form.get("connection_type", "source").strip()
    host = request.form.get("host", "").strip()
    port = request.form.get("port", "1521").strip()
    service_name = request.form.get("service_name", "").strip()
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    if not all([name, host, service_name, username]):
        return redirect(url_for("connection_page", message="Vui lòng điền đủ: Tên, Host, Service name, Username.", message_type="error"))
    try:
        fc_add_connection(name, connection_type, host, int(port), service_name, username, password or None)
        return redirect(url_for("connection_page", message="Đã thêm kết nối."))
    except Exception as e:
        return redirect(url_for("connection_page", message=f"Lỗi: {e}", message_type="error"))


@app.route("/connection/<int:id>/edit")
def edit_connection(id):
    try:
        conn = fc_get_connection(id)
        if not conn:
            return redirect(url_for("connection_page", message="Không tìm thấy kết nối.", message_type="error"))
        conn = {"id": conn["id"], "name": conn["name"], "connection_type": conn["connection_type"],
                "host": conn["host"], "port": conn["port"], "service_name": conn["service_name"], "username": conn["username"]}
        return render_template("connection_edit.html", active="connection", conn=conn)
    except Exception as e:
        return redirect(url_for("connection_page", message=f"Lỗi: {e}", message_type="error"))


@app.route("/connection/<int:id>/update", methods=["POST"])
def update_connection(id):
    name = request.form.get("name", "").strip()
    connection_type = request.form.get("connection_type", "source").strip()
    host = request.form.get("host", "").strip()
    port = request.form.get("port", "1521").strip()
    service_name = request.form.get("service_name", "").strip()
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    if not all([name, host, service_name, username]):
        return redirect(url_for("connection_page", message="Vui lòng điền đủ các trường.", message_type="error"))
    try:
        fc_update_connection(id, name, connection_type, host, int(port), service_name, username, password if password else None)
        return redirect(url_for("connection_page", message="Đã cập nhật kết nối."))
    except Exception as e:
        return redirect(url_for("connection_page", message=f"Lỗi: {e}", message_type="error"))


@app.route("/connection/<int:id>/delete", methods=["POST"])
def delete_connection(id):
    try:
        fc_delete_connection(id)
        return redirect(url_for("connection_page", message="Đã xóa kết nối."))
    except Exception as e:
        return redirect(url_for("connection_page", message=f"Lỗi: {e}", message_type="error"))


# --- API đọc cấu trúc bảng từ source/target ---
@app.route("/api/connections/<int:conn_id>/tables")
def api_tables(conn_id):
    table_name_filter = request.args.get("table_name", "").strip() or None
    try:
        tables = get_tables(conn_id, table_name_filter=table_name_filter)
        return jsonify({"tables": tables})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<int:conn_id>/columns")
def api_columns(conn_id):
    table = request.args.get("table", "").strip()
    if not table:
        return jsonify({"error": "Thiếu tham số table"}), 400
    try:
        columns = get_columns(conn_id, table)
        return jsonify({"columns": columns})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- Thêm bảng mới ---
@app.route("/job/new")
def job_new_page():
    data = load_connections_only()
    all_conns = data.get("connections", [])
    source_connections = [{"id": c.get("id"), "name": c.get("name")} for c in all_conns if c.get("connection_type") == "source"]
    target_connections = [{"id": c.get("id"), "name": c.get("name")} for c in all_conns if c.get("connection_type") == "target"]
    message = request.args.get("message")
    message_type = request.args.get("message_type", "success")
    return render_template("job_new.html", active="job_new", source_connections=source_connections, target_connections=target_connections, message=message, message_type=message_type)


@app.route("/job/create", methods=["POST"])
def job_create():
    job_name = request.form.get("job_name", "").strip()
    source_connection_id = request.form.get("source_connection_id", type=int)
    target_connection_id = request.form.get("target_connection_id", type=int)
    selected_src = request.form.get("selected_source_tables", "").strip()
    selected_tgt = request.form.get("selected_target_tables", "").strip()
    source_tables = [x.strip() for x in selected_src.split(",") if x.strip()]
    target_tables = [x.strip() for x in selected_tgt.split(",") if x.strip()]
    if not source_connection_id or not target_connection_id:
        return redirect(url_for("job_new_page", message="Vui lòng chọn kết nối nguồn và đích.", message_type="error"))
    try:
        data = load_connections_only()
        src_conn_name = _conn_name_by_id(data, source_connection_id)
        tgt_conn_name = _conn_name_by_id(data, target_connection_id)
        pairs = list(zip(source_tables, target_tables))
        if not pairs:
            pairs = [("", "")]
        last_id = None
        for src_tbl, tgt_tbl in pairs:
            last_id = add_statement_csv(src_conn_name, tgt_conn_name, src_tbl, tgt_tbl, name=job_name)
        return redirect(url_for("man_hinh_them_moi", id=last_id,
                                message="Đã tạo. Tạo mapping cột và lưu câu lệnh SQL."))
    except Exception as e:
        return redirect(url_for("job_new_page", message=f"Lỗi: {e}", message_type="error"))


def _load_stmt_for_page(stmt_id: int):
    """Đọc statement từ job_sync.csv và bổ sung connection_id tra từ sync_config.xml."""
    stmt = get_statement_csv(stmt_id)
    if not stmt:
        return None
    data = load_connections_only()
    stmt["source_connection_id"] = _conn_id_by_name(data, stmt["source_connection_name"])
    stmt["target_connection_id"] = _conn_id_by_name(data, stmt["target_connection_name"])
    return stmt


# --- Màn hình sửa / thêm câu lệnh ---
@app.route("/job/<int:id>/them-moi")
def man_hinh_them_moi(id):
    stmt = _load_stmt_for_page(id)
    if not stmt:
        return redirect(url_for("index", message="Không tìm thấy bản ghi.", message_type="error"))
    message = request.args.get("message")
    message_type = request.args.get("message_type", "success")
    return render_template("man_hinh_them_moi.html", active="jobs", stmt=stmt,
                           message=message, message_type=message_type)


@app.route("/job/<int:id>/edit")
def job_edit_page(id):
    return redirect(url_for("man_hinh_them_moi", id=id))


# --- Lưu câu lệnh SQL (UPDATE theo id, không insert mới) ---
@app.route("/job/<int:stmt_id>/save", methods=["POST"])
def statement_save(stmt_id):
    sql_text = request.form.get("sql_text", "").strip()
    if not sql_text:
        return redirect(url_for("man_hinh_them_moi", id=stmt_id,
                                message="Câu lệnh không được trống.", message_type="error"))
    try:
        update_statement_csv(stmt_id, sql_text)
        return redirect(url_for("man_hinh_them_moi", id=stmt_id, message="Đã lưu câu lệnh."))
    except Exception as e:
        return redirect(url_for("man_hinh_them_moi", id=stmt_id,
                                message=f"Lỗi: {e}", message_type="error"))


# --- Chạy các câu lệnh SQL đã chọn ---
@app.route("/jobs/run", methods=["POST"])
def jobs_run():
    stmt_ids = request.form.getlist("stmt_ids", type=int)
    if not stmt_ids:
        return redirect(url_for("index", message="Chưa chọn job nào để chạy.", message_type="error"))

    data = load_connections_only()
    results = []

    for sid in stmt_ids:
        # 1. Đọc thông tin từ job_sync.csv
        stmt = get_statement_csv(sid)
        if not stmt:
            results.append({"id": sid, "name": "?", "table": "?", "status": "error",
                            "delete_rows": None, "insert_rows": None,
                            "message": "Không tìm thấy bản ghi trong job_sync.csv."})
            continue

        label        = stmt.get("name") or stmt.get("target_table") or f"ID {sid}"
        target_table = stmt.get("target_table", "").strip()
        tgt_schema   = stmt.get("target_connection_name", "").strip()
        sql_insert   = stmt.get("sql_text", "").strip().rstrip(";")

        if not target_table:
            results.append({"id": sid, "name": label, "table": "?", "status": "error",
                            "delete_rows": None, "insert_rows": None,
                            "message": "Bảng đích trống."})
            continue
        if not sql_insert:
            results.append({"id": sid, "name": label, "table": target_table, "status": "error",
                            "delete_rows": None, "insert_rows": None,
                            "message": "Câu lệnh INSERT trống."})
            continue

        # 2. Tra thông tin kết nối target
        tgt_conn_id = _conn_id_by_name(data, tgt_schema)
        if not tgt_conn_id:
            results.append({"id": sid, "name": label, "table": target_table, "status": "error",
                            "delete_rows": None, "insert_rows": None,
                            "message": f"Không tìm thấy kết nối: {tgt_schema}"})
            continue

        # 3. Kết nối Oracle target: DELETE → commit → INSERT (APPEND nếu lớn) → commit
        sql_delete = f"DELETE FROM {tgt_schema}.{target_table}"
        try:
            conn_info = _get_connection_info(data, tgt_conn_id)
            with oracledb.connect(dsn=conn_info["dsn"], user=conn_info["user"],
                                  password=conn_info["password"]) as conn:
                conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute(sql_delete)
                    deleted = cur.rowcount
                conn.commit()
                add_delete_op(sid, label, tgt_schema, target_table, sql_delete, deleted)
                try:
                    inserted = _execute_insert(conn, sql_insert)
                    conn.commit()
                except Exception:
                    try: conn.rollback()
                    except Exception: pass
                    raise
            add_history(sid, label, target_table, "success", deleted, inserted, "")
            results.append({"id": sid, "name": label, "table": target_table, "status": "success",
                            "delete_rows": deleted, "insert_rows": inserted, "message": ""})
        except Exception as e:
            err_msg = str(e)
            add_history(sid, label, target_table, "error", None, None, err_msg)
            results.append({"id": sid, "name": label, "table": target_table, "status": "error",
                            "delete_rows": None, "insert_rows": None, "message": err_msg})

    return render_template("run_result.html", results=results)


# --- Stream tiến độ chạy job (SSE, đa luồng) ---
MAX_WORKERS = 4

@app.route("/api/jobs/run-stream")
def jobs_run_stream():
    ids_str  = request.args.get("ids", "")
    workers  = min(MAX_WORKERS, max(1, request.args.get("workers", MAX_WORKERS, type=int)))
    stmt_ids = [int(x) for x in ids_str.split(",") if x.strip().lstrip("-").isdigit()]

    run_id    = uuid.uuid4().hex[:12]
    stop_evt  = threading.Event()
    conns_map: dict = {}          # sid -> active oracledb connection
    conns_lock = threading.Lock()
    run_state = {"stop": stop_evt, "conns": conns_map, "lock": conns_lock}
    _active_runs[run_id] = run_state

    def _sse(payload: dict) -> str:
        return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

    def _run_one(idx: int, sid: int, data: dict, total: int, evt_q: _queue.Queue):
        """Chạy 1 job trong worker thread, đẩy events vào queue."""
        if stop_evt.is_set():
            evt_q.put({"type": "progress", "index": idx, "total": total,
                       "id": sid, "name": f"ID {sid}", "table": "?",
                       "status": "error", "message": "Bị dừng bởi người dùng."})
            return

        stmt = get_statement_csv(sid)
        if not stmt:
            evt_q.put({"type": "progress", "index": idx, "total": total,
                       "id": sid, "name": f"ID {sid}", "table": "?",
                       "status": "error", "message": "Không tìm thấy bản ghi."})
            return

        label        = stmt.get("name") or stmt.get("target_table") or f"ID {sid}"
        target_table = stmt.get("target_table", "").strip()
        tgt_schema   = stmt.get("target_connection_name", "").strip()
        sql_insert   = stmt.get("sql_text", "").strip().rstrip(";")

        # Thông báo bắt đầu job này
        evt_q.put({"type": "running", "index": idx, "total": total,
                   "id": sid, "name": label, "table": target_table})

        def _err(msg):
            with _history_lock:
                add_history(sid, label, target_table, "error", None, None, msg)
            evt_q.put({"type": "progress", "index": idx, "total": total,
                       "id": sid, "name": label, "table": target_table,
                       "status": "error", "message": msg})

        if not target_table:
            return _err("Bảng đích trống.")
        if not sql_insert:
            return _err("Câu lệnh INSERT trống.")

        tgt_conn_id = _conn_id_by_name(data, tgt_schema)
        if not tgt_conn_id:
            return _err(f"Không tìm thấy kết nối: {tgt_schema}")

        sql_delete = f"DELETE FROM {tgt_schema}.{target_table}"
        try:
            conn_info = _get_connection_info(data, tgt_conn_id)
            with oracledb.connect(dsn=conn_info["dsn"], user=conn_info["user"],
                                  password=conn_info["password"]) as oc:
                oc.autocommit = False
                with conns_lock:
                    conns_map[sid] = oc
                try:
                    # 1. DELETE
                    with oc.cursor() as cur:
                        cur.execute(sql_delete)
                        deleted = cur.rowcount
                    oc.commit()
                    with _history_lock:
                        add_delete_op(sid, label, tgt_schema, target_table, sql_delete, deleted)

                    # 2. INSERT (có thể chia chunk nếu > CHUNK_SIZE dòng)
                    def _insert_cb(total_rows, mode):
                        if mode == "append":
                            evt_q.put({
                                "type":       "chunk",
                                "id":         sid,
                                "total_rows": total_rows,
                                "mode":       "append",
                            })

                    inserted = _execute_insert(oc, sql_insert, progress_cb=_insert_cb)
                    oc.commit()

                except Exception:
                    try: oc.rollback()
                    except Exception: pass
                    raise
                finally:
                    with conns_lock:
                        conns_map.pop(sid, None)

            with _history_lock:
                add_history(sid, label, target_table, "success", deleted, inserted, "")
            evt_q.put({"type": "progress", "index": idx, "total": total,
                       "id": sid, "name": label, "table": target_table,
                       "status": "success", "delete_rows": deleted, "insert_rows": inserted,
                       "message": ""})
        except Exception as e:
            is_cancel = stop_evt.is_set()
            msg = "Bị dừng bởi người dùng." if is_cancel else str(e)
            with _history_lock:
                add_history(sid, label, target_table, "error", None, None, msg)
            evt_q.put({"type": "progress", "index": idx, "total": total,
                       "id": sid, "name": label, "table": target_table,
                       "status": "error", "message": msg})

    def generate():
        try:
            yield _sse({"type": "started", "run_id": run_id, "workers": workers})

            if not stmt_ids:
                yield _sse({"type": "error", "message": "Không có job nào được chọn."})
                return

            data     = load_connections_only()
            total    = len(stmt_ids)
            evt_q    = _queue.Queue()
            done_cnt = 0

            with ThreadPoolExecutor(max_workers=workers) as pool:
                for idx, sid in enumerate(stmt_ids, start=1):
                    pool.submit(_run_one, idx, sid, data, total, evt_q)

                # Đọc events từ queue cho đến khi tất cả job hoàn tất
                while done_cnt < total:
                    try:
                        event = evt_q.get(timeout=300)
                    except _queue.Empty:
                        break  # timeout (5 phút/job), thoát

                    yield _sse(event)

                    if event.get("type") == "progress":
                        done_cnt += 1
                        # Nếu đã dừng và tất cả thread đang chạy đã phản hồi
                        if stop_evt.is_set() and done_cnt >= total:
                            break

            if stop_evt.is_set():
                yield _sse({"type": "stopped"})
            else:
                yield _sse({"type": "done", "total": total})

        finally:
            with conns_lock:
                conns_map.clear()
            _active_runs.pop(run_id, None)

    return Response(
        stream_with_context(generate()),
        content_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# --- Dừng run đang chạy ---
@app.route("/api/jobs/stop/<run_id>", methods=["POST"])
def jobs_stop(run_id):
    run = _active_runs.get(run_id)
    if not run:
        return jsonify({"ok": False, "message": "Run không còn tồn tại hoặc đã kết thúc."})
    run["stop"].set()
    # Cancel tất cả Oracle connection đang blocking trong các thread
    with run["lock"]:
        for oc in list(run["conns"].values()):
            try: oc.cancel()
            except Exception: pass
    return jsonify({"ok": True})


# --- API lưu SQL nhanh (dùng cho inline edit ở index) ---
@app.route("/api/job/<int:stmt_id>/sql", methods=["POST"])
def api_save_sql(stmt_id):
    sql_text = request.form.get("sql_text", "").strip()
    if not sql_text:
        return jsonify({"error": "Câu lệnh không được trống."}), 400
    try:
        update_statement_csv(stmt_id, sql_text)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- Danh sách job lỗi gần nhất ---
@app.route("/jobs/errors")
def jobs_errors():
    errors = get_latest_errors()
    return render_template("job_errors.html", active="jobs_errors", errors=errors)


# --- Lịch sử chạy của 1 job ---
@app.route("/job/<int:stmt_id>/history")
def job_history(stmt_id):
    stmt = get_statement_csv(stmt_id)
    if not stmt:
        return redirect(url_for("index", message="Không tìm thấy bản ghi.", message_type="error"))
    histories = get_history_by_stmt(stmt_id)
    # Quay lại trang trước (errors hoặc index)
    referrer  = request.referrer or ""
    back_url  = referrer if referrer else url_for("index")
    return render_template("job_history.html", stmt=stmt, histories=histories, back_url=back_url)


# --- Xóa 1 dòng khỏi job_sync.csv ---
@app.route("/job/<int:stmt_id>/delete", methods=["POST"])
def statement_delete(stmt_id):
    try:
        delete_statement_csv(stmt_id)
        return redirect(url_for("index", message="Đã xóa."))
    except Exception as e:
        return redirect(url_for("index", message=f"Lỗi: {e}", message_type="error"))


# --- Xóa nhiều dòng khỏi job_sync.csv ---
@app.route("/jobs/delete-bulk", methods=["POST"])
def jobs_delete_bulk():
    stmt_ids = request.form.getlist("stmt_ids", type=int)
    if not stmt_ids:
        return redirect(url_for("index", message="Chưa chọn job nào để xóa.", message_type="error"))
    try:
        delete_statements_csv_bulk(stmt_ids)
        return redirect(url_for("index", message=f"Đã xóa {len(stmt_ids)} job."))
    except Exception as e:
        return redirect(url_for("index", message=f"Lỗi: {e}", message_type="error"))


# --- Thống kê bảng chạy thành công ---
@app.route("/jobs/stats-success")
def jobs_stats_success():
    rows = get_latest_successes()
    return render_template("job_stats_success.html", active="jobs_stats", rows=rows)


# --- Lịch sử câu lệnh DELETE đã thực thi ---
@app.route("/jobs/deleted-history")
def jobs_deleted_history():
    rows = get_all_delete_ops()
    return render_template("job_deleted_history.html", active="jobs_deleted", rows=rows)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
