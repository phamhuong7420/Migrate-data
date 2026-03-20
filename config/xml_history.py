"""
Quản lý lịch sử chạy job, lưu trong scripts/job_his.xml.
Mỗi lần chạy một job = 1 record <history>.
"""
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

_HIS_FILE = Path(__file__).resolve().parent.parent / "scripts" / "job_his.xml"


def _load_tree():
    if not _HIS_FILE.exists():
        root = ET.Element("histories")
        return ET.ElementTree(root), root
    try:
        tree = ET.parse(_HIS_FILE)
        return tree, tree.getroot()
    except ET.ParseError:
        root = ET.Element("histories")
        return ET.ElementTree(root), root


def _save_tree(tree: ET.ElementTree):
    _HIS_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass
    tree.write(_HIS_FILE, encoding="utf-8", xml_declaration=True)


def _next_id(root: ET.Element) -> int:
    ids = [int(el.get("id", 0)) for el in root.findall("history")]
    return max(ids, default=0) + 1


def _el_text(parent: ET.Element, tag: str, value: str):
    el = ET.SubElement(parent, tag)
    el.text = str(value or "")


def _read_text(node: ET.Element, tag: str, default: str = "") -> str:
    child = node.find(tag)
    return (child.text or "").strip() if child is not None else default


def add_history(stmt_id: int, job_name: str, target_table: str,
                status: str, delete_rows, insert_rows, message: str = "") -> int:
    """Ghi 1 record lịch sử. Trả về id mới."""
    tree, root = _load_tree()
    new_id = _next_id(root)
    node = ET.SubElement(root, "history", id=str(new_id))
    _el_text(node, "stmt_id",      str(stmt_id))
    _el_text(node, "job_name",     job_name)
    _el_text(node, "target_table", target_table)
    _el_text(node, "run_at",       datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    _el_text(node, "status",       status)
    _el_text(node, "delete_rows",  "" if delete_rows is None else str(delete_rows))
    _el_text(node, "insert_rows",  "" if insert_rows is None else str(insert_rows))
    _el_text(node, "message",      message)
    _save_tree(tree)
    return new_id


def _node_to_dict(node: ET.Element) -> dict:
    dr = _read_text(node, "delete_rows")
    ir = _read_text(node, "insert_rows")
    return {
        "id":           int(node.get("id", 0)),
        "stmt_id":      int(_read_text(node, "stmt_id") or 0),
        "job_name":     _read_text(node, "job_name"),
        "target_table": _read_text(node, "target_table"),
        "run_at":       _read_text(node, "run_at"),
        "status":       _read_text(node, "status"),
        "delete_rows":  int(dr) if dr.isdigit() else None,
        "insert_rows":  int(ir) if ir.isdigit() else None,
        "message":      _read_text(node, "message"),
    }


def get_history_by_stmt(stmt_id: int) -> list:
    """Lấy lịch sử của 1 stmt_id, sắp xếp mới nhất trước."""
    _, root = _load_tree()
    rows = [_node_to_dict(n) for n in root.findall("history")
            if _read_text(n, "stmt_id") == str(stmt_id)]
    return sorted(rows, key=lambda x: x["run_at"], reverse=True)


def get_all_history() -> list:
    """Lấy toàn bộ lịch sử, mới nhất trước."""
    _, root = _load_tree()
    rows = [_node_to_dict(n) for n in root.findall("history")]
    return sorted(rows, key=lambda x: x["run_at"], reverse=True)


def get_latest_errors() -> list:
    """Lấy lần chạy gần nhất của mỗi job, lọc ra những lần bị lỗi."""
    _, root = _load_tree()
    all_rows = [_node_to_dict(n) for n in root.findall("history")]
    # Lấy lần chạy gần nhất của mỗi stmt_id
    latest: dict[int, dict] = {}
    for row in sorted(all_rows, key=lambda x: x["run_at"]):
        latest[row["stmt_id"]] = row
    # Chỉ trả về những job có lần chạy gần nhất là lỗi
    errors = [r for r in latest.values() if r["status"] == "error"]
    return sorted(errors, key=lambda x: x["run_at"], reverse=True)


def get_latest_successes() -> list:
    """Lấy lần chạy gần nhất của mỗi job có status=success, kèm số dòng insert."""
    _, root = _load_tree()
    all_rows = [_node_to_dict(n) for n in root.findall("history")]
    success_rows = [r for r in all_rows if r["status"] == "success"]
    # Lấy lần chạy gần nhất của mỗi stmt_id (chỉ success)
    latest: dict[int, dict] = {}
    for row in sorted(success_rows, key=lambda x: x["run_at"]):
        latest[row["stmt_id"]] = row
    return sorted(latest.values(), key=lambda x: x["run_at"], reverse=True)
