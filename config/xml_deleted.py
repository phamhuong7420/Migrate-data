"""
Lưu lịch sử các câu lệnh DELETE đã thực thi khi chạy job đồng bộ.
File scripts/job_delete_his.xml.
Mỗi lần chạy DELETE FROM ... = 1 record <delete_op>.
"""
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

_FILE = Path(__file__).resolve().parent.parent / "scripts" / "job_delete_his.xml"


def _load_tree():
    if not _FILE.exists():
        root = ET.Element("delete_ops")
        return ET.ElementTree(root), root
    try:
        tree = ET.parse(_FILE)
        return tree, tree.getroot()
    except ET.ParseError:
        root = ET.Element("delete_ops")
        return ET.ElementTree(root), root


def _save_tree(tree: ET.ElementTree):
    _FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass
    tree.write(_FILE, encoding="utf-8", xml_declaration=True)


def _next_id(root: ET.Element) -> int:
    ids = [int(el.get("id", 0)) for el in root.findall("delete_op")]
    return max(ids, default=0) + 1


def _el_text(parent: ET.Element, tag: str, value: str):
    el = ET.SubElement(parent, tag)
    el.text = str(value or "")


def _read_text(node: ET.Element, tag: str, default: str = "") -> str:
    child = node.find(tag)
    return (child.text or "").strip() if child is not None else default


def add_delete_op(stmt_id: int, job_name: str, target_conn: str, target_table: str,
                  sql_delete: str, rows_deleted: int) -> int:
    """Ghi 1 record câu lệnh DELETE đã thực thi. Trả về id mới."""
    tree, root = _load_tree()
    new_id = _next_id(root)
    node = ET.SubElement(root, "delete_op", id=str(new_id))
    _el_text(node, "stmt_id", str(stmt_id))
    _el_text(node, "job_name", job_name)
    _el_text(node, "target_connection_name", target_conn)
    _el_text(node, "target_table", target_table)
    _el_text(node, "sql_delete", sql_delete)
    _el_text(node, "rows_deleted", str(rows_deleted))
    _el_text(node, "run_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    _save_tree(tree)
    return new_id


def get_all_delete_ops() -> list:
    """Lấy toàn bộ lịch sử câu lệnh DELETE, mới nhất trước."""
    _, root = _load_tree()
    rows = []
    for n in root.findall("delete_op"):
        rd = _read_text(n, "rows_deleted")
        rows.append({
            "id": int(n.get("id", 0)),
            "stmt_id": int(_read_text(n, "stmt_id") or 0),
            "job_name": _read_text(n, "job_name"),
            "target_connection_name": _read_text(n, "target_connection_name"),
            "target_table": _read_text(n, "target_table"),
            "sql_delete": _read_text(n, "sql_delete"),
            "rows_deleted": int(rd) if rd.isdigit() else None,
            "run_at": _read_text(n, "run_at"),
        })
    return sorted(rows, key=lambda x: x["run_at"], reverse=True)
