"""
Lưu lịch sử chạy các script SQL.
File scripts/script_his.xml.
Mỗi lần chạy 1 script = 1 record <run>.
"""
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

_FILE = Path(__file__).resolve().parent.parent / "scripts" / "script_his.xml"


def _load_tree():
    if not _FILE.exists():
        root = ET.Element("runs")
        return ET.ElementTree(root), root
    try:
        tree = ET.parse(_FILE)
        return tree, tree.getroot()
    except ET.ParseError:
        root = ET.Element("runs")
        return ET.ElementTree(root), root


def _save_tree(tree: ET.ElementTree):
    _FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass
    tree.write(_FILE, encoding="utf-8", xml_declaration=True)


def _next_id(root: ET.Element) -> int:
    ids = [int(el.get("id", 0)) for el in root.findall("run")]
    return max(ids, default=0) + 1


def _el_text(parent: ET.Element, tag: str, value: str):
    el = ET.SubElement(parent, tag)
    el.text = str(value or "")


def _read_text(node: ET.Element, tag: str, default: str = "") -> str:
    child = node.find(tag)
    return (child.text or "").strip() if child is not None else default


def add_script_run(filename: str, conn_id: int, conn_name: str,
                   status: str, message: str = "") -> int:
    """Ghi 1 record lịch sử chạy script. Trả về id mới."""
    tree, root = _load_tree()
    new_id = _next_id(root)
    node = ET.SubElement(root, "run", id=str(new_id))
    _el_text(node, "filename", filename)
    _el_text(node, "connection_id", str(conn_id))
    _el_text(node, "connection_name", conn_name)
    _el_text(node, "run_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    _el_text(node, "status", status)
    _el_text(node, "message", message)
    _save_tree(tree)
    return new_id


def get_all_script_runs() -> list:
    """Lấy toàn bộ lịch sử chạy script, mới nhất trước."""
    _, root = _load_tree()
    rows = []
    for n in root.findall("run"):
        rows.append({
            "id": int(n.get("id", 0)),
            "filename": _read_text(n, "filename"),
            "connection_id": int(_read_text(n, "connection_id") or 0),
            "connection_name": _read_text(n, "connection_name"),
            "run_at": _read_text(n, "run_at"),
            "status": _read_text(n, "status"),
            "message": _read_text(n, "message"),
        })
    return sorted(rows, key=lambda x: x["run_at"], reverse=True)
