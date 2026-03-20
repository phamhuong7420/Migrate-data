"""
Parse file SQL (DELETE + INSERT blocks) → sinh ra job_sync_import.csv
Cách dùng: python scripts/gen_csv_from_sql.py
"""
import re
import csv
from pathlib import Path

SQL_FILE = Path(r'c:\Users\thangnd.ETC\Downloads\insert 126 bảng 1.sql')
OUT_FILE = Path(__file__).parent / 'job_sync_import.csv'

SRC_CONN = 'QLPN_OLD'
TGT_CONN = 'QLPN'

FIELDNAMES = ['id', 'name', 'source_connection_name', 'target_connection_name',
              'source_table', 'target_table', 'sql_text']

def parse(text: str) -> list:
    # Bỏ dòng comment (--)
    lines = [l for l in text.splitlines() if not l.strip().startswith('--')]
    clean = '\n'.join(lines)

    # Ghép mỗi INSERT thành 1 block:
    # INSERT INTO <schema>.<tgt_table> (<cols>)
    # SELECT <exprs>
    # FROM <schema>.<src_table>[<alias>][;]
    pattern = re.compile(
        r'INSERT\s+INTO\s+\w+\s*\.\s*(\w+)\s*'   # tgt_table
        r'\(([^)]+)\)\s*'                          # (cols)
        r'SELECT\s+([\s\S]+?)'                     # SELECT exprs (lazy)
        r'\bFROM\s+\w+\s*\.\s*(\w+)'              # FROM schema.src_table
        r'[^;]*;',                                 # phần còn lại đến ;
        re.IGNORECASE,
    )

    rows = []
    seen = set()  # tránh trùng lặp (file có 1 cặp bị lặp 2 lần)
    for i, m in enumerate(pattern.finditer(clean), start=1):
        tgt_table = m.group(1).strip()
        tgt_cols  = re.sub(r'\s+', ' ', m.group(2).strip())
        src_exprs = re.sub(r'\s+', ' ', m.group(3).strip())
        src_table = m.group(4).strip()

        key = (tgt_table, src_table)
        if key in seen:
            continue
        seen.add(key)

        # Câu lệnh 1 dòng, thay schema bằng tên connection
        sql = (f"INSERT INTO {TGT_CONN}.{tgt_table} ({tgt_cols}) "
               f"SELECT {src_exprs} FROM {SRC_CONN}.{src_table};")

        rows.append({
            'id':                     len(rows) + 1,
            'name':                   f'JOB_{tgt_table}',
            'source_connection_name': SRC_CONN,
            'target_connection_name': TGT_CONN,
            'source_table':           src_table,
            'target_table':           tgt_table,
            'sql_text':               sql,
        })
    return rows


def main():
    text = SQL_FILE.read_text(encoding='utf-8', errors='replace')
    rows = parse(text)

    with open(OUT_FILE, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)

    print(f'Da tao {len(rows)} dong -> {OUT_FILE}')
    for r in rows:
        print(f"  [{r['id']:3d}] {r['target_table']}  <-  {r['source_table']}")


if __name__ == '__main__':
    main()
