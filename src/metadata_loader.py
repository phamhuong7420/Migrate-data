# src/metadata_loader.py
"""
Đọc cấu trúc bảng từ Oracle theo từng kết nối (connection).
- Bảng: SELECT table_name FROM user_tables [WHERE table_name = :f]
- Cột: SELECT column_name, data_type, data_length, nullable FROM user_tab_columns WHERE table_name = :tname
"""
import oracledb

from src.config_loader import get_connection_by_id


def get_tables(connection_id: int, table_name_filter: str | None = None) -> list[str]:
    """
    Lấy danh sách tên bảng của connection. Dùng kết nối tương ứng để chạy:
    SELECT table_name FROM user_tables [WHERE UPPER(table_name) = UPPER(:f)] ORDER BY table_name
    (Ví dụ có lọc: WHERE table_name = 'AN_GIAMS')
    """
    conn_info = get_connection_by_id(connection_id)
    with oracledb.connect(
        dsn=conn_info["dsn"],
        user=conn_info["user"],
        password=conn_info["password"],
    ) as conn:
        with conn.cursor() as cur:
            if table_name_filter and table_name_filter.strip():
                cur.execute(
                    """
                    SELECT table_name
                    FROM user_tables
                    WHERE UPPER(table_name) LIKE UPPER(:f)
                    ORDER BY table_name
                    """,
                    {"f": f"%{table_name_filter.strip()}%"},
                )
            else:
                cur.execute(
                    """
                    SELECT table_name
                    FROM user_tables
                    ORDER BY table_name
                    """
                )
            return [row[0] for row in cur.fetchall()]


def get_columns(connection_id: int, table_name: str) -> list[dict]:
    """
    Lấy danh sách cột của bảng. Chạy đúng câu lệnh:
    SELECT column_name, data_type, data_length, nullable
    FROM user_tab_columns
    WHERE UPPER(table_name) = UPPER(:tname)
    ORDER BY column_id
    (Ví dụ: WHERE table_name = 'DM_BAN_GIAO_TRICH_XUATS')
    """
    conn_info = get_connection_by_id(connection_id)
    with oracledb.connect(
        dsn=conn_info["dsn"],
        user=conn_info["user"],
        password=conn_info["password"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, data_length, nullable
                FROM user_tab_columns
                WHERE UPPER(table_name) = UPPER(:tname)
                ORDER BY column_id
                """,
                {"tname": table_name.strip()},
            )
            rows = cur.fetchall()
            return [
                {
                    "column_name": row[0],
                    "data_type": row[1],
                    "data_length": row[2] if row[2] is not None else None,
                    "nullable": row[3],
                    "column_id": i + 1,
                }
                for i, row in enumerate(rows)
            ]
