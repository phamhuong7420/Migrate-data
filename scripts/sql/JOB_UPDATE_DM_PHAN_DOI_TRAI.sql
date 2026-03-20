INSERT INTO QLPN.DM_PHAN_TRAIS (
    ID,
    PT_MA,
    PT_TEN,
    PT_TRANG_THAI,
    PT_DM_TRAI_GIAM_ID,
    CREATION_TIME,
    IS_DELETED
)
WITH tokens AS (
    /* (a) Các PT_TEN lấy sau dấu ',' (loại 'Chưa phân trại') */
    SELECT
        TRIM(SUBSTR(d.PD_TEN, INSTR(d.PD_TEN, ',') + 1)) AS PT_TEN,
        MIN(d.PD_MA) AS PT_MA,
        MAX(d.PD_TRANG_THAI) AS PT_TRANG_THAI
    FROM QLPN.DM_PHAN_DOIS d
    WHERE INSTR(d.PD_TEN, ',') > 0
      AND TRIM(SUBSTR(d.PD_TEN, INSTR(d.PD_TEN, ',') + 1)) IS NOT NULL
      AND TRIM(SUBSTR(d.PD_TEN, INSTR(d.PD_TEN, ',') + 1)) <> N'Chưa phân trại'
    GROUP BY TRIM(SUBSTR(d.PD_TEN, INSTR(d.PD_TEN, ',') + 1))
    UNION ALL
    /* (b) Bổ sung PT_TEN = 'Chưa phân trại' */
    SELECT
        N'Chưa phân trại' AS PT_TEN,
        MIN(d.PD_MA) AS PT_MA,
        MAX(d.PD_TRANG_THAI) AS PT_TRANG_THAI
    FROM QLPN.DM_PHAN_DOIS d
    WHERE d.PD_TEN = N'Chưa phân trại'
),
tg AS (
    /* Lấy 1 ID trại giam từ QLPN.DM_TRAI_GIAMS */
    SELECT MIN(id) AS PT_DM_TRAI_GIAM_ID
    FROM QLPN.DM_TRAI_GIAMS
    WHERE NVL(is_deleted, 0) = 0
      AND TG_TEN = N'Trại giam Xuân Nguyên'
),
mx AS (
    /* Lấy ID hiện tại lớn nhất để tránh trùng PK */
    SELECT NVL(MAX(id), 0) AS MX_ID
    FROM QLPN.DM_PHAN_TRAIS
)
SELECT
    mx.MX_ID + ROW_NUMBER() OVER (ORDER BY tk.PT_TEN) AS ID,
    tk.PT_MA, /* Tránh TO_CHAR để không bị mismatch charset */
    tk.PT_TEN,
    tk.PT_TRANG_THAI,
    tg.PT_DM_TRAI_GIAM_ID,
    SYSTIMESTAMP AS CREATION_TIME,
    0 AS IS_DELETED
FROM tokens tk
CROSS JOIN tg
CROSS JOIN mx
WHERE NOT EXISTS (
    SELECT 1
    FROM QLPN.DM_PHAN_TRAIS t
    WHERE t.PT_TEN = tk.PT_TEN
);
/* =========================================================
   2) UPDATE QLPN.DM_PHAN_DOIS
   ========================================================= */
MERGE INTO QLPN.DM_PHAN_DOIS d
USING (
    SELECT
        d.ID AS DOI_ID,
        pt.ID AS NEW_PD_DM_PHAN_TRAI_ID
    FROM QLPN.DM_PHAN_DOIS d
    JOIN QLPN.DM_PHAN_TRAIS pt
      ON pt.PT_TEN =
         CASE
           WHEN d.PD_TEN = N'Chưa phân trại' THEN N'Chưa phân trại'
           WHEN INSTR(d.PD_TEN, ',') > 0
             THEN TRIM(SUBSTR(d.PD_TEN, INSTR(d.PD_TEN, ',') + 1))
           ELSE NULL
         END
    WHERE d.PD_TEN = N'Chưa phân trại'
       OR INSTR(d.PD_TEN, ',') > 0
) s
ON (d.ID = s.DOI_ID)
WHEN MATCHED THEN
UPDATE SET d.PD_DM_PHAN_TRAI_ID = s.NEW_PD_DM_PHAN_TRAI_ID;