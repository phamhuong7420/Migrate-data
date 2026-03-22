-- Script dựng QLPN.DM_PHAN_TRAIS từ QLPN.DM_PHAN_DOIS và map lại QLPN.DM_PHAN_DOIS.PD_DM_PHAN_TRAI_ID
-- Quy tắc:
--   - PT_TEN = phần SAU dấu ',' của PD_TEN
--   - Sau khi map PD_DM_PHAN_TRAI_ID: PD_TEN chỉ giữ phần phân đội (TRƯỚC dấu ','); không có ',' thì giữ nguyên cả chuỗi
--   - Loại PT_TEN = 'Chưa phân trại' trong phần trích và bổ sung riêng
--   - PT_TEN = 'Chưa phân trại' được bổ sung riêng
--   - PT_DM_TRAI_GIAM_ID lấy từ QLPN.DM_TRAI_GIAMS (không dùng QLPN_ETC)
-- Lưu ý:
--   - Script dùng INSERT (NOT EXISTS) cho DM_PHAN_TRAIS
--   - Script dùng MERGE để UPDATE PD_DM_PHAN_TRAI_ID cho DM_PHAN_DOIS
--   - Script dùng UPDATE để chuẩn hóa PD_TEN (phần trước dấu ',')
-- Chạy trên: C10 (Oracle QLPN)
-- ======================================================================
/* =========================================================
   1) INSERT QLPN.DM_PHAN_TRAIS
   ========================================================= */
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
      AND (
        UPPER(TG_TEN) LIKE N'%XUÂN NGUYÊN%'
        OR UPPER(TG_TEN) LIKE N'%XUAN NGUYEN%'
      )
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
/* =========================================================
   3) Chuẩn hóa PD_TEN: chỉ giữ phân đội (trước dấu ','); không có ',' thì giữ nguyên
   (chạy sau MERGE để join map PD_DM_PHAN_TRAI_ID vẫn dùng PD_TEN đầy đủ)
   ========================================================= */
UPDATE QLPN.DM_PHAN_DOIS d
SET d.PD_TEN =
    CASE
        WHEN INSTR(d.PD_TEN, ',') > 0
            THEN TRIM(SUBSTR(d.PD_TEN, 1, INSTR(d.PD_TEN, ',') - 1))
        ELSE d.PD_TEN
    END
WHERE INSTR(d.PD_TEN, ',') > 0;