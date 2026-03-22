-- =============================================================================
-- SCRIPT_MIEN_HINH_PHAT.sql — Nạp MIEN_HINH_PHATS từ QLPN_OLD.PN_MIEN_HINH_PHAT
-- Quy ước migrate: .cursor/rules/sql-migration-qlpn-conventions.mdc
--   - Không INSERT: ID, CREATOR_USER_ID, LAST_MODIFICATION_TIME,
--     LAST_MODIFIER_USER_ID, DELETER_USER_ID, DELETION_TIME
--   - MA_DP* → MAPPING_PROVINCE + DM_TINH_THANH_PHO (ID tỉnh), không DM_DON_VI_HANH_CHINH
-- MHP_MATM_* : MA_MUC_AN_TRUOC_MIEN → DM_MA_THOI_GIANS (MTG_MA), tách MTG_TITLE → năm/tháng/ngày
-- MHP_THM_*  : MA_MUC_THOI_HAN_MIEN → DM_MA_THOI_GIANS (MTG_MA), tách MTG_TITLE → năm/tháng/ngày
--   Thiếu thành phần trong title → 0 (vd: "2 năm 3 ngày" → month=0; "3 tháng" → year=0, day=0).
--   Không có dòng DM / MTG_TITLE NULL → cả 3 cột YEAR/MONTH/DAY đều NULL.
-- MHP_TOI_DE_NGHI_MIEN / MHP_TOI_DANH_MIEN: danh sách mã tách bằng ; → DM_TOI_DANHS.ID,
--   Định dạng lưu: LISTAGG(TO_CHAR(ID), ',') ORDER BY seq — không [] và không khoảng trắng sau dấu phẩy.
-- =============================================================================
TRUNCATE TABLE QLPN.MIEN_HINH_PHATS;
INSERT /*+ APPEND */ INTO QLPN.MIEN_HINH_PHATS (
    MHP_SO_DE_NGHI,
    MHP_NGAY_DE_NGHI,
    MHP_TOI_DE_NGHI_MIEN,
    MHP_TRUONG_HOP_MIEN,
    MHP_SO_QUYET_DINH,
    MHP_NGAY_QUYET_DINH,
    MHP_TOA_QUYET_DINH,
    MHP_DIA_PHUONG_QD,
    MHP_TOI_DANH_MIEN,
    MHP_NGAY_TU_DO,
    MHP_MATM_YEAR,
    MHP_MATM_MONTH,
    MHP_MATM_DAY,
    MHP_THM_YEAR,
    MHP_THM_MONTH,
    MHP_THM_DAY,
    CREATION_TIME,
    IS_DELETED,
    MHP_NHAN_XET,
    PN_LAI_LICH_ID
)
WITH mien_hp_src AS (
    SELECT
        pmhp.CV_DN_MIEN_SO AS MHP_SO_DE_NGHI,
        CAST(pmhp.CV_DN_MIEN_NGAY AS TIMESTAMP(7)) AS MHP_NGAY_DE_NGHI,
        CASE
            WHEN TRIM(pmhp.MA_TOI_DANH_TRUOC_MIEN) IS NULL THEN CAST(NULL AS NVARCHAR2(510))
            ELSE CAST(
                (SELECT LISTAGG(TO_CHAR(dtd.ID), ',') WITHIN GROUP (ORDER BY tok.seq)
                   FROM (
                        SELECT TRIM(REGEXP_SUBSTR(pmhp.MA_TOI_DANH_TRUOC_MIEN, '[^;]+', 1, LEVEL)) AS td_ma,
                               LEVEL AS seq
                          FROM dual
                        CONNECT BY LEVEL <= REGEXP_COUNT(pmhp.MA_TOI_DANH_TRUOC_MIEN, ';') + 1
                           AND TRIM(REGEXP_SUBSTR(pmhp.MA_TOI_DANH_TRUOC_MIEN, '[^;]+', 1, LEVEL)) IS NOT NULL
                       ) tok
                  INNER JOIN QLPN.DM_TOI_DANHS dtd
                     ON TRIM(dtd.TD_MA) = tok.td_ma
                    AND NVL(dtd.IS_DELETED, 0) = 0
                ) AS NVARCHAR2(510)
             )
        END AS MHP_TOI_DE_NGHI_MIEN,
        dthm.ID AS MHP_TRUONG_HOP_MIEN,
        pmhp.QD_MIEN_SO AS MHP_SO_QUYET_DINH,
        CAST(pmhp.QD_MIEN_NGAY AS TIMESTAMP(7)) AS MHP_NGAY_QUYET_DINH,
        dcq.ID AS MHP_TOA_QUYET_DINH,
        dttp_qd.ID AS MHP_DIA_PHUONG_QD,
        CASE
            WHEN TRIM(pmhp.MA_TOI_DANH_MIEN) IS NULL THEN CAST(NULL AS NVARCHAR2(510))
            ELSE CAST(
                (SELECT LISTAGG(TO_CHAR(dtd.ID), ',') WITHIN GROUP (ORDER BY tok.seq)
                   FROM (
                        SELECT TRIM(REGEXP_SUBSTR(pmhp.MA_TOI_DANH_MIEN, '[^;]+', 1, LEVEL)) AS td_ma,
                               LEVEL AS seq
                          FROM dual
                        CONNECT BY LEVEL <= REGEXP_COUNT(pmhp.MA_TOI_DANH_MIEN, ';') + 1
                           AND TRIM(REGEXP_SUBSTR(pmhp.MA_TOI_DANH_MIEN, '[^;]+', 1, LEVEL)) IS NOT NULL
                       ) tok
                  INNER JOIN QLPN.DM_TOI_DANHS dtd
                     ON TRIM(dtd.TD_MA) = tok.td_ma
                    AND NVL(dtd.IS_DELETED, 0) = 0
                ) AS NVARCHAR2(510)
             )
        END AS MHP_TOI_DANH_MIEN,
        CAST(pmhp.NGAY_TRA_TU_DO AS TIMESTAMP(7)) AS MHP_NGAY_TU_DO,
        CASE WHEN mtg_matm.MTG_TITLE IS NOT NULL
             THEN NVL(TO_NUMBER(REGEXP_SUBSTR(mtg_matm.MTG_TITLE, '(\d+)\s*năm', 1, 1, 'i', 1)), 0)
        END AS MHP_MATM_YEAR,
        CASE WHEN mtg_matm.MTG_TITLE IS NOT NULL
             THEN NVL(TO_NUMBER(REGEXP_SUBSTR(mtg_matm.MTG_TITLE, '(\d+)\s*tháng', 1, 1, 'i', 1)), 0)
        END AS MHP_MATM_MONTH,
        CASE WHEN mtg_matm.MTG_TITLE IS NOT NULL
             THEN NVL(TO_NUMBER(REGEXP_SUBSTR(mtg_matm.MTG_TITLE, '(\d+)\s*ngày', 1, 1, 'i', 1)), 0)
        END AS MHP_MATM_DAY,
        CASE WHEN mtg_thm.MTG_TITLE IS NOT NULL
             THEN NVL(TO_NUMBER(REGEXP_SUBSTR(mtg_thm.MTG_TITLE, '(\d+)\s*năm', 1, 1, 'i', 1)), 0)
        END AS MHP_THM_YEAR,
        CASE WHEN mtg_thm.MTG_TITLE IS NOT NULL
             THEN NVL(TO_NUMBER(REGEXP_SUBSTR(mtg_thm.MTG_TITLE, '(\d+)\s*tháng', 1, 1, 'i', 1)), 0)
        END AS MHP_THM_MONTH,
        CASE WHEN mtg_thm.MTG_TITLE IS NOT NULL
             THEN NVL(TO_NUMBER(REGEXP_SUBSTR(mtg_thm.MTG_TITLE, '(\d+)\s*ngày', 1, 1, 'i', 1)), 0)
        END AS MHP_THM_DAY,
        SYSTIMESTAMP AS CREATION_TIME,
        0 AS IS_DELETED,
        pmhp.DANH_GIA_CAI_TAO AS MHP_NHAN_XET,
        plls.ID AS PN_LAI_LICH_ID
    FROM QLPN_OLD.PN_MIEN_HINH_PHAT pmhp
    INNER JOIN QLPN_OLD.PN_LAI_LICH pll_old
        ON pmhp.PN_ID = pll_old.PN_ID
    INNER JOIN QLPN.PN_LAI_LICHS plls
        ON pll_old.SO_HSLD = plls.LL_SO_HO_SO_LAN_DAU
    LEFT JOIN QLPN.DM_CO_QUANS dcq
        ON dcq.CQ_MA = pmhp.MA_CQ_TOA_QD_MIEN
    LEFT JOIN QLPN.MAPPING_PROVINCE mp_dp_qd
        ON TRIM(TO_CHAR(mp_dp_qd.MA_TINH_OLD)) = TRIM(TO_CHAR(pmhp.MA_DP_TOA_QD_MIEN))
    LEFT JOIN QLPN.DM_TINH_THANH_PHO dttp_qd
        ON TRIM(mp_dp_qd.MA_TINH_NEW) = TRIM(dttp_qd.TTP_MA)
       AND NVL(dttp_qd.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.DM_TRUONG_HOP_MIEN_HINH_PHATS dthm
        ON dthm.MHP_MA = pmhp.MA_TRUONG_HOP_MIEN
    LEFT JOIN QLPN.DM_MA_THOI_GIANS mtg_matm
        ON TRIM(mtg_matm.MTG_MA) = TRIM(pmhp.MA_MUC_AN_TRUOC_MIEN)
       AND NVL(mtg_matm.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.DM_MA_THOI_GIANS mtg_thm
        ON TRIM(mtg_thm.MTG_MA) = TRIM(pmhp.MA_MUC_THOI_HAN_MIEN)
       AND NVL(mtg_thm.IS_DELETED, 0) = 0
)
SELECT
    s.MHP_SO_DE_NGHI,
    s.MHP_NGAY_DE_NGHI,
    s.MHP_TOI_DE_NGHI_MIEN,
    s.MHP_TRUONG_HOP_MIEN,
    s.MHP_SO_QUYET_DINH,
    s.MHP_NGAY_QUYET_DINH,
    s.MHP_TOA_QUYET_DINH,
    s.MHP_DIA_PHUONG_QD,
    s.MHP_TOI_DANH_MIEN,
    s.MHP_NGAY_TU_DO,
    s.MHP_MATM_YEAR,
    s.MHP_MATM_MONTH,
    s.MHP_MATM_DAY,
    s.MHP_THM_YEAR,
    s.MHP_THM_MONTH,
    s.MHP_THM_DAY,
    s.CREATION_TIME,
    s.IS_DELETED,
    s.MHP_NHAN_XET,
    s.PN_LAI_LICH_ID
FROM mien_hp_src s;
COMMIT;