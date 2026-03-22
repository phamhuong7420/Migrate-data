-- =============================================================================
-- SCRIPT_DAC_XA.sql — Nạp PN_DAC_XAS từ QLPN_OLD.PN_DX_DAC_XA
-- Quy ước migrate: .cursor/rules/sql-migration-qlpn-conventions.mdc
--   - Không INSERT: ID, CREATOR_USER_ID, LAST_MODIFICATION_TIME,
--     LAST_MODIFIER_USER_ID, DELETER_USER_ID, DELETION_TIME
-- Cột old chưa có chỗ trên PN_DAC_XAS: GHI_CHU, các MA/LY_DO/Y_KIEN_* khác, SO_PHIEU, SO_NAM_CAI_TAO, ...
-- =============================================================================
TRUNCATE TABLE QLPN.PN_DAC_XAS;
INSERT /*+ APPEND */ INTO QLPN.PN_DAC_XAS (
    DX_NGAY_XET,
    DX_NGAY_THA,
    DX_NGAY_TRINH_DIEN,
    DX_THOI_GIAN_GIAM,
    DX_XEP_LOAI_CAI_TAO,
    DX_TINH_TRANG_DAC_XA,
    DX_YKIEN_DOI_PN,
    DX_NHAN_XET_TRAI,
    DX_KQCT_PHIEU,
    DX_KQCT_DS,
    PNLAI_LICH_ID,
    CREATION_TIME,
    IS_DELETED
)
WITH dac_xa_src AS (
    SELECT
        CAST(dx.NGAY_XET_DX AS TIMESTAMP(7)) AS DX_NGAY_XET,
        CAST(dx.NGAY_THA_DX AS TIMESTAMP(7)) AS DX_NGAY_THA,
        CAST(dx.NGAY_TRINH_DIEN_DX AS TIMESTAMP(7)) AS DX_NGAY_TRINH_DIEN,
        dtg.MTG_TITLE AS DX_THOI_GIAN_GIAM,
        dx.DANH_GIA_CAI_TAO AS DX_XEP_LOAI_CAI_TAO,
        '[' || CAST(dddx.ID AS NVARCHAR2(10)) || ']' AS DX_TINH_TRANG_DAC_XA,
        TO_NCLOB(dx.Y_KIEN_DOI_PN) AS DX_YKIEN_DOI_PN,
        TO_NCLOB(dx.NHAN_XET_DE_NGHI) AS DX_NHAN_XET_TRAI,
        TO_NCLOB(dx.KQCT_PHIEU_DX) AS DX_KQCT_PHIEU,
        TO_NCLOB(dx.KQCT_DS_DX) AS DX_KQCT_DS,
        plls.ID AS PNLAI_LICH_ID,
        SYSTIMESTAMP AS CREATION_TIME,
        0 AS IS_DELETED
    FROM QLPN_OLD.PN_DX_DAC_XA dx
    INNER JOIN QLPN_OLD.PN_LAI_LICH pll_old
        ON dx.PN_ID = pll_old.PN_ID
    INNER JOIN QLPN.PN_LAI_LICHS plls
        ON pll_old.SO_HSLD = plls.LL_SO_HO_SO_LAN_DAU
    LEFT JOIN QLPN.DM_MA_THOI_GIANS dtg
        ON dtg.MTG_MA = dx.MA_MUC_TG_CHAP_HANH_DX
       AND NVL(dtg.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.DM_DUOC_DAC_XAS dddx
        ON dddx.DDX_MA = dx.MA_DUOC_DAC_XA
       AND NVL(dddx.IS_DELETED, 0) = 0
)
SELECT
    s.DX_NGAY_XET,
    s.DX_NGAY_THA,
    s.DX_NGAY_TRINH_DIEN,
    s.DX_THOI_GIAN_GIAM,
    s.DX_XEP_LOAI_CAI_TAO,
    s.DX_TINH_TRANG_DAC_XA,
    s.DX_YKIEN_DOI_PN,
    s.DX_NHAN_XET_TRAI,
    s.DX_KQCT_PHIEU,
    s.DX_KQCT_DS,
    s.PNLAI_LICH_ID,
    s.CREATION_TIME,
    s.IS_DELETED
FROM dac_xa_src s;
COMMIT;