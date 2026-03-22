-- =============================================================================
-- Nạp QLPN.GIAM_THOI_HANS từ QLPN_OLD.PN_GIAM_THOI_HAN (+ bổ sung PN_KY_GIAM_THOI_HAN theo PN_ID)
-- Quy ước migrate: .cursor/rules/sql-migration-qlpn-conventions.mdc
--   - Không INSERT: ID, CREATOR_USER_ID, LAST_MODIFICATION_TIME,
--     LAST_MODIFIER_USER_ID, DELETER_USER_ID, DELETION_TIME
--   - MA_DP* → MAPPING_PROVINCE + DM_TINH_THANH_PHO (ID tỉnh), không DM_DON_VI_HANH_CHINH
-- PN_KY_GIAM_THOI_HAN: bổ sung TG_CHAP_HANH / TG_DA_GIAM / TG_CON_LAI khi không join được DM_MA_THOI_GIANS (MTG_TITLE NULL)
-- =============================================================================
DELETE FROM QLPN.GIAM_THOI_HANS;
INSERT /*+ APPEND */ INTO QLPN.GIAM_THOI_HANS (
    NGAY_XET,
    DA_CHAP_HANH,
    DA_GIAM,
    CON_LAI,
    MUC_DE_NGHI_ID,
    CAP_TREN_DUYET_ID,
    SO_QUYET_DINH,
    NGAY_QUYET_DINH,
    MUC_GIAM_ID,
    TOA_QUYET_DINH_ID,
    DIA_PHUONG_ID,
    NGAY_THA_GIAM_HET_AN,
    PHAM_NHAN_ID,
    CREATION_TIME,
    IS_DELETED,
    NHAN_XET,
    KQCT_PHIEU,
    KQCT_DANHSACH
)
WITH ky_one AS (
    SELECT
        k.*,
        ROW_NUMBER() OVER (PARTITION BY k.PN_ID ORDER BY k.ROWID) AS ky_rn
    FROM QLPN_OLD.PN_KY_GIAM_THOI_HAN k
),
giam_th_src AS (
    SELECT
        CAST(pgtg.NGAY_XET AS TIMESTAMP(7)) AS NGAY_XET,
        NVL(mtgch.MTG_TITLE, TO_NCHAR(ky.TG_CHAP_HANH)) AS DA_CHAP_HANH,
        NVL(mtgg.MTG_TITLE, TO_NCHAR(ky.TG_DA_GIAM)) AS DA_GIAM,
        NVL(mtgcl.MTG_TITLE, TO_NCHAR(ky.TG_CON_LAI)) AS CON_LAI,
        mtgmdn.ID AS MUC_DE_NGHI_ID,
        mtgbca.ID AS CAP_TREN_DUYET_ID,
        pgtg.SO_QD_GIAM AS SO_QUYET_DINH,
        CAST(pgtg.NGAY_QD_GIAM AS TIMESTAMP(7)) AS NGAY_QUYET_DINH,
        mtgqdg.ID AS MUC_GIAM_ID,
        dcq.ID AS TOA_QUYET_DINH_ID,
        dttp.ID AS DIA_PHUONG_ID,
        CAST(pgtg.NGAY_THA_GIAM_HET_AN AS TIMESTAMP(7)) AS NGAY_THA_GIAM_HET_AN,
        plls.ID AS PHAM_NHAN_ID,
        SYSTIMESTAMP AS CREATION_TIME,
        0 AS IS_DELETED,
        TO_NCLOB(
            NVL(pgtg.NHAN_XET_TG, '')
            || CASE WHEN pgtg.GHI_CHU_GTH IS NOT NULL THEN CHR(10) || pgtg.GHI_CHU_GTH END
        ) AS NHAN_XET,
        TO_NCLOB(pgtg.KQCT_PHIEU_GIAM) AS KQCT_PHIEU,
        TO_NCLOB(pgtg.KQCT_DS_GIAM) AS KQCT_DANHSACH
    FROM QLPN_OLD.PN_GIAM_THOI_HAN pgtg
    INNER JOIN QLPN_OLD.PN_LAI_LICH pll_old
        ON pgtg.PN_ID = pll_old.PN_ID
    INNER JOIN QLPN.PN_LAI_LICHS plls
        ON pll_old.SO_HSLD = plls.LL_SO_HO_SO_LAN_DAU
    LEFT JOIN ky_one ky
        ON ky.PN_ID = pgtg.PN_ID
       AND ky.ky_rn = 1
    LEFT JOIN QLPN.DM_MA_THOI_GIANS mtgch
        ON mtgch.MTG_MA = pgtg.MA_MUC_TG_CHAP_HANH
       AND NVL(mtgch.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.DM_MA_THOI_GIANS mtgg
        ON mtgg.MTG_MA = pgtg.MA_MUC_QD_GIAM_TOA_AN
       AND NVL(mtgg.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.DM_MA_THOI_GIANS mtgcl
        ON mtgcl.MTG_MA = pgtg.MA_MUC_TG_CON_LAI
       AND NVL(mtgcl.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.DM_MA_THOI_GIANS mtgmdn
        ON mtgmdn.MTG_MA = pgtg.MA_MUC_DN_GIAM_TG
       AND NVL(mtgmdn.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.DM_MA_THOI_GIANS mtgbca
        ON mtgbca.MTG_MA = pgtg.MA_MUC_DN_GIAM_V26
       AND NVL(mtgbca.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.DM_MA_THOI_GIANS mtgqdg
        ON mtgqdg.MTG_MA = pgtg.MA_MUC_QD_GIAM_TOA_AN
       AND NVL(mtgqdg.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.DM_CO_QUANS dcq
        ON dcq.CQ_MA = pgtg.MA_CQ_TOA_QD_GIAM
       AND NVL(dcq.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.MAPPING_PROVINCE mp_dp
        ON TRIM(TO_CHAR(mp_dp.MA_TINH_OLD)) = TRIM(TO_CHAR(pgtg.MA_DP_TOA_QD_GIAM))
    LEFT JOIN QLPN.DM_TINH_THANH_PHO dttp
        ON TRIM(mp_dp.MA_TINH_NEW) = TRIM(dttp.TTP_MA)
       AND NVL(dttp.IS_DELETED, 0) = 0
)
SELECT
    s.NGAY_XET,
    s.DA_CHAP_HANH,
    s.DA_GIAM,
    s.CON_LAI,
    s.MUC_DE_NGHI_ID,
    s.CAP_TREN_DUYET_ID,
    s.SO_QUYET_DINH,
    s.NGAY_QUYET_DINH,
    s.MUC_GIAM_ID,
    s.TOA_QUYET_DINH_ID,
    s.DIA_PHUONG_ID,
    s.NGAY_THA_GIAM_HET_AN,
    s.PHAM_NHAN_ID,
    s.CREATION_TIME,
    s.IS_DELETED,
    s.NHAN_XET,
    s.KQCT_PHIEU,
    s.KQCT_DANHSACH
FROM giam_th_src s;
COMMIT;