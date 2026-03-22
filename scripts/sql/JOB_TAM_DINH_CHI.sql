-- =============================================================================
-- SCRIPT_TAM_DINH_CHIS.sql — Nạp TAM_DINH_CHIS từ QLPN_OLD.PN_TDC_TAM_DINH_CHI
-- Quy ước migrate: .cursor/rules/sql-migration-qlpn-conventions.mdc
--   - Không INSERT: ID, CREATOR_USER_ID, LAST_MODIFICATION_TIME,
--     LAST_MODIFIER_USER_ID, DELETER_USER_ID, DELETION_TIME
--   - MA_DP* → MAPPING_PROVINCE + DM_TINH_THANH_PHO (ID tỉnh), không DM_DON_VI_HANH_CHINH
-- =============================================================================
TRUNCATE TABLE QLPN.TAM_DINH_CHIS;
INSERT /*+ APPEND */ INTO QLPN.TAM_DINH_CHIS (
    SO_QUYET_DINH,
    NGAY_QUYET_DINH,
    TOA_QUYET_DINH_ID,
    DIA_PHUONG_ID,
    TU_NGAY,
    NGAY_TDC,
    NGAY_LAI,
    LOAI_QUYET_DINH_ID,
    NGAY_DE_NGHI,
    LY_DO_SUC_KHOE_ID,
    LY_DO_CHI_TIET,
    THOI_HAN_ID,
    TC8_DUYET_ID,
    NOI_VE_DIA_PHUONG_ID,
    NHAN_XET,
    PHAM_NHAN_ID,
    CREATION_TIME,
    IS_DELETED,
    TINH_TRANG_ID,
    NOI_VE_XA
)
WITH tam_dinh_chi_src AS (
    SELECT
        tdc.SO_QD AS SO_QUYET_DINH,
        CAST(tdc.NGAY_QD AS TIMESTAMP(7)) AS NGAY_QUYET_DINH,
        dcq_toa.ID AS TOA_QUYET_DINH_ID,
        dttp_toa.ID AS DIA_PHUONG_ID,
        CAST(tdc.TU_NGAY AS TIMESTAMP(7)) AS TU_NGAY,
        CAST(tdc.NGAY_TDC AS TIMESTAMP(7)) AS NGAY_TDC,
        CAST(tdc.NGAY_LAI AS TIMESTAMP(7)) AS NGAY_LAI,
        lqdt.ID AS LOAI_QUYET_DINH_ID,
        CAST(tdc.NGAY_DE_NGHI AS TIMESTAMP(7)) AS NGAY_DE_NGHI,
        lydo_sk.ID AS LY_DO_SUC_KHOE_ID,
        tdc.LY_DO_TDC AS LY_DO_CHI_TIET,
        mtg_th.ID AS THOI_HAN_ID,
        mtg_tc8.ID AS TC8_DUYET_ID,
        dttp_ve.ID AS NOI_VE_DIA_PHUONG_ID,
        tdc.NHAN_XET AS NHAN_XET,
        pll2.ID AS PHAM_NHAN_ID,
        SYSTIMESTAMP AS CREATION_TIME,
        0 AS IS_DELETED,
        tt_tdc.ID AS TINH_TRANG_ID,
        tdc.NOI_VE_XA AS NOI_VE_XA
    FROM QLPN_OLD.PN_TDC_TAM_DINH_CHI tdc
    INNER JOIN QLPN_OLD.PN_LAI_LICH pll_old
        ON pll_old.PN_ID = tdc.PN_ID
    INNER JOIN QLPN.PN_LAI_LICHS pll2
        ON pll2.LL_SO_HO_SO_LAN_DAU = pll_old.SO_HSLD
    LEFT JOIN QLPN.DM_CO_QUANS dcq_toa
        ON dcq_toa.CQ_MA = tdc.MA_CQ_CUA_TOA
    LEFT JOIN QLPN.MAPPING_PROVINCE mp_dp_toa
        ON TRIM(TO_CHAR(mp_dp_toa.MA_TINH_OLD)) = TRIM(TO_CHAR(tdc.MA_DP_CUA_TOA))
    LEFT JOIN QLPN.DM_TINH_THANH_PHO dttp_toa
        ON TRIM(mp_dp_toa.MA_TINH_NEW) = TRIM(dttp_toa.TTP_MA)
       AND NVL(dttp_toa.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.DM_LOAI_QUYET_DINH_TAM_DINH_CHIES lqdt
        ON lqdt.LQDTDC_MA = tdc.MA_LENH_TDC
    LEFT JOIN QLPN.DM_LY_DO_TAM_DINH_CHIES lydo_sk
        ON lydo_sk.LDTDC_MA = tdc.MA_LY_DO_TDC
    LEFT JOIN QLPN.DM_MA_THOI_GIANS mtg_th
        ON mtg_th.MTG_MA = tdc.MA_MUC_THOI_HAN_TDC
       AND NVL(mtg_th.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.DM_MA_THOI_GIANS mtg_tc8
        ON mtg_tc8.MTG_MA = tdc.MA_THOI_HAN_TC8
       AND NVL(mtg_tc8.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.MAPPING_PROVINCE mp_dp_ve
        ON TRIM(TO_CHAR(mp_dp_ve.MA_TINH_OLD)) = TRIM(TO_CHAR(tdc.MA_DP_NOI_VE))
    LEFT JOIN QLPN.DM_TINH_THANH_PHO dttp_ve
        ON TRIM(mp_dp_ve.MA_TINH_NEW) = TRIM(dttp_ve.TTP_MA)
       AND NVL(dttp_ve.IS_DELETED, 0) = 0
    LEFT JOIN QLPN.DM_TINH_TRANG_TAM_DINH_CHIES tt_tdc
        ON tt_tdc.TTTDC_MA = tdc.MA_TINH_TRANG_TDC
)
SELECT
    s.SO_QUYET_DINH,
    s.NGAY_QUYET_DINH,
    s.TOA_QUYET_DINH_ID,
    s.DIA_PHUONG_ID,
    s.TU_NGAY,
    s.NGAY_TDC,
    s.NGAY_LAI,
    s.LOAI_QUYET_DINH_ID,
    s.NGAY_DE_NGHI,
    s.LY_DO_SUC_KHOE_ID,
    s.LY_DO_CHI_TIET,
    s.THOI_HAN_ID,
    s.TC8_DUYET_ID,
    s.NOI_VE_DIA_PHUONG_ID,
    s.NHAN_XET,
    s.PHAM_NHAN_ID,
    s.CREATION_TIME,
    s.IS_DELETED,
    s.TINH_TRANG_ID,
    s.NOI_VE_XA
FROM tam_dinh_chi_src s;
COMMIT;