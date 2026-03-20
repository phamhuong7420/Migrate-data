-- New script in ORCL.

-- Date: Mar 19, 2026

-- Time: 9:41:31 AM

DELETE FROM QLPN.PN_BIEN_BANS;

DELETE FROM QLPN.PN_BIEN_BAN_PHAM_NHANS;



TRUNCATE TABLE QLPN.TMP_PN_BB_MAP;



INSERT INTO QLPN.PN_BIEN_BANS (

    BB_NGAY,

    HO_TEN_BG,

    CHUC_VU_BG,

    MA_CQ_BG,

    MA_DP_BG,

    SO_CMND_BG,

    NGAY_CAP_CMND_BG,

    NOI_CAP_CMND_BG,

    HO_TEN_BN,

    CHUC_VU_BN,

    MA_CQ_BN,

    MA_DP_BN,

    SO_CMND_BN,

    NGAY_CAP_CMND_BN,

    NOI_CAP_CMND_BN,

    CAN_CU,

    CREATION_TIME,

    IS_DELETED

)

SELECT

    bb.BB_NGAY,

    bb.HO_TEN_BG,

    bb.CHUC_VU_BG,

    dcqg.ID,

    ddvhcg.ID,

    bb.SO_CMND_BG,

    bb.NGAY_CAP_CMND_BG,

    bb.NOI_CAP_CMND_BG,

    bb.HO_TEN_BN,

    bb.CHUC_VU_BN,

    dcqn.ID,

    ddvhcn.ID,

    bb.SO_CMND_BN,

    bb.NGAY_CAP_CMND_BN,

    bb.NOI_CAP_CMND_BN,

    bb.CAN_CU,

    SYSTIMESTAMP,

    0

FROM QLPN_OLD.PN_BIEN_BAN bb

LEFT JOIN QLPN.DM_CO_QUANS dcqg 

ON dcqg.CQ_MA = bb.MA_CQ_BG 

LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH ddvhcg

ON ddvhcg.DVHC_MA_DVHC = bb.MA_DP_BG

LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH ddvhcn

ON ddvhcn.DVHC_MA_DVHC = bb.MA_DP_BN

LEFT JOIN QLPN.DM_CO_QUANS dcqn

ON dcqn.CQ_MA = bb.MA_CQ_BN ; 



INSERT INTO QLPN.TMP_PN_BB_MAP (OLD_ID, NEW_ID)

SELECT 

    bb.ID_BB_GN,

    nb.ID

FROM QLPN_OLD.PN_BIEN_BAN bb

JOIN QLPN.PN_BIEN_BANS nb

    ON nb.BB_NGAY = bb.BB_NGAY

   AND nb.HO_TEN_BG = bb.HO_TEN_BG

   AND ROWNUM = 1;



INSERT INTO QLPN.PN_BIEN_BAN_PHAM_NHANS (

    PN_BIEN_BAN_ID,

    PN_LAI_LICH_ID,

    SO_HSLD,

    SO_CCCD,

    HO_TEN,

    NGAY_SINH,

    GIOI_TINH_ID,

    DC_TT_TINH_ID,

    DC_TT_PHUONG_XA_ID,

    DC_TT_THON_PHO,

    TOI_DANH,

    NGAY_BAT,

    AN_PHAT,

    CREATION_TIME,

    IS_DELETED

)

SELECT

    map.NEW_ID,

    pll2.ID,

    gn.SO_HSLD,

    pll2.LL_CCCD, -- SO_CCCD (OLD không có)

    gn.HO_TEN,

    pll2.LL_NGAY_SINH, 

    dmgt.ID, -- GIOI_TINH_ID (map từ MA_GIOI_TINH)

    pll2.LL_DIA_CHI_THUONG_TRU_PROVICE, -- DC_TT_TINH_ID (map danh mục)

    pll2.LL_DIA_CHI_THUONG_TRU_WARD, -- DC_TT_PHUONG_XA_ID (map danh mục)

    pll2.LL_DIA_CHI_THUONG_TRU,

    td.ID, -- join DM_TOI_DANH

    gn.NGAY_BAT,

    dmtg.ID, -- join DM_MUC_AN

    SYSTIMESTAMP,

    0

FROM QLPN_OLD.PN_GIAO_NHAN gn

JOIN QLPN.TMP_PN_BB_MAP map

    ON map.OLD_ID = gn.ID_BB_GN

INNER JOIN QLPN.PN_LAI_LICHS pll2

    ON pll2.LL_SO_HO_SO_LAN_DAU = gn.SO_HSLD

LEFT JOIN QLPN.DM_GIOI_TINHS dmgt

	ON gn.MA_GIOI_TINH = dmgt.GT_MA

LEFT JOIN QLPN.DM_TOI_DANHS td

	ON td.TD_MA = gn.MA_TOI_DANH

LEFT JOIN QLPN.DM_MA_THOI_GIANS dmtg 

	ON dmtg.MTG_MA = gn.MA_MUC_AN_PHAT;



--DROP TABLE QLPN.TMP_PN_BB_MAP;