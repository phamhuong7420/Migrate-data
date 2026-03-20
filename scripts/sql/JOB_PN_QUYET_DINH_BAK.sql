-- New script in ORCL.

-- Date: Mar 20, 2026

-- Time: 8:29:28 AM

INSERT INTO QLPN.PN_QUYET_DINH_BAN_AN_KHACS (

    QDBAK_SO_QUYET_DINH,

    QDBAK_NGAY_QUYET_DINH,

    QDBAK_TOA_QUYET_DINH_ID,

    QDBAK_TOA_QUYET_DINH_DP_ID,

    QDBAK_SO_BAN_AN,

    QDBAK_NGAY_BAN_AN,

    QDBAK_TOA_XU_ID,

    QDBAK_TOA_XU_DP_ID,

    QDBAK_GHI_CHU,

    QDBAK_PN_LAI_LICH_ID,

    CREATION_TIME,

    IS_DELETED

)

SELECT

    o.SO_QDTH_AN,

    o.NGAY_QDTH_AN,

    

    /*map danh mục */

    dcqt.ID, -- MA_CQ_TOA_QDTH_AN → DM_TOA_AN.ID

    ddvhct.ID, -- MA_DP_TOA_QDTH_AN → DM_DIA_PHUONG.ID



    o.SO_BAN_AN,

    o.NGAY_BAN_AN,



    dcqx.ID, -- MA_CQ_TOA_XU → DM_TOA_AN.ID

    ddvhcx.ID, -- MA_DP_TOA_XU → DM_DIA_PHUONG.ID



    o.GHI_CHU_QDTHA,

    pll2.ID,



    SYSTIMESTAMP,

    0

FROM QLPN_OLD.PN_QD_THI_HANH_AN o



/* map PN */

LEFT JOIN QLPN_OLD.PN_LAI_LICH pll

    ON pll.PN_ID = o.PN_ID



INNER JOIN QLPN.PN_LAI_LICHS pll2

    ON pll2.LL_SO_HO_SO_LAN_DAU = pll.SO_HSLD

    

LEFT JOIN QLPN.DM_CO_QUANS dcqx

	ON dcqx.CQ_MA = o.MA_CQ_TOA_XU

LEFT JOIN QLPN.DM_CO_QUANS dcqt

	ON dcqt.CQ_MA = o.MA_CQ_TOA_QDTH_AN

LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH ddvhcx

	ON ddvhcx.DVHC_MA_DVHC =  o.MA_DP_TOA_XU

LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH ddvhct

	ON ddvhct.DVHC_MA_DVHC = o.MA_DP_TOA_QDTH_AN;



