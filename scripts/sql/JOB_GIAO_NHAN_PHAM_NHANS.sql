-- New script in ORCL.

-- Date: Mar 18, 2026

-- Time: 11:32:04 AM





INSERT INTO QLPN.PN_GIAO_NHAN_PHAN_NHANS (

    GNPN_NGAY_GIAO_NHAN,

    GNPN_CAN_CU_QUYET_DINH,



    /* BÊN GIAO */

    GNPN_HO_TEN_BEN_GIAO,

    GNPN_CHUC_VU_BEN_GIAO,

    GNPN_SO_CHUNG_MINH_BEN_GIAO,

    GNPN_NGAY_CAP_BEN_GIAO,

    GNPN_TEN_DON_VI_CAP_BEN_GIAO,

    GNPN_MA_CO_QUAN_BEN_GIAO,

    GNPN_TEN_CO_QUAN_BEN_GIAO,



    /* BÊN NHẬN */

    GNPN_HO_TEN_BEN_NHAN,

    GNPN_CHUC_VU_BEN_NHAN,

    GNPN_SO_CHUNG_MINH_BEN_NHAN,

    GNPN_NGAY_CAP_BEN_NHAN,

    GNPN_TEN_DON_VI_CAP_BEN_NHAN,

    GNPN_MA_CO_QUAN_BEN_NHAN,

    GNPN_TEN_CO_QUAN_BEN_NHAN,



    /* COUNT */

    GNPN_SO_LUONG_PHAM_NHAN,



    /* FK (để bạn tự map thêm) */

    GNPN_CO_QUAN_BEN_GIAO_ID,

    GNPN_CO_QUAN_BEN_NHAN_ID,

    GNPN_DIA_PHUONG_BEN_GIAO_ID,

    GNPN_DIA_PHUONG_BEN_NHAN_ID,



    /* audit */

    CREATION_TIME,

    CREATOR_USER_ID,

    LAST_MODIFICATION_TIME,

    LAST_MODIFIER_USER_ID

)

SELECT

    /* ===== HEADER ===== */

    CAST(bb.BB_NGAY AS TIMESTAMP),

    bb.CAN_CU,



    /* ===== BÊN GIAO ===== */

    bb.HO_TEN_BG,

    bb.CHUC_VU_BG,

    bb.SO_CMND_BG,

    CAST(bb.NGAY_CAP_CMND_BG AS TIMESTAMP),

    bb.NOI_CAP_CMND_BG,

    bb.MA_CQ_BG, -- Cần map ->

    cq_bg.CQ_TEN, -- tên cơ quan (map sau)



    /* ===== BÊN NHẬN ===== */

    bb.HO_TEN_BN,

    bb.CHUC_VU_BN,

    bb.SO_CMND_BN,

    CAST(bb.NGAY_CAP_CMND_BN AS TIMESTAMP),

    bb.NOI_CAP_CMND_BN,

    bb.MA_CQ_BN,

    cq_bn.CQ_TEN, -- tên cơ quan



    /* ===== COUNT PHẠM NHÂN ===== */

    COUNT(gn.ID_BB_GN),



    /* ===== FK (LEFT JOIN để bạn sửa) ===== */

    cq_bg.ID,

    cq_bn.ID,

    dp_bg.ID,

    dp_bn.ID,



    /* ===== AUDIT ===== */

    SYSTIMESTAMP,

    NULL,

    NULL,

    NULL



FROM QLPN_OLD.PN_BIEN_BAN bb



/* ===== JOIN sang bảng giao nhận (detail) ===== */

LEFT JOIN QLPN_OLD.PN_GIAO_NHAN gn

    ON gn.ID_BB_GN = bb.ID_BB_GN



/* ===== MAP CƠ QUAN ===== */

LEFT JOIN QLPN.DM_CO_QUANS cq_bg

    ON cq_bg.CQ_MA = bb.MA_CQ_BG



LEFT JOIN QLPN.DM_CO_QUANS cq_bn

    ON cq_bn.CQ_MA = bb.MA_CQ_BN



/* ===== MAP ĐỊA PHƯƠNG ===== */

LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH dp_bg

    ON dp_bg.DVHC_MA_DVHC = bb.MA_DP_BG



LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH dp_bn

    ON dp_bn.DVHC_MA_DVHC = bb.MA_DP_BN



GROUP BY

    bb.ID_BB_GN,

    bb.BB_NGAY,

    bb.CAN_CU,



    bb.HO_TEN_BG,

    bb.CHUC_VU_BG,

    bb.SO_CMND_BG,

    bb.NGAY_CAP_CMND_BG,

    bb.NOI_CAP_CMND_BG,

    bb.MA_CQ_BG,

    cq_bg.CQ_TEN,



    bb.HO_TEN_BN,

    bb.CHUC_VU_BN,

    bb.SO_CMND_BN,

    bb.NGAY_CAP_CMND_BN,

    bb.NOI_CAP_CMND_BN,

    bb.MA_CQ_BN,

	cq_bn.CQ_TEN,

	

    cq_bg.ID,

    cq_bn.ID,

    dp_bg.ID,

    dp_bn.ID;