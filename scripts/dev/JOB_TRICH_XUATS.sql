-- New script in ORCL.



-- Date: Mar 17, 2026



-- Time: 5:00:33 PM



TRUNCATE TABLE QLPN.TRICH_XUATS;







INSERT INTO QLPN.TRICH_XUATS (



    TX_LOAI_LENH_ID,



    TX_SO_LENH,



    TX_NGAY_RA_LENH,



    TX_TU_NGAY,



    TX_DON_VI_DE_NGHI_ID,



    TX_SO_DE_NGHI,



    TX_NGAY_DE_NGHI,



    TX_DIA_PHUONG_VE_ID,



    TX_DIA_PHUONG_DE_NGHI_ID,



    TX_NGAY_TRICH_XUAT,



    TX_NGAY_TRA,



    TX_BAN_GIAO_ID,



    TX_DON_VI_VE_ID,



    PHAM_NHAN_ID,



    CREATION_TIME,



    CREATOR_USER_ID,



    LAST_MODIFICATION_TIME,



    LAST_MODIFIER_USER_ID,



    IS_DELETED,



    DELETER_USER_ID,



    DELETION_TIME,



    TX_LY_DO_ID,



    TX_THOI_HAN_ID,



    TX_TINH_TRANG_ID



)



SELECT


/*+ USE_HASH(pll  pll2) */
    lqdtx.id,







    tx.SO_LENH_TX,



    CAST(tx.NGAY_LENH_TX AS TIMESTAMP),







    CAST(tx.TU_NGAY_TX AS TIMESTAMP),







    /* MA_CQ_DN_TX → đơn vị đề nghị */



    DCQ.ID,







    tx.SO_CV_DN_TX,



    CAST(tx.NGAY_CV_DN_TX AS TIMESTAMP),







    /* MA_DP_TX_VE → địa phương về */



    DDVHCV.ID,







    /* MA_DP_DN_TX → địa phương đề nghị */



    DDVHC.ID,







    CAST(tx.NGAY_TX AS TIMESTAMP),



    CAST(tx.NGAY_TRA_LAI AS TIMESTAMP),







    /* MA_BAN_GIAO_TX */



    NULL,







    /* MA_CQ_TX_VE → đơn vị về (chưa chắc) */



    DCQV.ID,







    pll2.ID,







    /* Audit */



    SYSTIMESTAMP,



    NULL,



    NULL,



    NULL,



    0,



    NULL,



    NULL,







    /* MA_LY_DO_TX */



    NULL,







    /* MA_MUC_THOI_HAN_TX */



    NULL,







    /* MA_TINH_TRANG_TX */



    NULL







FROM QLPN_OLD.PN_TX_TRICH_XUAT tx







LEFT JOIN QLPN_OLD.PN_LAI_LICH pll



    ON pll.PN_ID = tx.PN_ID







INNER JOIN QLPN.PN_LAI_LICHS pll2



    ON pll2.LL_SO_HO_SO_LAN_DAU = pll.SO_HSLD



LEFT JOIN QLPN.DM_LOAI_QUYET_DINH_TRICH_XUATS lqdtx



	ON lqdtx.LQDTX_MA = tx.MA_LENH_TX



LEFT JOIN QLPN.DM_CO_QUANS DCQ 



	ON DCQ.CQ_MA = tx.MA_CQ_DN_TX 



LEFT JOIN QLPN.DM_CO_QUANS DCQV 



	ON DCQV.CQ_MA = tx.MA_CQ_TX_VE



LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH DDVHC 



	ON DDVHC.DVHC_MA_DVHC = tx.MA_DP_DN_TX



	LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH DDVHCV 



	ON DDVHCV.DVHC_MA_DVHC = tx.MA_DP_TX_VE



LEFT JOIN QLPN.DM_TRAI_GIAMS dmtrg



	ON dmtrg.TG_MA = tx.MA_TRAI_GIAM_VE;



