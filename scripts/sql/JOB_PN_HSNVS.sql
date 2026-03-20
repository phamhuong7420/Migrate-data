-- New script in ORCL.



-- Date: Mar 20, 2026



-- Time: 9:55:36 AM



INSERT INTO QLPN.PN_HSNVS (



    HO_TEN_BG,



    CAP_BAC_BG,



    CHUC_VU_BG,



    MA_CQ_BG,



    MA_DP_BG,



    HO_TEN_BN,



    CAP_BAC_BN,



    CHUC_VU_BN,



    MA_CQ_BN,



    MA_DP_BN,



    HO_TEN_DK,



    CAP_BAC_DK,



    CHUC_VU_DK,



    MA_CQ_DK,



    MA_DP_DK,



    NGAY_DK,



    CREATION_TIME,



    IS_DELETED



)



WITH SRC AS (



    SELECT DISTINCT



        h.HO_TEN_BG,



        h.CAP_BAC_BG,



        h.CHUC_VU_BG,



        dcqg.ID AS MA_CQ_BG,



        ddvhcg.ID AS MA_DP_BG,



        h.HO_TEN_BN,



        h.CAP_BAC_BN,



        h.CHUC_VU_BN,



        dcqn.ID AS MA_CQ_BN,



        ddvhcn.ID AS MA_DP_BN,



        h.HO_TEN_DK,



        h.CAP_BAC_DK,



        h.CHUC_VU_DK,



        dcqdk.ID AS MA_CQ_DK,



        ddvhcdk.ID AS MA_DP_DK,



        CAST(h.NGAY_DK AS TIMESTAMP) AS NGAY_DK



    FROM QLPN_OLD.PN_HSNV h



    LEFT JOIN QLPN.DM_CO_QUANS dcqg 



        ON dcqg.CQ_MA = h.MA_CQ_BG 



    LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH ddvhcg



        ON ddvhcg.DVHC_MA_DVHC = h.MA_DP_BG



    LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH ddvhcn



        ON ddvhcn.DVHC_MA_DVHC = h.MA_DP_BN



    LEFT JOIN QLPN.DM_CO_QUANS dcqn



        ON dcqn.CQ_MA = h.MA_CQ_BN 



    LEFT JOIN QLPN.DM_CO_QUANS dcqdk



        ON dcqdk.CQ_MA = h.MA_CQ_DK



    LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH ddvhcdk



        ON ddvhcdk.DVHC_MA_DVHC = h.MA_DP_DK



)



SELECT



    s.HO_TEN_BG,



    s.CAP_BAC_BG,



    s.CHUC_VU_BG,



    s.MA_CQ_BG,



    s.MA_DP_BG,



    s.HO_TEN_BN,



    s.CAP_BAC_BN,



    s.CHUC_VU_BN,



    s.MA_CQ_BN,



    s.MA_DP_BN,



    s.HO_TEN_DK,



    s.CAP_BAC_DK,



    s.CHUC_VU_DK,



    s.MA_CQ_DK,



    s.MA_DP_DK,



    s.NGAY_DK,



    SYSTIMESTAMP,



    0



FROM SRC s;











INSERT INTO QLPN.PN_HSNV_PHAM_NHANS (



    PN_HSNV_ID,



    PN_LAI_LICH_ID,



    CREATION_TIME,



    IS_DELETED



)



WITH SRC AS (



    SELECT



        h.*,



        dcqg.ID  AS MA_CQ_BG_NEW,



        ddvhcg.ID AS MA_DP_BG_NEW,



        dcqn.ID  AS MA_CQ_BN_NEW,



        ddvhcn.ID AS MA_DP_BN_NEW,



        dcqdk.ID AS MA_CQ_DK_NEW,



        ddvhcdk.ID AS MA_DP_DK_NEW



    FROM QLPN_OLD.PN_HSNV h



    LEFT JOIN QLPN.DM_CO_QUANS dcqg 



        ON dcqg.CQ_MA = h.MA_CQ_BG 



    LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH ddvhcg



        ON ddvhcg.DVHC_MA_DVHC = h.MA_DP_BG



    LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH ddvhcn



        ON ddvhcn.DVHC_MA_DVHC = h.MA_DP_BN



    LEFT JOIN QLPN.DM_CO_QUANS dcqn



        ON dcqn.CQ_MA = h.MA_CQ_BN 



    LEFT JOIN QLPN.DM_CO_QUANS dcqdk



        ON dcqdk.CQ_MA = h.MA_CQ_DK



    LEFT JOIN QLPN.DM_DON_VI_HANH_CHINH ddvhcdk



        ON ddvhcdk.DVHC_MA_DVHC = h.MA_DP_DK



)



SELECT



    hsnv_new.ID,



    pll2.ID,



    SYSTIMESTAMP,



    0



FROM SRC h







JOIN QLPN.PN_HSNVS hsnv_new



    ON hsnv_new.HO_TEN_BG = h.HO_TEN_BG



   AND NVL(hsnv_new.CAP_BAC_BG, '##') = NVL(h.CAP_BAC_BG, '##')



   AND NVL(hsnv_new.CHUC_VU_BG, '##') = NVL(h.CHUC_VU_BG, '##')



   AND NVL(hsnv_new.MA_CQ_BG, -1) = NVL(h.MA_CQ_BG_NEW, -1)



   AND NVL(hsnv_new.MA_DP_BG, -1) = NVL(h.MA_DP_BG_NEW, -1)







   AND NVL(hsnv_new.HO_TEN_BN, '##') = NVL(h.HO_TEN_BN, '##')



   AND NVL(hsnv_new.CAP_BAC_BN, '##') = NVL(h.CAP_BAC_BN, '##')



   AND NVL(hsnv_new.CHUC_VU_BN, '##') = NVL(h.CHUC_VU_BN, '##')



   AND NVL(hsnv_new.MA_CQ_BN, -1) = NVL(h.MA_CQ_BN_NEW, -1)



   AND NVL(hsnv_new.MA_DP_BN, -1) = NVL(h.MA_DP_BN_NEW, -1)







   AND NVL(hsnv_new.HO_TEN_DK, '##') = NVL(h.HO_TEN_DK, '##')



   AND NVL(hsnv_new.CAP_BAC_DK, '##') = NVL(h.CAP_BAC_DK, '##')



   AND NVL(hsnv_new.CHUC_VU_DK, '##') = NVL(h.CHUC_VU_DK, '##')



   AND NVL(hsnv_new.MA_CQ_DK, -1) = NVL(h.MA_CQ_DK_NEW, -1)



   AND NVL(hsnv_new.MA_DP_DK, -1) = NVL(h.MA_DP_DK_NEW, -1)







   AND hsnv_new.NGAY_DK = CAST(h.NGAY_DK AS TIMESTAMP)







LEFT JOIN QLPN_OLD.PN_LAI_LICH pll



    ON pll.PN_ID = h.PN_ID







INNER JOIN QLPN.PN_LAI_LICHS pll2



    ON pll2.LL_SO_HO_SO_LAN_DAU = pll.SO_HSLD;