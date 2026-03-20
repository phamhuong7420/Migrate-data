DELETE FROM "QLPN"."PN_BAN_AN_KHACS";



INSERT INTO "QLPN"."PN_BAN_AN_KHACS" (







    BAK_SO_BAN_AN,







    BAK_NGAY_BAN_AN,







    BAK_SO_QUYET_DINH,







    BAK_NGAY_QUYET_DINH,







    BAK_GHI_CHU,







    BAK_TOA_XU_ID,







    BAK_TOA_XU_DP_ID,







    BAK_TOA_QUYET_DINH_ID,







    BAK_CAP_TOA_XU_ID,







    BAK_TOI_DANH_ID,







    BAK_MUC_AN_ID,







    BAK_PN_LAI_LICH_ID,







    CREATION_TIME,







    CREATOR_USER_ID,







    LAST_MODIFICATION_TIME,







    LAST_MODIFIER_USER_ID







)







SELECT 

 /*+ USE_HASH(B C) */





    A.SO_BAN_AN,                        -- BAK_SO_BAN_AN







    A.NGAY_BAN_AN,                      -- BAK_NGAY_BAN_AN







    D.SO_QD_TH_AN,                               -- BAK_SO_QUYET_DINH (Gán NULL)







    D.NGAY_QD_TH_AN,                               -- BAK_NGAY_QUYET_DINH (Gán NULL)







    A.GHI_CHU_BA,                       -- BAK_GHI_CHU







    DCQ.ID,                               -- BAK_TOA_XU_ID (Cần map danh mục Tòa án)







    DDVHC.ID,                               -- BAK_TOA_XU_DP_ID (Cần map danh mục Địa phương)







    NULL,                               -- BAK_TOA_QUYET_DINH_ID 







    DCTX.ID,                               -- BAK_CAP_TOA_XU_ID







    DTD.ID,                               -- BAK_TOI_DANH_ID (Cần map danh mục Tội danh)







    DMA.ID,                               -- BAK_MUC_AN_ID (Cần map danh mục Mức án)







    C.ID,                               -- BAK_PN_LAI_LICH_ID (Lấy từ bảng Lai lịch mới)







    SYSDATE,                            -- CREATION_TIME







    NULL,                               -- CREATOR_USER_ID







    NULL,                               -- LAST_MODIFICATION_TIME







    NULL                                -- LAST_MODIFIER_USER_ID







FROM (SELECT 



    A.PN_ID,SO_BAN_AN,NGAY_BAN_AN,MA_CAP_TOA_XU_BA,MA_CQ_TOA_XU_BA,MA_DP_TOA_XU_BA,MA_MUC_AN_BA,



    REGEXP_SUBSTR(A.MA_TOI_DANH_BA, '[^;]+', 1, LEVEL) AS MA_TOI_DANH_BA,GHI_CHU_BA



FROM QLPN_OLD.PN_BAN_AN A  



CONNECT BY 



    REGEXP_SUBSTR(A.MA_TOI_DANH_BA, '[^;]+', 1, LEVEL) IS NOT NULL



    AND PRIOR A.PN_ID = A.PN_ID



    AND PRIOR SYS_GUID() IS NOT NULL) A



INNER JOIN "QLPN_OLD"."PN_LAI_LICH" B ON A.PN_ID = B.PN_ID



INNER JOIN "QLPN"."PN_LAI_LICHS" C ON B.SO_HSLD = C.LL_SO_HO_SO_LAN_DAU



LEFT JOIN "QLPN_OLD"."PN_TONG_HOP_GIAM" D ON A.PN_ID = D.PN_ID



LEFT JOIN "QLPN"."DM_CO_QUANS" DCQ ON A.MA_CQ_TOA_XU_BA = DCQ.CQ_MA



LEFT JOIN "QLPN"."DM_DON_VI_HANH_CHINH" DDVHC ON A.MA_DP_TOA_XU_BA = DDVHC.DVHC_MA_DVHC



LEFT JOIN "QLPN"."DM_CAP_TOA_XUS" DCTX ON A.MA_CAP_TOA_XU_BA = DCTX.CTX_MA



LEFT JOIN "QLPN"."DM_TOI_DANHS" DTD ON A.MA_TOI_DANH_BA = DTD.TD_MA



LEFT JOIN "QLPN"."DM_MA_THOI_GIANS" DMA ON A.MA_MUC_AN_BA = DMA.MTG_MA;