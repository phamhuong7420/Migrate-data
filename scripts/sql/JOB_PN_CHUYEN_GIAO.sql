DELETE FROM "QLPN"."PN_CHUYEN_GIAO";

INSERT INTO "QLPN"."PN_CHUYEN_GIAO" (

    PNCG_SO_QDCG,

    PNCG_NGAY_QDCG,

    PNCG_TOA_QDCG,

    PNCG_SO_QDTHQDCG,

    PNCG_NGAY_QDTHQDCG,

    PNCG_TOA_QDTHQDCG,

    PNCG_GHI_CHU,

    PNCG_NGAY_CHUYEN_GIAO,

    PNCG_LAI_LICH_ID,

    PNCG_DIA_PHUONG_QDCGID,

    PNCG_DIA_PHUONG_THQDCGID,

    PNCG_NUOC_CHUYEN_GIAO_ID,

    CREATION_TIME,

    CREATOR_USER_ID,

    IS_DELETED

)

SELECT 

    A.SO_QD_CG,                         -- PNCG_SO_QDCG

    A.NGAY_QD_CG,                       -- PNCG_NGAY_QDCG

    NULL,                               -- PNCG_TOA_QDCG (Gán NULL)

    A.SO_QD_QDTH_CG,                    -- PNCG_SO_QDTHQDCG

    A.NGAY_QD_QDTH_CG,                  -- PNCG_NGAY_QDTHQDCG

    NULL,                               -- PNCG_TOA_QDTHQDCG (Gán NULL)

    A.GHI_CHU,                          -- PNCG_GHI_CHU

    A.NGAY_CHUYEN_GIAO,                 -- PNCG_NGAY_CHUYEN_GIAO

    C.ID,                               -- PNCG_LAI_LICH_ID (Lấy từ bảng Lai lịch mới)

    MA_DP_TOA_CG,                               -- PNCG_DIA_PHUONG_QDCGID (Gán NULL)

    DDVHC.ID ,                               -- PNCG_DIA_PHUONG_THQDCGID (Gán NULL)

    DQT.ID,                               -- PNCG_NUOC_CHUYEN_GIAO_ID (Gán NULL)

    SYSDATE,                            -- CREATION_TIME

    NULL,                               -- CREATOR_USER_ID

    0                                   -- IS_DELETED

FROM "QLPN_OLD"."PN_CHUYEN_GIAO" A

INNER JOIN "QLPN_OLD"."PN_LAI_LICH" B ON A.PN_ID = B.PN_ID

INNER JOIN "QLPN"."PN_LAI_LICHS" C ON B.SO_HSLD = C.LL_SO_HO_SO_LAN_DAU

LEFT JOIN "QLPN"."DM_DON_VI_HANH_CHINH" DDVHC ON DDVHC.DVHC_MA_DVHC = A.MA_DP_TOA_QDTH_CG

LEFT JOIN "QLPN"."DM_QUOC_TICHS" DQT ON DQT.QT_MA = A.MA_QUOC_GIA_CG;