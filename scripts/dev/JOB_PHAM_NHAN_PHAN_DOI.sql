DELETE FROM "QLPN"."PHAM_NHAN_PHAN_DOI";

INSERT INTO "QLPN"."PHAM_NHAN_PHAN_DOI" (

    PNPD_NGAY_CHUYEN,

    PNPD_PHAN_DOI_ID,

    PNPD_GHI_CHU,

    PNPD_LAI_LICH_ID,

    CREATION_TIME,

    CREATOR_USER_ID,

    IS_DELETED

)



SELECT 

    A.NGAY_CHUYEN_DEN,               -- PNPD_NGAY_CHUYEN

    D.ID,    -- PNPD_PHAN_DOI_ID (Ép kiểu nếu mã cũ là số)

    A.GHI_CHU,                       -- PNPD_GHI_CHU

    C.ID,                            -- PNPD_LAI_LICH_ID (ID từ bảng mới)

    SYSDATE,                         -- CREATION_TIME

    NULL,                            -- CREATOR_USER_ID

    0                                -- IS_DELETED

FROM "QLPN_OLD"."PN_PHAN_DOI" A

INNER JOIN "QLPN_OLD"."PN_LAI_LICH" B ON A.PN_ID = B.PN_ID

INNER JOIN "QLPN"."PN_LAI_LICHS" C ON B.SO_HSLD = C.LL_SO_HO_SO_LAN_DAU

LEFT JOIN "QLPN"."DM_PHAN_DOIS" D ON A.MA_PHAN_DOI_DEN = D.PD_MA;