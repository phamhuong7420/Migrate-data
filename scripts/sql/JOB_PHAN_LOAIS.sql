

DELETE FROM "QLPN"."PHAN_LOAIS";

INSERT INTO "QLPN"."PHAN_LOAIS" (

    NGAY_PHAN_LOAI,

    LOAI_PHAN_LOAI_ID,

    TU_LOAI_ID,

    DEN_LOAI_ID,

    NGAY_BIEN_BAN,

    GHI_CHU,

    PHAM_NHAN_ID,

    CREATION_TIME,

    CREATOR_USER_ID,

    LAST_MODIFICATION_TIME,

    LAST_MODIFIER_USER_ID,

    IS_DELETED,

    DELETER_USER_ID,

    DELETION_TIME

)

SELECT 
/*+ USE_HASH(B C) */
    A.NGAY_PHAN_LOAI,                   -- NGAY_PHAN_LOAI

    MA_LOAI_PHAN_LOAI,                               -- LOAI_PHAN_LOAI_ID (Gán NULL vì mã cũ là VARCHAR2)

    MA_TU_LOAI,                               -- TU_LOAI_ID (Gán NULL)

    MA_DEN_LOAI,                               -- DEN_LOAI_ID (Gán NULL)

    A.NGAY_BIEN_BAN,                    -- NGAY_BIEN_BAN

    SUBSTR(A.GHI_CHU, 1, 1000),         -- GHI_CHU (Cắt chuỗi tránh lỗi tràn dung lượng)

    C.ID,                               -- PHAM_NHAN_ID (Lấy từ bảng Lai lịch mới)

    SYSDATE,                            -- CREATION_TIME

    NULL,                               -- CREATOR_USER_ID

    NULL,                               -- LAST_MODIFICATION_TIME

    NULL,                               -- LAST_MODIFIER_USER_ID

    0,                                  -- IS_DELETED

    NULL,                               -- DELETER_USER_ID

    NULL                                -- DELETION_TIME

FROM "QLPN_OLD"."PN_PHAN_LOAI_GIAM_GIU" A

INNER JOIN "QLPN_OLD"."PN_LAI_LICH" B ON A.PN_ID = B.PN_ID

INNER JOIN "QLPN"."PN_LAI_LICHS" C ON B.SO_HSLD = C.LL_SO_HO_SO_LAN_DAU;