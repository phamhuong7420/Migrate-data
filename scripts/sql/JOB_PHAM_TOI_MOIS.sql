DELETE FROM "QLPN"."PHAM_TOI_MOIS";

INSERT INTO "QLPN"."PHAM_TOI_MOIS" (

    NGAY_PHAM_TOI,

    NOI_PHAM_TOI,

    TOI_DANH_ID,

    HINH_THUC_XU_LY,

    GHI_CHU,

    PHAM_NHAN_ID,

    CREATION_TIME,

    CREATOR_USER_ID,

    LAST_MODIFICATION_TIME,

    LAST_MODIFIER_USER_ID,

    IS_DELETED,

    DELETER_USER_ID,

    DELETION_TIME,

    DM_TOI_DANH,

    MUC_DO_NGHIEM_TRONG

)

SELECT 
 /*+ USE_HASH(B C) */
    A.NGAY_PT_MOI,                      -- NGAY_PHAM_TOI

    A.NOI_PT_MOI,                       -- NOI_PHAM_TOI

    D.ID,                               -- TOI_DANH_ID (Gán NULL vì chưa có ID danh mục)

    A.HINH_THUC_XL,                     -- HINH_THUC_XU_LY

    SUBSTR(A.GHI_CHU_PTM, 1, 1000),     -- GHI_CHU (Cắt về 1000 để tránh ORA-01401)

    C.ID,                               -- PHAM_NHAN_ID (Lấy từ bảng Lai lịch mới)

    SYSDATE,                            -- CREATION_TIME

    NULL,                               -- CREATOR_USER_ID

    NULL,                               -- LAST_MODIFICATION_TIME

    NULL,                               -- LAST_MODIFIER_USER_ID

    0,                                  -- IS_DELETED

    NULL,                               -- DELETER_USER_ID

    NULL,                               -- DELETION_TIME

    D.TD_TEN,                  -- DM_TOI_DANH (Lưu mã cũ vào cột text)

    NULL                                -- MUC_DO_NGHIEM_TRONG

FROM "QLPN_OLD"."PN_PHAM_TOI_MOI" A

INNER JOIN "QLPN_OLD"."PN_LAI_LICH" B ON A.PN_ID = B.PN_ID

INNER JOIN "QLPN"."PN_LAI_LICHS" C ON B.SO_HSLD = C.LL_SO_HO_SO_LAN_DAU

INNER JOIN "QLPN"."DM_TOI_DANHS" D ON A.MA_TOI_DANH_PTM = D.TD_MA;