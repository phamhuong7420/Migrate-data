DELETE FROM "QLPN"."PN_DAC_XAS";

INSERT INTO "QLPN"."PN_DAC_XAS" (

    DX_NGAY_XET,

    DX_NGAY_THA,

    DX_NGAY_TRINH_DIEN,

    DX_THOI_GIAN_GIAM,

    DX_XEP_LOAI_CAI_TAO,

    DX_TINH_TRANG_DAC_XA,

    DX_YKIEN_DOI_PN,

    DX_NHAN_XET_TRAI,

    DX_KQCT_PHIEU,

    DX_KQCT_DS,

    PNLAI_LICH_ID,

    CREATION_TIME,

    CREATOR_USER_ID,

    IS_DELETED

)

SELECT 

    A.NGAY_XET_DX,                      -- DX_NGAY_XET

    A.NGAY_THA_DX,                     -- DX_NGAY_THA

    A.NGAY_TRINH_DIEN_DX,               -- DX_NGAY_TRINH_DIEN

    A.MA_MUC_TG_CHAP_HANH_DX,           -- DX_THOI_GIAN_GIAM

    A.DANH_GIA_CAI_TAO,                 -- DX_XEP_LOAI_CAI_TAO

    A.MA_DUOC_DAC_XA,                   -- DX_TINH_TRANG_DAC_XA

    TO_NCLOB(A.Y_KIEN_DOI_PN),          -- DX_YKIEN_DOI_PN

    TO_NCLOB(A.NHAN_XET_DE_NGHI),       -- DX_NHAN_XET_TRAI

    TO_NCLOB(A.KQCT_PHIEU_DX),          -- DX_KQCT_PHIEU

    TO_NCLOB(A.KQCT_DS_DX),             -- DX_KQCT_DS

    C.ID,                               -- PNLAI_LICH_ID (ID từ bảng Lai lịch mới)

    SYSDATE,                            -- CREATION_TIME

    NULL,                               -- CREATOR_USER_ID

    0                                   -- IS_DELETED

FROM "QLPN_OLD"."PN_DX_DAC_XA" A

INNER JOIN "QLPN_OLD"."PN_LAI_LICH" B ON A.PN_ID = B.PN_ID

INNER JOIN "QLPN"."PN_LAI_LICHS" C ON B.SO_HSLD = C.LL_SO_HO_SO_LAN_DAU

LEFT JOIN  "QLPN"."DM_DUOC_DAC_XAS"  DDDX   ON DDDX.DDX_MA = A.MA_DUOC_DAC_XA;