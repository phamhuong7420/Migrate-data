-- New script in ORCL.
-- Date: Mar 17, 2026
-- Time: 6:23:55 PM
-- 1. DELETE toàn bộ bảng con & FK liên quan
DELETE FROM QLPN.DX_THOI_GIAN_CON_LAIES;
COMMIT;

DELETE FROM QLPN.DX_XEP_LOAI_CAI_TAOS;
COMMIT;

DELETE FROM QLPN.DX_THOI_GIAN_CHAP_HANHS;
COMMIT;

DELETE FROM QLPN.DX_HINH_PHAT_BO_SUNGS;
COMMIT;

DELETE FROM QLPN.DX_TOI_DANH_RIENGS;
COMMIT;

DELETE FROM QLPN.DX_DIEU_KIEN_DAC_XAS;
COMMIT;

DELETE FROM QLPN.DX_DU_DIEU_KIEN_DAC_XAS;
COMMIT;




-- 2. TRUNCATE bảng cha
DELETE FROM QLPN.PN_DOT_DAC_XAS;

INSERT INTO QLPN.PN_DOT_DAC_XAS (
    DDX_NAM_DAC_XA,
    DDX_TEN_DOT,
    DDX_NGAY_XET,
    DDX_SO_QUYET_DINH,
    DDX_NGAY_QUYET_DINH,
    DDX_SO_HUONG_DAN,
    DDX_NGAY_HUONG_DAN,
    DDX_TRANG_THAI,
    DDX_DA_KHOA_LAN_DAU,
    DDX_MA_XAC_THUC_CUC,
    DDX_TG_MA_TRAI_GIAM,
    DDX_TG_MA_XAC_THUC,
    DDX_TG_NGAY_DE_NGHI,
    DDX_TG_SO_PHIEU,
    DDX_TG_SO_GCN,
    DDX_TG_QUYET_DINH_TU,
    DDX_TG_NGAY_QUYET_DINH,
    DDX_TG_SO_QUYET_DINH_THI_DUA,
    DDX_TG_NGAY_QUYET_DINH_THI_DUA,
    DDX_TG_NGAY_HOP,
    DDX_TG_SO_HUONG_DAN,
    DDX_TG_NGAY_HUONG_DAN,
    DDX_TG_KET_QUA_THI_DUA_TU,
    DDX_TG_KET_QUA_THI_DUA_DEN,
    CREATION_TIME,
    CREATOR_USER_ID,
    LAST_MODIFICATION_TIME,
    LAST_MODIFIER_USER_ID,
    IS_DELETED,
    DELETER_USER_ID,
    DELETION_TIME
)
SELECT
    /* năm đặc xá từ ngày xét */
	-- DDX_NAM_DAC_XA
    TO_CHAR(dx.NGAY_XET_DX, 'YYYY'),
	
	--    DDX_TEN_DOT
    /* tên đợt → không có → generate */
    'Đợt đặc xá ' || TO_CHAR(dx.NGAY_XET_DX, 'YYYY'),
	
	--    DDX_NGAY_XET
    CAST(dx.NGAY_XET_DX AS TIMESTAMP),
	-- DDX_SO_QUYET_DINH
    dx.QD_DX_SO,
    -- DDX_NGAY_QUYET_DINH
    CAST(dx.QD_DX_NGAY AS TIMESTAMP),
	
    -- DDX_SO_HUONG_DAN
    dx.HD_LAM_DX_SO,
    
    -- DDX_NGAY_HUONG_DAN
    CAST(dx.HD_LAM_DX_NGAY AS TIMESTAMP),
	
    -- DDX_TRANG_THAI
    /* NOT NULL → default */
    1,

    -- DDX_DA_KHOA_LAN_DAU
    /* chưa có khái niệm → NULL */
    NULL,
	
    /* ====== block trại giam (mapping tạm) ====== */
	
    /* mã xác thực cục → không có */
    NULL,

    /* mã trại giam → không có */
    NULL,
--    dmmtg.id,
	
    -- DDX_TG_MA_XAC_THUC
    NULL,

    /* ngày đề nghị → dùng ngày xét */
    CAST(dx.NGAY_XET_DX AS TIMESTAMP),

    /* số phiếu → reuse */
    dx.QD_LAM_DX_SO,

    /* số GCN → reuse */
    dx.QD_BIEU_MAU_SO,

    /* quyết định từ → reuse */
    dx.QD_LAM_DX_SO,

    CAST(dx.QD_LAM_DX_NGAY AS TIMESTAMP),

    /* thi đua → không có */
    NULL,
    NULL,

    /* ngày họp */
    CAST(dx.NGAY_HOP_HOI_DONG AS TIMESTAMP),

    /* hướng dẫn TG → reuse */
    dx.HD_LAM_DX_SO,
    CAST(dx.HD_LAM_DX_NGAY AS TIMESTAMP),

    /* kết quả thi đua → không có */
    NULL,
    NULL,

    /* audit */
    SYSTIMESTAMP,
    NULL,
    NULL,
    NULL,
    0,
    NULL,
    NULL

FROM QLPN_OLD.PN_DX_DOT_DAC_XA dx;

