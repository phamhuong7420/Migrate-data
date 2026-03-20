TRUNCATE TABLE QLPN.THAN_NHAN_NGUOI_THANS;
INSERT INTO QLPN.THAN_NHAN_NGUOI_THANS (
    HO_TEN_THAN_NHAN,
    CCCD,
    QUAN_HE_ID,
    GIOI_TINH_ID,
    NGAY_SINH,
    NGHE_NGHIEP,
    NOI_O,
    DIEN_THOAI,
    PHAM_NHAN_ID,
    CREATION_TIME,
    CREATOR_USER_ID,
    LAST_MODIFICATION_TIME,
    LAST_MODIFIER_USER_ID,
    IS_DELETED,
    DELETER_USER_ID,
    DELETION_TIME,
    THONG_BAO,
    GHI_CHU
)
SELECT
 /*+ USE_HASH(pll pll2) */
    qhgd.HO_TEN_QHGD,
    /* Old không có CCCD → NULL */
    NULL,
    /* cần map danh mục */
    qhpn.ID,
    gt.ID,
    /* Old đang là VARCHAR2(10) → giữ nguyên hoặc normalize */
    CASE
        WHEN REGEXP_LIKE(qhgd.NGAY_SINH_QHGD, '^\d{2}/\d{2}/\d{4}$')
        THEN TO_CHAR(TO_DATE(qhgd.NGAY_SINH_QHGD, 'DD/MM/YYYY'), 'YYYY-MM-DD')
        WHEN REGEXP_LIKE(qhgd.NGAY_SINH_QHGD, '^\d{4}-\d{2}-\d{2}$')
        THEN qhgd.NGAY_SINH_QHGD
        ELSE NULL
    END,
    qhgd.NGHE_NGHIEP_QHGD,
    qhgd.NOI_O,
    SUBSTR(qhgd.DIEN_THOAI, 1, 10),
    pll2.ID,
    /* Audit */
    SYSTIMESTAMP,
    NULL,
    NULL,
    NULL,
    0,
    NULL,
    NULL,
    /* BAO_TIN → THONG_BAO */
    qhgd.BAO_TIN,
    /* VARCHAR2 → NCLOB */
    qhgd.GHI_CHU_QHGD
FROM QLPN_OLD.PN_QUAN_HE_GIA_DINH qhgd
INNER JOIN QLPN_OLD.PN_LAI_LICH pll
    ON pll.PN_ID = qhgd.PN_ID
INNER JOIN QLPN.PN_LAI_LICHS pll2
    ON pll2.LL_SO_HO_SO_LAN_DAU = pll.SO_HSLD
INNER JOIN QLPN.DM_QUAN_HE_VOI_PHAM_NHANS qhpn
	ON qhpn.QHVPN_MA = qhgd.MA_QUAN_HE
INNER JOIN QLPN.DM_GIOI_TINHS gt
	ON gt.GT_MA = qhgd.MA_GIOI_TINH_QHGD;