-- New script in ORCL.

-- Date: Mar 17, 2026

-- Time: 4:48:00 PM

TRUNCATE TABLE QLPN.TIEN_LAO_DONG_SAN_XUATS;



INSERT INTO QLPN.TIEN_LAO_DONG_SAN_XUATS (

    NAM,

    TIEN_Q1,

    TIEN_Q2,

    TIEN_Q3,

    TIEN_Q4,

    TIEN_KHAC,

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
 /*+ USE_HASH(pll  pll2) */
    tcsx.NAM_SX,



    /* NUMBER(10,0) → NUMBER(38,2) OK */

    tcsx.TIEN_Q1,

    tcsx.TIEN_Q2,

    tcsx.TIEN_Q3,

    tcsx.TIEN_Q4,

    tcsx.TIEN_KHAC,



    tcsx.GHI_CHU,



    pll2.ID,



    /* Audit */

    SYSTIMESTAMP,

    NULL,

    NULL,

    NULL,

    0,

    NULL,

    NULL



FROM QLPN_OLD.PN_TIEN_CONG_SX tcsx



INNER JOIN QLPN_OLD.PN_LAI_LICH pll

    ON pll.PN_ID = tcsx.PN_ID



INNER JOIN QLPN.PN_LAI_LICHS pll2

    ON pll2.LL_SO_HO_SO_LAN_DAU = pll.SO_HSLD;