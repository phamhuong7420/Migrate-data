DELETE FROM "QLPN"."PHAM_NHAN_PHAN_BUONG" ;



INSERT INTO "QLPN"."PHAM_NHAN_PHAN_BUONG" 



(PNPB_NGAY_CHUYEN,PNPB_GHI_CHU,PNPB_LAI_LICH_ID,PNPB_SO_BUONG,CREATION_TIME,CREATOR_USER_ID,IS_DELETED)



SELECT 



    A.NGAY_CHUYEN,                     -- PNPB_NGAY_CHUYEN



    A.GHI_CHU,                         -- PNPB_GHI_CHU



    C.ID,                              -- PNPB_LAI_LICH_ID (Lấy từ bảng mới)



    A.SO_HIEU_BUONG,                   -- PNPB_SO_BUONG



    SYSDATE,                           -- CREATION_TIME



    NULL,                              -- CREATOR_USER_ID



    0                                  -- IS_DELETED



FROM "QLPN_OLD"."PN_PHAN_BUONG" A



INNER JOIN "QLPN_OLD".PN_LAI_LICH B



ON A.PN_ID =B.PN_ID 



INNER JOIN "QLPN".PN_LAI_LICHS C



ON B.SO_HSLD = C.LL_SO_HO_SO_LAN_DAU;