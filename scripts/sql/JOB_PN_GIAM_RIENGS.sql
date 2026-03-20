

DELETE FROM QLPN.PN_GIAM_RIENGS;
    INSERT INTO QLPN.pn_giam_riengs (
        gr_loai_quyet_dinh,
        gr_so_quyet_dinh_giam_rieng,
        gr_ngay_quyet_dinh_giam_rieng,
        gr_ly_do,
        gr_chi_tiet_ly_do,
        gr_thoi_han_giam_rieng,
        gr_ngay_bat_dau,
        gr_so_quyet_dinh_ra,
        gr_ngay_quyet_dinh_ra,
        gr_ngay_ketthuc,
        gr_ghi_chu,
        gr_ly_do_dua_ra,
        gr_pn_lai_lich_id,
        creation_time,
        is_deleted
    )
        SELECT /*+ USE_HASH(ll  lln) */
            pg.ma_lenh_giam_rieng,
            pg.so_qd_gr,
            pg.ngay_qd_gr,
            pg.ma_ly_do_gr,
            pg.ly_do_giam_rieng,
            pg.ma_muc_thoi_han_gr,
            pg.ngay_bd_gr,
            pg.so_qd_ra,
            pg.ngay_qd_ra,
            pg.ngay_kt_gr,
            pg.ghi_chu_gr,
            pg.ly_do_dua_ra,
            lln.id                             pn_id,
            CAST(systimestamp AS TIMESTAMP(7)) AS creation_time,
            0                                  AS is_deleted
        FROM
                 qlpn_old.pn_gr_giam_rieng pg
            INNER JOIN qlpn_old.pn_lai_lich  ll ON pg.pn_id = ll.pn_id
            INNER JOIN QLPN.pn_lai_lichs lln ON ll.so_hsld = lln.ll_so_ho_so_lan_dau;
            
            