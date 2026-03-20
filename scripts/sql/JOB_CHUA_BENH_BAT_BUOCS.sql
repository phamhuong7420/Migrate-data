DELETE FROM QLPN.CHUA_BENH_BAT_BUOCS;
    INSERT INTO qlpn.chua_benh_bat_buocs (
        pham_nhan_id,
        so_quyet_dinh,
        ngay_quyet_dinh,
        co_quan_quyet_dinh_id,
        dia_phuong_quyet_dinh_id,
        thoi_han,
        tu_ngay,
        benh_tat_id,
        chi_tiet_benh,
        ngay_bat_dau,
        ngay_ket_thuc,
        noi_chua_benh,
        creation_time,
        is_deleted
    )
        SELECT
            pll.id_moi AS pn_id,
            qd_so,
            qd_ngay,
            dcq.id     AS ma_cq_toa_an_qd,
            ddvhc.id   AS ma_dp_toa_an_qd,
            dmtg.id    AS ma_muc_thoi_han,
            tu_ngay,
            dbt.id     AS ma_benh_tat,
            benh_tat,
            ngay_bat_dau,
            ngay_ket_thuc,
            noi_chua_benh,
            sysdate    AS creation_time,
            0          AS is_deleted
        FROM
            qlpn_old.pn_chua_benh_bat_buoc cbbb
            LEFT JOIN (
                SELECT
                    lln.id    AS id_moi,
                    llo.pn_id AS id_cu
                FROM
                    qlpn_old.pn_lai_lich llo
                    LEFT JOIN qlpn.pn_lai_lichs    lln ON lln.ll_so_ho_so_lan_dau = llo.so_hsld
            )                              pll ON cbbb.pn_id = pll.id_cu
            LEFT JOIN qlpn.dm_co_quans               dcq ON cbbb.ma_cq_toa_an_qd = dcq.cq_ma
            LEFT JOIN qlpn.dm_don_vi_hanh_chinh      ddvhc ON cbbb.ma_dp_toa_an_qd = ddvhc.dvhc_ma_dvhc
            LEFT JOIN qlpn.dm_benh_tats              dbt ON cbbb.ma_benh_tat = dbt.bt_ma
            LEFT JOIN qlpn.dm_ma_thoi_gians          dmtg ON cbbb.ma_muc_thoi_han = dmtg.mtg_ma;