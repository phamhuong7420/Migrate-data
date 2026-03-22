DELETE
FROM
    QLPN.AN_GIAMS;

INSERT INTO
    qlpn.an_giams (
        pham_nhan_id,
        ag_so_qd_chu_tich_nuoc,
        ag_ngay_qd_chu_tich_nuoc,
        ag_ghi_chu,
        ag_so_qd_toa_an,
        ag_ngay_qd_toa_an,
        ag_ly_do_an_giam,
        creation_time,
        is_deleted
    )
SELECT
    pll.id_moi,
    so_qd,
    ngay_qd,
    ghi_chu,
    qd_giam_toa_an_so,
    qd_giam_toa_an_ngay,
    dmag.id,
    sysdate AS creation_time,
    0 AS is_deleted
FROM
    qlpn_old.pn_qd_an_giam ag
    LEFT JOIN (
        SELECT
            lln.id AS id_moi,
            llo.pn_id AS id_cu
        FROM
            qlpn_old.pn_lai_lich llo
            LEFT JOIN qlpn.pn_lai_lichs lln ON lln.ll_so_ho_so_lan_dau = llo.so_hsld
    ) pll ON ag.pn_id = pll.id_cu
    LEFT JOIN qlpn.dm_ly_do_an_giams dmag ON ag.ma_ld_an_giam = dmag.ldag_ma;