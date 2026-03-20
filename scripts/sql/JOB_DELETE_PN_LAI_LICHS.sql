-- ===== NHÓM CHI TIẾT / PHỤ THUỘC =====


DELETE FROM qlpn.bien_ban_ghi_nhans;

DELETE FROM qlpn.chua_benh_bat_buocs;

DELETE FROM qlpn.dx_du_dieu_kien_dac_xas;

DELETE FROM qlpn.dx_hinh_phat_bo_sungs;

DELETE FROM qlpn.dx_thoi_gian_chap_hanhs;

DELETE FROM qlpn.dx_thoi_gian_con_laies;

DELETE FROM qlpn.dx_toi_danh_riengs;

DELETE FROM qlpn.dx_truong_hop_dac_biets;

DELETE FROM qlpn.dx_xep_loai_cai_taos;

DELETE FROM qlpn.gth_hinh_phat_bo_sungs;

DELETE FROM qlpn.gth_ky_giam_thoi_hans;

DELETE FROM qlpn.gth_thoi_gian_chap_hanhs;

DELETE FROM qlpn.gth_truong_hop_dac_biets;

DELETE FROM qlpn.gth_xep_loai_cai_taos;


-- KỲ XẾP LOẠI CẢI TẠO


DELETE FROM qlpn.mo_ky_xep_loai_cai_tao_pn_lai_lich;

DELETE FROM qlpn.mo_ky_xep_loai_cai_taos;


-- ===== NHÓM NGHIỆP VỤ =====



DELETE FROM qlpn.pn_du_dieu_kien_xet_thas;

DELETE FROM qlpn.pn_dot_tha_co_dieu_kiens;

DELETE FROM qlpn.giam_thoi_hans;

DELETE FROM qlpn.hoan_thi_hanh_ans;

DELETE FROM qlpn.khen_thuongs;

DELETE FROM qlpn.mien_hinh_phats;

DELETE FROM qlpn.tron_trais;

DELETE FROM qlpn.trich_xuats;

DELETE FROM qlpn.pn_ket_thuc_chets;

DELETE FROM qlpn.pn_ket_thuc_ra_trais;

DELETE FROM qlpn.pn_giam_riengs;

DELETE FROM qlpn.pn_dac_xas;

DELETE FROM qlpn.pn_chuyen_giao;

DELETE FROM qlpn.pn_ban_an_khacs;

DELETE FROM qlpn.pn_suc_khoes;

-- ===== NHÓM PHẠM NHÂN (ĐÚNG ERD) =====


DELETE FROM qlpn.pham_nhan_chuyen_trai;

DELETE FROM qlpn.pham_nhan_phan_doi;

DELETE FROM qlpn.pham_nhan_phan_buong;

DELETE FROM qlpn.pn_nghien_mt;

DELETE FROM qlpn.pn_nhan_dangs;

DELETE FROM qlpn.pn_quyet_dinh_ban_an_khacs;

DELETE FROM qlpn.pn_tai_lieu_dinh_kems;

DELETE FROM qlpn.pn_thi_hanh_an;

DELETE FROM qlpn.pn_tien_an;

DELETE FROM qlpn.pn_tien_sus;

DELETE FROM qlpn.tam_dinh_chis;

DELETE FROM qlpn.nop_luus;

DELETE FROM qlpn.xep_loais;

DELETE FROM qlpn.tha_co_dieu_kiens;

DELETE FROM qlpn.than_nhan_con_cais;

DELETE FROM qlpn.than_nhan_nguoi_thans;

DELETE FROM qlpn.chua_benh_bat_buocs;

DELETE FROM qlpn.pn_hinh_phat_bo_sungs;

DELETE FROM qlpn.pn_lai_lichs;


-- Đặc Xá 


DELETE FROM qlpn.dx_dieu_kien_dac_xas;
DELETE FROM qlpn.pn_dot_dac_xas;



-- GIẢM THỜI HẠN


DELETE FROM qlpn.gth_dieu_kien_giam_thoi_hans;

DELETE FROM qlpn.pn_giao_nhan_phan_nhans;

DELETE FROM qlpn.dm_buong_giams;

DELETE FROM qlpn.dot_quyet_dinh_trch_xuats;

-- CREATE GLOBAL TEMPORARY TABLE qlpn.tmp_pn_bb_map (
--     old_id VARCHAR2(12),
--     new_id NUMBER
-- ) ON COMMIT PRESERVE ROWS;