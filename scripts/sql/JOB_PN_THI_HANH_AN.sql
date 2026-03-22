DELETE FROM QLPN.PN_THI_HANH_AN;
INSERT INTO qlpn.pn_thi_hanh_an (
    tha_dieu,
    tha_khoan,
    tha_toi_danh,
    tha_can_cu_tuyen_phat,
    tha_ngay_bat,
    tha_an_phat,
    tha_co_quan_bat_id,
    tha_ngay_pham_toi,
    tha_so_ban_an,
    tha_thoi_gian_tam_giam,
    tha_thoi_gian_tam_giu,
    tha_thoi_gian_o_truoc,
    tha_ngay_qd_tha,
    tha_ngay_ban_an,
    tha_loai_ban_an_id,
    tha_toa_qd_id,
    tha_ngay_den_trai,
    tha_so_qd_dieu_chuyen,
    tha_ngay_qd_dieu_chuyen,
    tha_so_de_nghi,
    tha_ngay_de_nghi,
    tha_don_vi_de_nghi_id,
    tha_chuyen_den_tu_id,
    tha_ngay_het_han,
    tha_so_gcn_chxhpt,
    tha_so_danh_ban,
    tha_ngay_lap_danh_ban,
    tha_co_quan_lap_id,
    tha_cong_thuc_van_tay,
    tha_cong_thuc_van_tay_2,
    tha_ghi_chu_dac_biet,
    tha_chi_muc_tiem_kiem,
    tha_so_qd_tha,
    tha_doi_tuong,
    tha_cap_toa_id,
    tha_dia_phuong_don_vi_de_nghi_id,
    tha_dia_phuong_toa_qd_tha_id,
    tha_dia_phuong_don_co_quan_lap_id,
    tha_dia_phuong_cap_toa_id,
    tha_pnlai_lich_id,
    tha_dia_phuong_co_quan_bat_id,
    creation_time,
    creator_user_id,
    deleter_user_id,
    deletion_time,
    is_deleted,
    last_modification_time,
    last_modifier_user_id,
    tha_tong_an_giam,
    tha_ngay_bat_dau_o_tu,
    tha_muc_an_id,
    tha_thoi_gian_con_lai,
    tha_thoi_gian_da_chap_hanh,
    tha_ma_an_phat,
    tha_buong_giam,
    tha_an_phat_goc,
    tha_muc_do_nghiem_trong_id
)
SELECT
	NULL AS THA_DIEU,
	NULL AS THA_KHOAN,
	'[' || (
		SELECT LISTAGG(TD.ID, ',') WITHIN GROUP (ORDER BY TOK.POS)
		FROM (
			SELECT TRIM(REGEXP_SUBSTR(LL.MA_TOI_DANH, '[^,;]+', 1, LEVEL)) AS MA_TOI_DANH,
				   LEVEL AS POS
			FROM DUAL
			CONNECT BY REGEXP_SUBSTR(LL.MA_TOI_DANH, '[^,;]+', 1, LEVEL) IS NOT NULL
		) TOK
		LEFT JOIN QLPN.DM_TOI_DANHS TD ON TRIM(TOK.MA_TOI_DANH) = TRIM(TO_CHAR(TD.TD_MA))
	) || ']' AS THA_TOI_DANH,
	LL.CAN_CU AS THA_CAN_CU_TUYEN_PHAT,
	LL.NGAY_BAT AS THA_NGAY_BAT,
	MTG_AN_PHAT.MTG_TITLE AS THA_AN_PHAT,
	CQ_RL_BAT.ID AS THA_CO_QUAN_BAT_ID,
	LL.NGAY_PHAM_TOI AS THA_NGAY_PHAM_TOI,
	LL.BAN_AN_SO AS THA_SO_BAN_AN,
	MTG_TAM_GIAM.MTG_TITLE AS THA_THOI_GIAN_TAM_GIAM,
	MTG_TAM_GIU.MTG_TITLE AS THA_THOI_GIAN_TAM_GIU,
	MTG_O_TRUOC.MTG_TITLE AS THA_THOI_GIAN_O_TRUOC,
	LL.NGAY_QD_TH_AN AS THA_NGAY_QD_THA,
	LL.NGAY_XU AS THA_NGAY_BAN_AN,
	CAP_TOA_XU.ID AS THA_LOAI_BAN_AN_ID,
	CQ_TOA_QD.ID AS THA_TOA_QD_ID,
	LL.NGAY_DEN_TRAI AS THA_NGAY_DEN_TRAI,
	LL.SO_QD_DC AS THA_SO_QD_DIEU_CHUYEN,
	LL.NGAY_QD_DC AS THA_NGAY_QD_DIEU_CHUYEN,
	LL.SO_DE_NGHI_DC AS THA_SO_DE_NGHI,
	LL.NGAY_DE_NGHI_DC AS THA_NGAY_DE_NGHI,
	CQ_DN_DC.ID AS THA_DON_VI_DE_NGHI_ID,
	TG_CHUYEN_DEN.ID AS THA_CHUYEN_DEN_TU_ID,
	LL.NGAY_RA_TRAI AS THA_NGAY_HET_HAN,
	NULL AS THA_SO_GCN_CHXHPT,
	LL.SO_DB AS THA_SO_DANH_BAN,
	LL.NGAY_LAP_DB AS THA_NGAY_LAP_DANH_BAN,
	CQ_LAP_DB.ID AS THA_CO_QUAN_LAP_ID,
	LL.CT_VAN_TAY1 AS THA_CONG_THUC_VAN_TAY,
	LL.CT_VAN_TAY1 AS THA_CONG_THUC_VAN_TAY_2,
	NULL AS THA_GHI_CHU_DAC_BIET,
	1 AS THA_CHI_MUC_TIEM_KIEM,
	LL.SO_QD_TH_AN AS THA_SO_QD_THA,
	LDT.ID AS THA_DOI_TUONG,
	CQ_TOA_XU.ID AS THA_CAP_TOA_ID,
	dttp_dn_dc.ID AS THA_DIA_PHUONG_DON_VI_DE_NGHI_ID,
	dttp_toa_qd.ID AS THA_DIA_PHUONG_TOA_QD_THA_ID,
	dttp_lap_db.ID AS THA_DIA_PHUONG_DON_CO_QUAN_LAP_ID,
	dttp_toa_xu.ID AS THA_DIA_PHUONG_CAP_TOA_ID,
	LLN.ID AS THA_PNLAI_LICH_ID,
	dttp_rl_bat.ID AS THA_DIA_PHUONG_CO_QUAN_BAT_ID,
	SYSDATE AS CREATION_TIME,
	 1 AS CREATOR_USER_ID,
	NULL AS DELETER_USER_ID,
	NULL AS DELETION_TIME,
	0 AS IS_DELETED,
	NULL AS LAST_MODIFICATION_TIME,
	NULL AS LAST_MODIFIER_USER_ID,
	NULL AS THA_TONG_AN_GIAM,
    NULL AS  tha_ngay_bat_dau_o_tu,
   QLPN.PKG_TINH_AN.FN_GET_MUC_AN_ID_TU_AN_PHAT(CAST(MTG_AN_PHAT.MTG_TITLE AS NVARCHAR2(2000))) AS tha_muc_an_id,
    NULL AS tha_thoi_gian_con_lai,
    NULL AS tha_thoi_gian_da_chap_hanh,
    NULL AS tha_ma_an_phat,
    NULL AS tha_buong_giam,
    MTG_AN_PHAT.MTG_TITLE AS tha_an_phat_goc,
    NULL AS tha_muc_do_nghiem_trong_id
FROM QLPN_OLD.PN_LAI_LICH LL
LEFT JOIN QLPN.PN_LAI_LICHS LLN ON TO_CHAR(LL.SO_HSLD) = TO_CHAR(LLN.LL_SO_HO_SO_LAN_DAU)
LEFT JOIN QLPN.DM_MA_THOI_GIANS MTG_TAM_GIAM ON TO_CHAR(LL.MA_MUC_TAM_GIAM) = TO_CHAR(MTG_TAM_GIAM.MTG_MA)
LEFT JOIN QLPN.DM_MA_THOI_GIANS MTG_TAM_GIU ON TO_CHAR(LL.MA_MUC_TAM_GIU) = TO_CHAR(MTG_TAM_GIU.MTG_MA)
LEFT JOIN QLPN.DM_MA_THOI_GIANS MTG_O_TRUOC ON TO_CHAR(LL.MA_MUC_TG_O_TRUOC) = TO_CHAR(MTG_O_TRUOC.MTG_MA)
LEFT JOIN QLPN.DM_MA_THOI_GIANS MTG_AN_PHAT ON TO_CHAR(LL.MA_MUC_AN_PHAT) = TO_CHAR(MTG_AN_PHAT.MTG_MA)
LEFT JOIN QLPN.DM_CO_QUANS CQ_RL_BAT ON TO_CHAR(LL.MA_CQ_RL_BAT) = TO_CHAR(CQ_RL_BAT.CQ_MA)
LEFT JOIN QLPN.DM_CAP_TOA_XUS CAP_TOA_XU ON TO_CHAR(LL.MA_CAP_TOA_XU) = TO_CHAR(CAP_TOA_XU.CTX_MA)
LEFT JOIN QLPN.DM_CO_QUANS CQ_TOA_QD ON TO_CHAR(ll.MA_CQ_TOA_QD) = TO_CHAR(CQ_TOA_QD.CQ_MA)
LEFT JOIN QLPN.DM_CO_QUANS CQ_DN_DC ON TO_CHAR(LL.MA_CQ_DN_DC) = TO_CHAR(CQ_DN_DC.CQ_MA)
LEFT JOIN QLPN.DM_TRAI_GIAMS TG_CHUYEN_DEN ON TO_CHAR(LL.MA_TRAI_CHUYEN_DEN) = TO_CHAR(TG_CHUYEN_DEN.TG_MA)
LEFT JOIN QLPN.DM_CO_QUANS CQ_LAP_DB ON TO_CHAR(LL.MA_CQ_LAP_DB) = TO_CHAR(CQ_LAP_DB.CQ_MA)
LEFT JOIN QLPN.DM_CO_QUANS CQ_TOA_XU ON TO_CHAR(LL.MA_CQ_TOA_XU) = TO_CHAR(CQ_TOA_XU.CQ_MA)
LEFT JOIN QLPN.DM_LOAI_DOI_TUONGS LDT ON TO_CHAR(LL.MA_LOAI_DOI_TUONG) = TO_CHAR(LDT.LDT_MA)
LEFT JOIN QLPN.MAPPING_PROVINCE mp_dn_dc
  ON TRIM(TO_CHAR(mp_dn_dc.MA_TINH_OLD)) = TRIM(LL.MA_DP_DN_DC)
LEFT JOIN QLPN.DM_TINH_THANH_PHO dttp_dn_dc
  ON TRIM(mp_dn_dc.MA_TINH_NEW) = TRIM(dttp_dn_dc.TTP_MA)
 AND NVL(dttp_dn_dc.IS_DELETED, 0) = 0
LEFT JOIN QLPN.MAPPING_PROVINCE mp_toa_qd
  ON TRIM(TO_CHAR(mp_toa_qd.MA_TINH_OLD)) = TRIM(LL.MA_DP_TOA_QD)
LEFT JOIN QLPN.DM_TINH_THANH_PHO dttp_toa_qd
  ON TRIM(mp_toa_qd.MA_TINH_NEW) = TRIM(dttp_toa_qd.TTP_MA)
 AND NVL(dttp_toa_qd.IS_DELETED, 0) = 0
LEFT JOIN QLPN.MAPPING_PROVINCE mp_lap_db
  ON TRIM(TO_CHAR(mp_lap_db.MA_TINH_OLD)) = TRIM(LL.MA_DP_LAP_DB)
LEFT JOIN QLPN.DM_TINH_THANH_PHO dttp_lap_db
  ON TRIM(mp_lap_db.MA_TINH_NEW) = TRIM(dttp_lap_db.TTP_MA)
 AND NVL(dttp_lap_db.IS_DELETED, 0) = 0
LEFT JOIN QLPN.MAPPING_PROVINCE mp_toa_xu
  ON TRIM(TO_CHAR(mp_toa_xu.MA_TINH_OLD)) = TRIM(LL.MA_DP_TOA_XU)
LEFT JOIN QLPN.DM_TINH_THANH_PHO dttp_toa_xu
  ON TRIM(mp_toa_xu.MA_TINH_NEW) = TRIM(dttp_toa_xu.TTP_MA)
 AND NVL(dttp_toa_xu.IS_DELETED, 0) = 0
LEFT JOIN QLPN.MAPPING_PROVINCE mp_rl_bat
  ON TRIM(TO_CHAR(mp_rl_bat.MA_TINH_OLD)) = TRIM(LL.MA_DP_RL_BAT)
LEFT JOIN QLPN.DM_TINH_THANH_PHO dttp_rl_bat
  ON TRIM(mp_rl_bat.MA_TINH_NEW) = TRIM(dttp_rl_bat.TTP_MA)
 AND NVL(dttp_rl_bat.IS_DELETED, 0) = 0;