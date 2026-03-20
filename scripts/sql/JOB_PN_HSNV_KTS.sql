delete from qlpn.pn_hsnv_kts;

INSERT INTO qlpn.pn_hsnv_kts (

    ho_ten_bg,

    cap_bac_bg,

    chuc_vu_bg,

    ma_cq_bg,

    ma_dp_bg,

    ho_ten_bn,

    cap_bac_bn,

    chuc_vu_bn,

    ma_cq_bn,

    ma_dp_bn,

    ho_ten_dk,

    cap_bac_dk,

    chuc_vu_dk,

    ma_cq_dk,

    ma_dp_dk,

    ngay_dk,

    creation_time,

    is_deleted

) 

 SELECT

    ho_ten_bg,

    cap_bac_bg,

    chuc_vu_bg,

DCQBG.ID    ma_cq_bg,

DDVHCBG.ID    ma_dp_bg,

    ho_ten_bn,

    cap_bac_bn,

    chuc_vu_bn,

DCQBN.ID    ma_cq_bn,

DDVHCBN.ID    ma_dp_bn,

    ho_ten_dk,

    cap_bac_dk,

    chuc_vu_dk,

DCQDK.ID    ma_cq_dk,

DDVHCDK.ID    ma_dp_dk,

    ngay_dk,

    CAST(SYSTIMESTAMP AS TIMESTAMP(7)) AS CREATION_TIME,

    0 AS IS_DELETED  

FROM

    qlpn_old.pn_hsnv_kt PHK

LEFT JOIN qlpn.DM_CO_QUANS DCQBG ON DCQBG.CQ_MA = PHK.ma_cq_bg    

LEFT JOIN qlpn.DM_DON_VI_HANH_CHINH DDVHCBG ON DDVHCBG.DVHC_MA_DVHC = PHK.ma_dp_bg    

LEFT JOIN qlpn.DM_CO_QUANS DCQBN ON DCQBN.CQ_MA = PHK.ma_cq_bn    

LEFT JOIN qlpn.DM_DON_VI_HANH_CHINH DDVHCBN ON DDVHCBN.DVHC_MA_DVHC = PHK.ma_dp_bn    

LEFT JOIN qlpn.DM_CO_QUANS DCQDK ON DCQDK.CQ_MA = PHK.ma_cq_bn    

LEFT JOIN qlpn.DM_DON_VI_HANH_CHINH DDVHCDK ON DDVHCDK.DVHC_MA_DVHC = PHK.ma_dp_bn;