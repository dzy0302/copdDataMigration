#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymysql
import time
import logging
import configparser
import datetime
import requests
import hashlib
import json

# 第一步，创建一个logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Log等级总开关

# 第二步，创建一个handler，用于写入日志文件
logfile = 'logs/logs.txt'
fh = logging.FileHandler(logfile, mode='a', encoding='utf-8')
fh.setLevel(logging.ERROR)  # 用于写到file的等级开关

# 第三步，再创建一个handler,用于输出到控制台
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)  # 输出到console的log等级的开关

# 第四步，定义handler的输出格式
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# 第五步，将logger添加到handler里面
logger.addHandler(fh)
logger.addHandler(ch)


def request_api(page):
    config = configparser.RawConfigParser()
    config.read('configs/request.ini')
    t = time.time()
    timestamp = str(int(t))
    md5_time = hashlib.md5(timestamp.encode("utf8")).hexdigest()
    key = config.get('key', 'key')
    auth_code = hashlib.md5((md5_time + key).encode("utf8")).hexdigest()
    r = requests.get(url=config.get('url', 'url'),
                     headers={'timestamp': str(timestamp), 'authCode': str(auth_code)},
                     params={'page': page})
    if r.status_code == 200:
        response_json = json.loads(r.text)
        if response_json.get('code') == 0 and response_json.get('message') == 'ok':
            data = response_json.get('data')
            return data
        else:
            logging.error('[接口数据获取失败]' + response_json.get('message'))
            return None
    else:
        logging.error('[接口访问异常]' + r.text)
        return None


def get_data():
    # 获取页数
    data_page = request_api(1)
    if data_page is not None:
        total_page = data_page.get('page')
        total_record = data_page.get('total')
        logging.info('api接口正常，开始获取病例数据（共' + str(total_record) + '份）')
        record_list = []
        num_start = 1
        num_end = 0
        for page in range(1, total_page + 1):
            data_record = request_api(page)
            if data_record is not None:
                record_page = data_record.get('data')
                record_list = record_list + record_page
                num_end = num_end + len(record_page)
                logging.info('获取第' + str(num_start) + '-' + str(num_end) + '份电子病例成功，等待1秒')
                num_start = num_end + 1
                time.sleep(1)
        if len(record_list) == total_record:
            logging.info('全部病例数据获取完成')
            return record_list
        else:
            logging.info('数据数量有更新，重新获取数据')
            get_data()
    else:
        return None


def get_conn():
    config = configparser.RawConfigParser()
    config.read('configs/db.ini')
    conn = pymysql.connect(host=config.get('database-mysql', 'host'),
                           port=config.getint('database-mysql', 'port'),
                           user=config.get('database-mysql', 'username'),
                           passwd=config.get('database-mysql', 'password'),
                           db=config.get('database-mysql', 'dbname'))
    return conn


def user_to_db(cur, user):
    patient_id = user.get('uid')
    hzbh = 105100000 + patient_id
    cur.execute('SELECT ID FROM patient where hzbh = %s;', hzbh)
    table_patient_check = cur.fetchone()
    if table_patient_check is None:
        try:
            logging.info('受试者' + str(hzbh) + '基本信息入库')
            cur.execute(
                'INSERT INTO patient(ID, xm, zjlx, zjhm, xb, sjh, gzgzh, bdgzh, hzbh, CREATE_TIME, UPDATE_TIME)'
                'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',
                (None, user.get('name'), 1, user.get('id_card'), user.get('sex'), user.get('mobile'), 0, 0, str(hzbh),
                 time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                 time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            table_patient_id = conn.insert_id()
            conn.commit()
            return table_patient_id
        except Exception as ex:
            logging.error('[插入patient异常]' + str(ex))
            conn.rollback()
            return None
    else:
        table_patient_id = table_patient_check[0]
        try:
            logging.info('受试者' + str(hzbh) + '基本信息刷新')
            cur.execute(
                'UPDATE patient SET xm = %s, zjhm = %s, xb = %s, sjh = %s, UPDATE_TIME = %s WHERE ID = %s;',
                (user.get('name'), user.get('id_card'), user.get('sex'), user.get('mobile'),
                 time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), table_patient_id))
            conn.commit()
            return table_patient_id
        except Exception as ex:
            logging.error('[更新patient异常]' + str(ex))
            conn.rollback()
            return None


def common_to_db(cur, common, table_patient_id):
    cur.execute('SELECT ID FROM record_common2 where PATIENT_ID = %s;', table_patient_id)
    table_common_check = cur.fetchone()
    if table_common_check is None:
        try:
            logging.info('common表入库')
            cur.execute(
                'INSERT INTO record_common2(ID, BLLX, SUBJECT_ID, DISEASE_CODE, yljg_bh, yljg_mc, blh, nl, xingb, '
                'csrq, zy, zy_qt, sg, tz, whcd, shent, st_fz, st_zw, st_hm, st_ss, st_hh, st_dm, ms_qk, wn, sm, '
                'sm_rskn, sm_yx, sm_dm, sm_ss, sm_hz, sm_qt, xb, xb_cs, db, xy, xy_mtpjl, xy_sc, jy, jy_sc, '
                'gxy_bs, gxy_bc, gxb_jzs, gxb_bc, tnb_bs, tnb_bc, gzss_bs, gzss_bc, mxzsxfb_bs, fss_bs, wsgfl_bs, '
                'bxb_js, xxb_js, xhdb_hl, ssxlxb_js, hxb_js, sx, sx_ls, sx_ns, sx_pds, sx_sxs, sx_chs, ss_hs, '
                'ss_qzs, ts, tz_bot, tz_ht, tz_nt, tz_bat, tz_rt, tz_zt, sx_mlxt, mz_f, mz_che, mz_chi, mz_sh, '
                'mz_h, mz_se, mz_j, mz_xia, mz_xi, mz_d, mz_yl, mz_wl, tz_phz, tz_qxz, tz_yaxz, tz_yixz, tz_tsz, '
                'tz_srz, tz_xyz, tz_qyz, tz_tbz, tz_jlcp, tz_ypf, tz_sydrwl, tz_sblhli, tz_xmbl, tz_nsyhjbh, '
                'tz_rysm, tz_ryws, tz_rypf, tz_ryxh, tz_ryqd, tz_ryty, tz_qyhgm, tz_xhaj, tz_shdrwl, tz_ycxh, '
                'tz_sjfl, tz_wbypl, tz_pl, tz_sblhle, tz_yyhgm, tz_pclddx, tz_ryfx, tz_sjxfr, tz_stlsfr, tz_pfkcg, '
                'tz_kcysh, tz_rybm, tz_yjgs, tz_kgyz, tz_xm, tz_stcz, tz_fbfm, tz_etyzfmgd, tz_syjz, tz_zlnn, '
                'tz_td, tz_mbbbyn, tz_yszc, tz_kk, tz_ndfr, tz_dbjbj, tz_dxsh, tz_yncs, tz_pxcx, tz_lqxwhs, '
                'tz_sstt, tz_msha, tz_hyq, tz_jw, tz_kcyspa, tz_qxdc, tz_jsjz, tz_dcsg, tz_ysjx, tz_rfzt, tz_wgtq, '
                'tz_yhywg, tz_dpt, tz_lbt, tz_kc, tz_rygm, tz_ryxmz, tz_pfgmzd, tz_zh, USER_ID, ORG_ID, '
                'CREATE_TIME, UPDATE_TIME, STATUS, PATIENT_ID)'
                'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s);',
                (None, 0, 105, 'MZF' if common.get('DISEASE_CODE') == 3 else '', 125, '浙江省中医院',
                 common.get('blh') if common.get('blh') != '' else None,
                 common.get('nl') if common.get('nl') != '' else None,
                 common.get('xingb') if common.get('xingb') != '' else None,
                 common.get('csrq') if common.get('csrq') != '' else None,
                 common.get('zy') if common.get('zy') != '' else None,
                 common.get('zy_qt') if common.get('zy_qt') != '' else None,
                 common.get('sg') if common.get('sg') != '' else None,
                 common.get('tz') if common.get('tz') != '' else None,
                 common.get('whcd') if common.get('whcd') != '' else None,
                 common.get('st') if common.get('st') != '' else None,
                 common.get('st_fz') if common.get('st_fz') != '' else None,
                 common.get('st_zw') if common.get('st_zw') != '' else None,
                 common.get('st_hw') if common.get('st_hw') != '' else None,
                 common.get('st_ss') if common.get('st_ss') != '' else None,
                 common.get('st_hh') if common.get('st_hh') != '' else None,
                 common.get('st_dw') if common.get('st_dw') != '' else None,
                 common.get('ms_qk') if common.get('ms_qk') != '' else None,
                 common.get('wn') if common.get('wn') != '' else None,
                 common.get('sm') if common.get('sm') != '' else None,
                 common.get('sm_rskn') if common.get('sm_rskn') != '' else None,
                 common.get('sm_yx') if common.get('sm_yx') != '' else None,
                 common.get('sm_dm') if common.get('sm_dm') != '' else None,
                 common.get('sm_ss') if common.get('sm_ss') != '' else None,
                 common.get('sm_hz') if common.get('sm_hz') != '' else None,
                 common.get('sm_qt') if common.get('sm_qt') != '' else None,
                 common.get('xb') if common.get('xb') != '' else None,
                 common.get('xb_cs') if common.get('xb_cs') != '' else None,
                 common.get('db') if common.get('db') != '' else None,
                 common.get('xy') if common.get('xy') != '' else None,
                 common.get('xy_mtpjl') if common.get('xy_mtpjl') != '' else None,
                 common.get('xy_sc') if common.get('xy_sc') != '' else None,
                 common.get('jy') if common.get('jy') != '' else None,
                 common.get('jy_sc') if common.get('jy_sc') != '' else None,
                 common.get('gxy_bs') if common.get('gxy_bs') != '' else None,
                 common.get('gxy_bc') if common.get('gxy_bc') != '' else None,
                 common.get('gxb_jzs') if common.get('gxb_jzs') != '' else None,
                 common.get('gxb_bc') if common.get('gxb_bc') != '' else None,
                 common.get('tnb_bs') if common.get('tnb_bs') != '' else None,
                 common.get('tnb_bc') if common.get('tnb_bc') != '' else None,
                 common.get('gzss_bs') if common.get('gzss_bs') != '' else None,
                 common.get('gzss_bc') if common.get('gzss_bc') != '' else None,
                 common.get('mxzsxfb_bs') if common.get('mxzsxfb_bs') != '' else None,
                 common.get('fss_bs') if common.get('fss_bs') != '' else None,
                 common.get('wsgfl_bs') if common.get('wsgfl_bs') != '' else None,
                 common.get('bxb_js') if common.get('bxb_js') != '' else None,
                 common.get('xxb_js') if common.get('xxb_js') != '' else None,
                 common.get('xhdb_hl') if common.get('xhdb_hl') != '' else None,
                 common.get('ssxlxb_js') if common.get('ssxlxb_js') != '' else None,
                 common.get('hxb_js') if common.get('hxb_js') != '' else None,
                 common.get('sx') if common.get('sx') != '' else None,
                 common.get('sx_ls') if common.get('sx_ls') != '' else None,
                 common.get('sx_ns') if common.get('sx_ns') != '' else None,
                 common.get('sx_pds') if common.get('sx_pds') != '' else None,
                 common.get('sx_sxs') if common.get('sx_sxs') != '' else None,
                 common.get('sx_chs') if common.get('sx_chs') != '' else None,
                 common.get('ss_hs') if common.get('ss_hs') != '' else None,
                 common.get('ss_qzs') if common.get('ss_qzs') != '' else None,
                 common.get('ts') if common.get('ts') != '' else None,
                 common.get('tz_bot') if common.get('tz_bot') != '' else None,
                 common.get('tz_ht') if common.get('tz_ht') != '' else None,
                 common.get('tz_nt') if common.get('tz_nt') != '' else None,
                 common.get('tz_bat') if common.get('tz_bat') != '' else None,
                 common.get('tz_rt') if common.get('tz_rt') != '' else None,
                 common.get('tz_zt') if common.get('tz_zt') != '' else None,
                 common.get('sx_mlxt') if common.get('sx_mlxt') != '' else None,
                 common.get('mz_f') if common.get('mz_f') != '' else None,
                 common.get('mz_che') if common.get('mz_che') != '' else None,
                 common.get('mz_chi') if common.get('mz_chi') != '' else None,
                 common.get('mz_sh') if common.get('mz_sh') != '' else None,
                 common.get('mz_h') if common.get('mz_h') != '' else None,
                 common.get('mz_se') if common.get('mz_se') != '' else None,
                 common.get('mz_j') if common.get('mz_j') != '' else None,
                 common.get('mz_xia') if common.get('mz_xia') != '' else None,
                 common.get('mz_xi') if common.get('mz_xi') != '' else None,
                 common.get('mz_d') if common.get('mz_d') != '' else None,
                 common.get('mz_yl') if common.get('mz_yl') != '' else None,
                 common.get('mz_wl') if common.get('mz_wl') != '' else None,
                 common.get('tz_phz') if common.get('tz_phz') != '' else None,
                 common.get('tz_qxz') if common.get('tz_qxz') != '' else None,
                 common.get('tz_yaxz') if common.get('tz_yaxz') != '' else None,
                 common.get('tz_yixz') if common.get('tz_yixz') != '' else None,
                 common.get('tz_tsz') if common.get('tz_tsz') != '' else None,
                 common.get('tz_srz') if common.get('tz_srz') != '' else None,
                 common.get('tz_xyz') if common.get('tz_xyz') != '' else None,
                 common.get('tz_qyz') if common.get('tz_qyz') != '' else None,
                 common.get('tz_tbz') if common.get('tz_tbz') != '' else None,
                 common.get('tz_jlcp') if common.get('tz_jlcp') != '' else None,
                 common.get('tz_ypf') if common.get('tz_ypf') != '' else None,
                 common.get('tz_sydrwl') if common.get('tz_sydrwl') != '' else None,
                 common.get('tz_sblhli') if common.get('tz_sblhli') != '' else None,
                 common.get('tz_xmbl') if common.get('tz_xmbl') != '' else None,
                 common.get('tz_nsyhjbh') if common.get('tz_nsyhjbh') != '' else None,
                 common.get('tz_rysm') if common.get('tz_rysm') != '' else None,
                 common.get('tz_ryws') if common.get('tz_ryws') != '' else None,
                 common.get('tz_rypf') if common.get('tz_rypf') != '' else None,
                 common.get('tz_ryxh') if common.get('tz_ryxh') != '' else None,
                 common.get('tz_ryqd') if common.get('tz_ryqd') != '' else None,
                 common.get('tz_ryty') if common.get('tz_ryty') != '' else None,
                 common.get('tz_qyhgm') if common.get('tz_qyhgm') != '' else None,
                 common.get('tz_xhaj') if common.get('tz_xhaj') != '' else None,
                 common.get('tz_shdrwl') if common.get('tz_shdrwl') != '' else None,
                 common.get('tz_ycxh') if common.get('tz_ycxh') != '' else None,
                 common.get('tz_sjfl') if common.get('tz_sjfl') != '' else None,
                 common.get('tz_wbypl') if common.get('tz_wbypl') != '' else None,
                 common.get('tz_pl') if common.get('tz_pl') != '' else None,
                 common.get('tz_sblhle') if common.get('tz_sblhle') != '' else None,
                 common.get('tz_yyhgm') if common.get('tz_yyhgm') != '' else None,
                 common.get('tz_pclddx') if common.get('tz_pclddx') != '' else None,
                 common.get('tz_ryfx') if common.get('tz_ryfx') != '' else None,
                 common.get('tz_sjxfr') if common.get('tz_sjxfr') != '' else None,
                 common.get('tz_stlsfr') if common.get('tz_stlsfr') != '' else None,
                 common.get('tz_pfkcg') if common.get('tz_pfkcg') != '' else None,
                 common.get('tz_kcysh') if common.get('tz_kcysh') != '' else None,
                 common.get('tz_rybm') if common.get('tz_rybm') != '' else None,
                 common.get('tz_yjgs') if common.get('tz_yjgs') != '' else None,
                 common.get('tz_kgyz') if common.get('tz_kgyz') != '' else None,
                 common.get('tz_xm') if common.get('tz_xm') != '' else None,
                 common.get('tz_stcz') if common.get('tz_stcz') != '' else None,
                 common.get('tz_fbfm') if common.get('tz_fbfm') != '' else None,
                 common.get('tz_etyzfmgd') if common.get('tz_etyzfmgd') != '' else None,
                 common.get('tz_syjz') if common.get('tz_syjz') != '' else None,
                 common.get('tz_zlnn') if common.get('tz_zlnn') != '' else None,
                 common.get('tz_td') if common.get('tz_td') != '' else None,
                 common.get('tz_mbbbyn') if common.get('tz_mbbbyn') != '' else None,
                 common.get('tz_yszc') if common.get('tz_yszc') != '' else None,
                 common.get('tz_kk') if common.get('tz_kk') != '' else None,
                 common.get('tz_ndfr') if common.get('tz_ndfr') != '' else None,
                 common.get('tz_dbjbj') if common.get('tz_dbjbj') != '' else None,
                 common.get('tz_dxsh') if common.get('tz_dxsh') != '' else None,
                 common.get('tz_yncs') if common.get('tz_yncs') != '' else None,
                 common.get('tz_pxcx') if common.get('tz_pxcx') != '' else None,
                 common.get('tz_lqxwhs') if common.get('tz_lqxwhs') != '' else None,
                 common.get('tz_sstt') if common.get('tz_sstt') != '' else None,
                 common.get('tz_msha') if common.get('tz_msha') != '' else None,
                 common.get('tz_hyq') if common.get('tz_hyq') != '' else None,
                 common.get('tz_jw') if common.get('tz_jw') != '' else None,
                 common.get('tz_kcyspa') if common.get('tz_kcyspa') != '' else None,
                 common.get('tz_qxdc') if common.get('tz_qxdc') != '' else None,
                 common.get('tz_jsjz') if common.get('tz_jsjz') != '' else None,
                 common.get('tz_dcsg') if common.get('tz_dcsg') != '' else None,
                 common.get('tz_ysjx') if common.get('tz_ysjx') != '' else None,
                 common.get('tz_rfzt') if common.get('tz_rfzt') != '' else None,
                 common.get('tz_wgtq') if common.get('tz_wgtq') != '' else None,
                 common.get('tz_yhywg') if common.get('tz_yhywg') != '' else None,
                 common.get('tz_dpt') if common.get('tz_dpt') != '' else None,
                 common.get('tz_lbt') if common.get('tz_lbt') != '' else None,
                 common.get('tz_kc') if common.get('tz_kc') != '' else None,
                 common.get('tz_rygm') if common.get('tz_rygm') != '' else None,
                 common.get('tz_ryxmz') if common.get('tz_ryxmz') != '' else None,
                 common.get('tz_pfgmzd') if common.get('tz_pfgmzd') != '' else None,
                 common.get('tz_zh') if common.get('tz_zh') != '' else None,
                 'COPD_grd', 125,
                 common.get('CREATE_TIME') if common.get('blh') != '' else time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                 time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 0, table_patient_id))
            table_common_id = conn.insert_id()
            conn.commit()
            return table_common_id
        except Exception as ex:
            logging.error('[插入common异常]' + str(ex))
            conn.rollback()
            return None
    else:
        table_common_id = table_common_check[0]
        try:
            logging.info('common表刷新')
            cur.execute(
                'UPDATE record_common2 SET DISEASE_CODE = %s, blh = %s, nl = %s, xingb = %s, '
                'csrq = %s, zy = %s, zy_qt = %s, sg = %s, tz = %s, whcd = %s, shent = %s, st_fz = %s, st_zw = %s, '
                'st_hm = %s, st_ss = %s, st_hh = %s, st_dm = %s, ms_qk = %s, wn = %s, sm = %s, sm_rskn = %s, '
                'sm_yx = %s, sm_dm = %s, sm_ss = %s, sm_hz = %s, sm_qt = %s, xb = %s, xb_cs = %s, db = %s, '
                'xy = %s, xy_mtpjl = %s, xy_sc = %s, jy = %s, jy_sc = %s, gxy_bs = %s, gxy_bc = %s, gxb_jzs = %s, '
                'gxb_bc = %s, tnb_bs = %s, tnb_bc = %s, gzss_bs = %s, gzss_bc = %s, mxzsxfb_bs = %s, fss_bs = %s, '
                'wsgfl_bs = %s, bxb_js = %s, xxb_js = %s, xhdb_hl = %s, ssxlxb_js = %s, hxb_js = %s, sx = %s, '
                'sx_ls = %s, sx_ns = %s, sx_pds = %s, sx_sxs = %s, sx_chs = %s, ss_hs = %s, ss_qzs = %s, ts = %s, '
                'tz_bot = %s, tz_ht = %s, tz_nt = %s, tz_bat = %s, tz_rt = %s, tz_zt = %s, sx_mlxt = %s, '
                'mz_f = %s, mz_che = %s, mz_chi = %s, mz_sh = %s, mz_h = %s, mz_se = %s, mz_j = %s, mz_xia = %s, '
                'mz_xi = %s, mz_d = %s, mz_yl = %s, mz_wl = %s, tz_phz = %s, tz_qxz = %s, tz_yaxz = %s, '
                'tz_yixz = %s, tz_tsz = %s, tz_srz = %s, tz_xyz = %s, tz_qyz = %s, tz_tbz = %s, tz_jlcp = %s, '
                'tz_ypf = %s, tz_sydrwl = %s, tz_sblhli = %s, tz_xmbl = %s, tz_nsyhjbh = %s, tz_rysm = %s, '
                'tz_ryws = %s, tz_rypf = %s, tz_ryxh = %s, tz_ryqd = %s, tz_ryty = %s, tz_qyhgm = %s, '
                'tz_xhaj = %s, tz_shdrwl = %s, tz_ycxh = %s, tz_sjfl = %s, tz_wbypl = %s, tz_pl = %s, '
                'tz_sblhle = %s, tz_yyhgm = %s, tz_pclddx = %s, tz_ryfx = %s, tz_sjxfr = %s, tz_stlsfr = %s, '
                'tz_pfkcg = %s, tz_kcysh = %s, tz_rybm = %s, tz_yjgs = %s, tz_kgyz = %s, tz_xm = %s, tz_stcz = %s, '
                'tz_fbfm = %s, tz_etyzfmgd = %s, tz_syjz = %s, tz_zlnn = %s, tz_td = %s, tz_mbbbyn = %s, '
                'tz_yszc = %s, tz_kk = %s, tz_ndfr = %s, tz_dbjbj = %s, tz_dxsh = %s, tz_yncs = %s, tz_pxcx = %s, '
                'tz_lqxwhs = %s, tz_sstt = %s, tz_msha = %s, tz_hyq = %s, tz_jw = %s, tz_kcyspa = %s, '
                'tz_qxdc = %s, tz_jsjz = %s, tz_dcsg = %s, tz_ysjx = %s, tz_rfzt = %s, tz_wgtq = %s, '
                'tz_yhywg = %s, tz_dpt = %s, tz_lbt = %s, tz_kc = %s, tz_rygm = %s, tz_ryxmz = %s, tz_pfgmzd = %s, '
                'tz_zh = %s, UPDATE_TIME = %s WHERE ID = %s;',
                (
                    'MZF' if common.get('DISEASE_CODE') == 3 else '',
                    common.get('blh') if common.get('blh') != '' else None,
                    common.get('nl') if common.get('nl') != '' else None,
                    common.get('xingb') if common.get('xingb') != '' else None,
                    common.get('csrq') if common.get('csrq') != '' else None,
                    common.get('zy') if common.get('zy') != '' else None,
                    common.get('zy_qt') if common.get('zy_qt') != '' else None,
                    common.get('sg') if common.get('sg') != '' else None,
                    common.get('tz') if common.get('tz') != '' else None,
                    common.get('whcd') if common.get('whcd') != '' else None,
                    common.get('st') if common.get('st') != '' else None,
                    common.get('st_fz') if common.get('st_fz') != '' else None,
                    common.get('st_zw') if common.get('st_zw') != '' else None,
                    common.get('st_hw') if common.get('st_hw') != '' else None,
                    common.get('st_ss') if common.get('st_ss') != '' else None,
                    common.get('st_hh') if common.get('st_hh') != '' else None,
                    common.get('st_dw') if common.get('st_dw') != '' else None,
                    common.get('ms_qk') if common.get('ms_qk') != '' else None,
                    common.get('wn') if common.get('wn') != '' else None,
                    common.get('sm') if common.get('sm') != '' else None,
                    common.get('sm_rskn') if common.get('sm_rskn') != '' else None,
                    common.get('sm_yx') if common.get('sm_yx') != '' else None,
                    common.get('sm_dm') if common.get('sm_dm') != '' else None,
                    common.get('sm_ss') if common.get('sm_ss') != '' else None,
                    common.get('sm_hz') if common.get('sm_hz') != '' else None,
                    common.get('sm_qt') if common.get('sm_qt') != '' else None,
                    common.get('xb') if common.get('xb') != '' else None,
                    common.get('xb_cs') if common.get('xb_cs') != '' else None,
                    common.get('db') if common.get('db') != '' else None,
                    common.get('xy') if common.get('xy') != '' else None,
                    common.get('xy_mtpjl') if common.get('xy_mtpjl') != '' else None,
                    common.get('xy_sc') if common.get('xy_sc') != '' else None,
                    common.get('jy') if common.get('jy') != '' else None,
                    common.get('jy_sc') if common.get('jy_sc') != '' else None,
                    common.get('gxy_bs') if common.get('gxy_bs') != '' else None,
                    common.get('gxy_bc') if common.get('gxy_bc') != '' else None,
                    common.get('gxb_jzs') if common.get('gxb_jzs') != '' else None,
                    common.get('gxb_bc') if common.get('gxb_bc') != '' else None,
                    common.get('tnb_bs') if common.get('tnb_bs') != '' else None,
                    common.get('tnb_bc') if common.get('tnb_bc') != '' else None,
                    common.get('gzss_bs') if common.get('gzss_bs') != '' else None,
                    common.get('gzss_bc') if common.get('gzss_bc') != '' else None,
                    common.get('mxzsxfb_bs') if common.get('mxzsxfb_bs') != '' else None,
                    common.get('fss_bs') if common.get('fss_bs') != '' else None,
                    common.get('wsgfl_bs') if common.get('wsgfl_bs') != '' else None,
                    common.get('bxb_js') if common.get('bxb_js') != '' else None,
                    common.get('xxb_js') if common.get('xxb_js') != '' else None,
                    common.get('xhdb_hl') if common.get('xhdb_hl') != '' else None,
                    common.get('ssxlxb_js') if common.get('ssxlxb_js') != '' else None,
                    common.get('hxb_js') if common.get('hxb_js') != '' else None,
                    common.get('sx') if common.get('sx') != '' else None,
                    common.get('sx_ls') if common.get('sx_ls') != '' else None,
                    common.get('sx_ns') if common.get('sx_ns') != '' else None,
                    common.get('sx_pds') if common.get('sx_pds') != '' else None,
                    common.get('sx_sxs') if common.get('sx_sxs') != '' else None,
                    common.get('sx_chs') if common.get('sx_chs') != '' else None,
                    common.get('ss_hs') if common.get('ss_hs') != '' else None,
                    common.get('ss_qzs') if common.get('ss_qzs') != '' else None,
                    common.get('ts') if common.get('ts') != '' else None,
                    common.get('tz_bot') if common.get('tz_bot') != '' else None,
                    common.get('tz_ht') if common.get('tz_ht') != '' else None,
                    common.get('tz_nt') if common.get('tz_nt') != '' else None,
                    common.get('tz_bat') if common.get('tz_bat') != '' else None,
                    common.get('tz_rt') if common.get('tz_rt') != '' else None,
                    common.get('tz_zt') if common.get('tz_zt') != '' else None,
                    common.get('sx_mlxt') if common.get('sx_mlxt') != '' else None,
                    common.get('mz_f') if common.get('mz_f') != '' else None,
                    common.get('mz_che') if common.get('mz_che') != '' else None,
                    common.get('mz_chi') if common.get('mz_chi') != '' else None,
                    common.get('mz_sh') if common.get('mz_sh') != '' else None,
                    common.get('mz_h') if common.get('mz_h') != '' else None,
                    common.get('mz_se') if common.get('mz_se') != '' else None,
                    common.get('mz_j') if common.get('mz_j') != '' else None,
                    common.get('mz_xia') if common.get('mz_xia') != '' else None,
                    common.get('mz_xi') if common.get('mz_xi') != '' else None,
                    common.get('mz_d') if common.get('mz_d') != '' else None,
                    common.get('mz_yl') if common.get('mz_yl') != '' else None,
                    common.get('mz_wl') if common.get('mz_wl') != '' else None,
                    common.get('tz_phz') if common.get('tz_phz') != '' else None,
                    common.get('tz_qxz') if common.get('tz_qxz') != '' else None,
                    common.get('tz_yaxz') if common.get('tz_yaxz') != '' else None,
                    common.get('tz_yixz') if common.get('tz_yixz') != '' else None,
                    common.get('tz_tsz') if common.get('tz_tsz') != '' else None,
                    common.get('tz_srz') if common.get('tz_srz') != '' else None,
                    common.get('tz_xyz') if common.get('tz_xyz') != '' else None,
                    common.get('tz_qyz') if common.get('tz_qyz') != '' else None,
                    common.get('tz_tbz') if common.get('tz_tbz') != '' else None,
                    common.get('tz_jlcp') if common.get('tz_jlcp') != '' else None,
                    common.get('tz_ypf') if common.get('tz_ypf') != '' else None,
                    common.get('tz_sydrwl') if common.get('tz_sydrwl') != '' else None,
                    common.get('tz_sblhli') if common.get('tz_sblhli') != '' else None,
                    common.get('tz_xmbl') if common.get('tz_xmbl') != '' else None,
                    common.get('tz_nsyhjbh') if common.get('tz_nsyhjbh') != '' else None,
                    common.get('tz_rysm') if common.get('tz_rysm') != '' else None,
                    common.get('tz_ryws') if common.get('tz_ryws') != '' else None,
                    common.get('tz_rypf') if common.get('tz_rypf') != '' else None,
                    common.get('tz_ryxh') if common.get('tz_ryxh') != '' else None,
                    common.get('tz_ryqd') if common.get('tz_ryqd') != '' else None,
                    common.get('tz_ryty') if common.get('tz_ryty') != '' else None,
                    common.get('tz_qyhgm') if common.get('tz_qyhgm') != '' else None,
                    common.get('tz_xhaj') if common.get('tz_xhaj') != '' else None,
                    common.get('tz_shdrwl') if common.get('tz_shdrwl') != '' else None,
                    common.get('tz_ycxh') if common.get('tz_ycxh') != '' else None,
                    common.get('tz_sjfl') if common.get('tz_sjfl') != '' else None,
                    common.get('tz_wbypl') if common.get('tz_wbypl') != '' else None,
                    common.get('tz_pl') if common.get('tz_pl') != '' else None,
                    common.get('tz_sblhle') if common.get('tz_sblhle') != '' else None,
                    common.get('tz_yyhgm') if common.get('tz_yyhgm') != '' else None,
                    common.get('tz_pclddx') if common.get('tz_pclddx') != '' else None,
                    common.get('tz_ryfx') if common.get('tz_ryfx') != '' else None,
                    common.get('tz_sjxfr') if common.get('tz_sjxfr') != '' else None,
                    common.get('tz_stlsfr') if common.get('tz_stlsfr') != '' else None,
                    common.get('tz_pfkcg') if common.get('tz_pfkcg') != '' else None,
                    common.get('tz_kcysh') if common.get('tz_kcysh') != '' else None,
                    common.get('tz_rybm') if common.get('tz_rybm') != '' else None,
                    common.get('tz_yjgs') if common.get('tz_yjgs') != '' else None,
                    common.get('tz_kgyz') if common.get('tz_kgyz') != '' else None,
                    common.get('tz_xm') if common.get('tz_xm') != '' else None,
                    common.get('tz_stcz') if common.get('tz_stcz') != '' else None,
                    common.get('tz_fbfm') if common.get('tz_fbfm') != '' else None,
                    common.get('tz_etyzfmgd') if common.get('tz_etyzfmgd') != '' else None,
                    common.get('tz_syjz') if common.get('tz_syjz') != '' else None,
                    common.get('tz_zlnn') if common.get('tz_zlnn') != '' else None,
                    common.get('tz_td') if common.get('tz_td') != '' else None,
                    common.get('tz_mbbbyn') if common.get('tz_mbbbyn') != '' else None,
                    common.get('tz_yszc') if common.get('tz_yszc') != '' else None,
                    common.get('tz_kk') if common.get('tz_kk') != '' else None,
                    common.get('tz_ndfr') if common.get('tz_ndfr') != '' else None,
                    common.get('tz_dbjbj') if common.get('tz_dbjbj') != '' else None,
                    common.get('tz_dxsh') if common.get('tz_dxsh') != '' else None,
                    common.get('tz_yncs') if common.get('tz_yncs') != '' else None,
                    common.get('tz_pxcx') if common.get('tz_pxcx') != '' else None,
                    common.get('tz_lqxwhs') if common.get('tz_lqxwhs') != '' else None,
                    common.get('tz_sstt') if common.get('tz_sstt') != '' else None,
                    common.get('tz_msha') if common.get('tz_msha') != '' else None,
                    common.get('tz_hyq') if common.get('tz_hyq') != '' else None,
                    common.get('tz_jw') if common.get('tz_jw') != '' else None,
                    common.get('tz_kcyspa') if common.get('tz_kcyspa') != '' else None,
                    common.get('tz_qxdc') if common.get('tz_qxdc') != '' else None,
                    common.get('tz_jsjz') if common.get('tz_jsjz') != '' else None,
                    common.get('tz_dcsg') if common.get('tz_dcsg') != '' else None,
                    common.get('tz_ysjx') if common.get('tz_ysjx') != '' else None,
                    common.get('tz_rfzt') if common.get('tz_rfzt') != '' else None,
                    common.get('tz_wgtq') if common.get('tz_wgtq') != '' else None,
                    common.get('tz_yhywg') if common.get('tz_yhywg') != '' else None,
                    common.get('tz_dpt') if common.get('tz_dpt') != '' else None,
                    common.get('tz_lbt') if common.get('tz_lbt') != '' else None,
                    common.get('tz_kc') if common.get('tz_kc') != '' else None,
                    common.get('tz_rygm') if common.get('tz_rygm') != '' else None,
                    common.get('tz_ryxmz') if common.get('tz_ryxmz') != '' else None,
                    common.get('tz_pfgmzd') if common.get('tz_pfgmzd') != '' else None,
                    common.get('tz_zh') if common.get('tz_zh') != '' else None,
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), table_common_id))
            conn.commit()
            return table_common_id
        except Exception as ex:
            logging.error('[更新common异常]' + str(ex))
            conn.rollback()
            return None


def mzf_to_db(cur, common, table_common_id):
    cur.execute('SELECT ID FROM record_mzf2 where COMMON_ID = %s;', table_common_id)
    table_mzf_check = cur.fetchone()
    if table_mzf_check is None:
        try:
            logging.info('mzf表入库')
            cur.execute(
                'INSERT INTO record_mzf2(ID, COMMON_ID, gwys_xy, gwys_hymxtdjb, gwys_jzzkqwryzdq, gwys_csjcfcgz, '
                'gwys_ffhxhxdgr, copdzd, ywzl, ywzl_dyxrzqgkzjsaba, ywzl_dyxrzqgkzjsana, ywzl_dyxrzqgkzjlaba, '
                'ywzl_dyxrzqgkzjlama, ywzl_dyxrzqgkzjsfgl, ywzl_xrjssfgl, ywzl_lhxrzjicslaba, ywzl_lhxrzjicslabalama, '
                'ywzl_lhxrzjlabalama, ywzl_lhxrzjsfgl, ywzl_kfjssfgl, ywzl_kfcjlsfgl, ywzl_htlywsfgl, fywzl, fywzl_jy, '
                'fywzl_ywjzym, fywzl_cjzlgym, fywzl_cjzfyym, fywzl_ywfkfzl, fywzl_ywjtyl, fywzl_jtylsc, fywzl_ywwchxj, '
                'fywzl_wchxjsc, fywzl_dxzqgkzj, fywzl_dxzqgkzjcs, fywzl_jzjz, fywzl_jzjzcs, fywzl_zyzl, fywzl_zyzlcs, '
                'hxxtjb, hxxtjb_zqgkz, hxxtjb_zqgxc, hxxtjb_gmxby, hxxtjb_fbdy, hxxtjb_fss, hxxtjb_fjzxwh, hxxtjb_fa, '
                'hxxtjb_smhxztzhz, hxxtjb_hxsj, hxxtjb_fjh, hxxtjb_fdmgy, hxxtjb_sc, xxgjb, xxgjb_gxy, xxgjb_gxb, '
                'xxgjb_jxxgnbq, xxgjb_fyxxzb, xxgjb_mxxgnbq, xxgjb_fc, xxgjb_yszcdzz, xxgjb_sxzb, xxgjb_sc, nfmxtjb, '
                'nfmxtjb_tnb, nfmxtjb_gzss, nfmxtjb_sc, xhxtjb, xhxtjb_wsgfl, xhxtjb_bm, xhxtjb_yzxcb, xhxtjb_gyh, '
                'xhxtjb_sc, qtxtjb, qtxtjb_yyz, qtxtjb_qtbwzl, qtxtjb_sgnbq, qtxtjb_gljrza, qtxtjb_xkjx, '
                'qtxtjb_swywygms, qtxtjb_xbss, xypc, yl, xycsnl, xyjsnl, pjmtxys, xysc, jynl, jysc, esyjc, esyjc_e14, '
                'esyjc_a14, esyjcsj, swrlzf, mnsnqnyw, snzfqncxsj, snzfqnsyqj, zfqnfhcs, zfqnfhcs_kz, zfqnfhcs_fdmj, '
                'zfqnfhcs_yyj, zfqnfhcs_pqs, zfqnfhpc, qtfszf, qtfszf_trq, qtfszf_mb, qtfszf_yhq, mnsnzfyw, snzfcxsj, '
                'snzfsyqj, snzffhcs, snzffhcs_kz, snzffhcs_fdmj, snzffhcs_yyj, snzffhcs_pqs, snzffhcsqt, snzffhpc, '
                'fcjc, mnfcjcys, fcjccxsj, fcjcfhcs, fcjcfhcs_kz, fcjcfhcs_fdmj, fcjcfhcs_fhf, fcjcfhcs_fctk, '
                'fcjcfhcs_qxz, fcjcfhpc, ydqtjc, mnydqtjcys, ydqtjccxsj, ydqtjcfhcs, ydqtjcfhcs_kz, ydqtjcfhcs_fdmj, '
                'ydqtjcfhcs_fhf, ydqtjcfhcs_fctk, ydqtjcfhcs_qxz, ydqtjcfhpc, zjskljc, mnzjskljcys, zjskljccxsj, '
                'zjskljcfhcs, zjskljcfhcs_kz, zjskljcfhcs_fdmj, zjskljcfhcs_fhf, zjskljcfhcs_fctk, zjskljcfhcs_qxz, '
                'zjskljcfhpc, ydkljc, mnydkljcys, ydkljccxsj, ydkljcfhcs, ydkljcfhcs_kz, ydkljcfhcs_fdmz, '
                'ydkljcfhcs_fhf, ydkljcfhcs_fctk, ydkljcfhcs_qxz, ydkljcfhpc, cakljc, mncakljcys, cakljccxsj, '
                'cakljcfhcs, cakljcfhcs_kz, cakljcfhcs_fdmz, cakljcfhcs_fhf, cakljcfhcs_fctk, cakljcfhcs_qxz, '
                'cakljcfhpc, ynffmxqdjb, hxdjbjzs, ynnhxdgr, ynnhxdgrcs, zg, psyg, ynpjgmcs, bbzz_ht, bbzz_qt, '
                'bbzz_bs, bbzz_hxbm, ks, kscd, kssx, ksjzys, ksjzys_gm, ksjzys_tyhxxtyw, ksjzys_ll, ksjzys_lkq, '
                'ksjzys_yd, ksjzys_ys, ksjzys_jh, ksjzys_gmy, ksjzys_jl, ksjzys_hjys, kslxx, ksjjx, ksjjx_c, ksjjx_x, '
                'ksjjx_q, ksjjx_d, ksjjx_jjzh, ksjccxsj, ksfffzsj, kt, tzz, tl, ts_h, ts_b, ts_hbxj, ts_tzdx, '
                'ktjccxsj, ktfffzsj, cxzz, cxywfzpl, cxfzpl, cxyfys_gm, cxyfys_ll, cxyfys_lkq, cxyfys_tw, cxyfys_ys, '
                'cxyfys_jh, cxyfys_gmy, cxyfys_jl, cxyfys_hjys, cxjjx, cxfffzsj, fl, zhdhlx, yxsr, em, xm, qd, hr, xp, '
                'xp_lfwlzc, xp_ljxzk, fbct, fbct_mxzqgy, fbct_fqz, fbct_fdp, szqfev1fvc, szqfev1yjz, szqmmef, '
                'szhfev1fvc, szhfev1yjz, szhmmef, hqflspef, mxzsxfjb, qlsxjb, qlsxzb, kss, xrzqgkzyw, kfzqgkzyw, xrjs, '
                'qsjs, qty, mlst, zyzl, hxknpf, ydnl6mwt, hxjbpf, kcnyht, shzlpf, sx_pd, sx_sx, sx_jz, sx_wx, sx_ch, '
                'ss_d, ss_h, ss_z, ss_a, ss_ybyd, sst_lh, sst_jy, sst_l, sst_lan, ts_bai, ts_hu, ts_he, tz_bo, tz_h, '
                'tz_n, tz_ba, stgzcd, stl, stwz, sxml, CREATE_TIME, UPDATE_TIME, STATUS)'
                'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',
                (None, 0, 105, 'MZF' if common.get('DISEASE_CODE') == 3 else '', 125, '浙江省中医院',
                 common.get('blh') if common.get('blh') != '' else None,
                 common.get('nl') if common.get('nl') != '' else None,
                 common.get('xingb') if common.get('xingb') != '' else None,
                 common.get('csrq') if common.get('csrq') != '' else None,
                 common.get('zy') if common.get('zy') != '' else None,
                 common.get('zy_qt') if common.get('zy_qt') != '' else None,
                 common.get('sg') if common.get('sg') != '' else None,
                 common.get('tz') if common.get('tz') != '' else None,
                 common.get('whcd') if common.get('whcd') != '' else None,
                 common.get('st') if common.get('st') != '' else None,
                 common.get('st_fz') if common.get('st_fz') != '' else None,
                 common.get('st_zw') if common.get('st_zw') != '' else None,
                 common.get('st_hw') if common.get('st_hw') != '' else None,
                 common.get('st_ss') if common.get('st_ss') != '' else None,
                 common.get('st_hh') if common.get('st_hh') != '' else None,
                 common.get('st_dw') if common.get('st_dw') != '' else None,
                 common.get('ms_qk') if common.get('ms_qk') != '' else None,
                 common.get('wn') if common.get('wn') != '' else None,
                 common.get('sm') if common.get('sm') != '' else None,
                 common.get('sm_rskn') if common.get('sm_rskn') != '' else None,
                 common.get('sm_yx') if common.get('sm_yx') != '' else None,
                 common.get('sm_dm') if common.get('sm_dm') != '' else None,
                 common.get('sm_ss') if common.get('sm_ss') != '' else None,
                 common.get('sm_hz') if common.get('sm_hz') != '' else None,
                 common.get('sm_qt') if common.get('sm_qt') != '' else None,
                 common.get('xb') if common.get('xb') != '' else None,
                 common.get('xb_cs') if common.get('xb_cs') != '' else None,
                 common.get('db') if common.get('db') != '' else None,
                 common.get('xy') if common.get('xy') != '' else None,
                 common.get('xy_mtpjl') if common.get('xy_mtpjl') != '' else None,
                 common.get('xy_sc') if common.get('xy_sc') != '' else None,
                 common.get('jy') if common.get('jy') != '' else None,
                 common.get('jy_sc') if common.get('jy_sc') != '' else None,
                 common.get('gxy_bs') if common.get('gxy_bs') != '' else None,
                 common.get('gxy_bc') if common.get('gxy_bc') != '' else None,
                 common.get('gxb_jzs') if common.get('gxb_jzs') != '' else None,
                 common.get('gxb_bc') if common.get('gxb_bc') != '' else None,
                 common.get('tnb_bs') if common.get('tnb_bs') != '' else None,
                 common.get('tnb_bc') if common.get('tnb_bc') != '' else None,
                 common.get('gzss_bs') if common.get('gzss_bs') != '' else None,
                 common.get('gzss_bc') if common.get('gzss_bc') != '' else None,
                 common.get('mxzsxfb_bs') if common.get('mxzsxfb_bs') != '' else None,
                 common.get('fss_bs') if common.get('fss_bs') != '' else None,
                 common.get('wsgfl_bs') if common.get('wsgfl_bs') != '' else None,
                 common.get('bxb_js') if common.get('bxb_js') != '' else None,
                 common.get('xxb_js') if common.get('xxb_js') != '' else None,
                 common.get('xhdb_hl') if common.get('xhdb_hl') != '' else None,
                 common.get('ssxlxb_js') if common.get('ssxlxb_js') != '' else None,
                 common.get('hxb_js') if common.get('hxb_js') != '' else None,
                 common.get('sx') if common.get('sx') != '' else None,
                 common.get('sx_ls') if common.get('sx_ls') != '' else None,
                 common.get('sx_ns') if common.get('sx_ns') != '' else None,
                 common.get('sx_pds') if common.get('sx_pds') != '' else None,
                 common.get('sx_sxs') if common.get('sx_sxs') != '' else None,
                 common.get('sx_chs') if common.get('sx_chs') != '' else None,
                 common.get('ss_hs') if common.get('ss_hs') != '' else None,
                 common.get('ss_qzs') if common.get('ss_qzs') != '' else None,
                 common.get('ts') if common.get('ts') != '' else None,
                 common.get('tz_bot') if common.get('tz_bot') != '' else None,
                 common.get('tz_ht') if common.get('tz_ht') != '' else None,
                 common.get('tz_nt') if common.get('tz_nt') != '' else None,
                 common.get('tz_bat') if common.get('tz_bat') != '' else None,
                 common.get('tz_rt') if common.get('tz_rt') != '' else None,
                 common.get('tz_zt') if common.get('tz_zt') != '' else None,
                 common.get('sx_mlxt') if common.get('sx_mlxt') != '' else None,
                 common.get('mz_f') if common.get('mz_f') != '' else None,
                 common.get('mz_che') if common.get('mz_che') != '' else None,
                 common.get('mz_chi') if common.get('mz_chi') != '' else None,
                 common.get('mz_sh') if common.get('mz_sh') != '' else None,
                 common.get('mz_h') if common.get('mz_h') != '' else None,
                 common.get('mz_se') if common.get('mz_se') != '' else None,
                 common.get('mz_j') if common.get('mz_j') != '' else None,
                 common.get('mz_xia') if common.get('mz_xia') != '' else None,
                 common.get('mz_xi') if common.get('mz_xi') != '' else None,
                 common.get('mz_d') if common.get('mz_d') != '' else None,
                 common.get('mz_yl') if common.get('mz_yl') != '' else None,
                 common.get('mz_wl') if common.get('mz_wl') != '' else None,
                 common.get('tz_phz') if common.get('tz_phz') != '' else None,
                 common.get('tz_qxz') if common.get('tz_qxz') != '' else None,
                 common.get('tz_yaxz') if common.get('tz_yaxz') != '' else None,
                 common.get('tz_yixz') if common.get('tz_yixz') != '' else None,
                 common.get('tz_tsz') if common.get('tz_tsz') != '' else None,
                 common.get('tz_srz') if common.get('tz_srz') != '' else None,
                 common.get('tz_xyz') if common.get('tz_xyz') != '' else None,
                 common.get('tz_qyz') if common.get('tz_qyz') != '' else None,
                 common.get('tz_tbz') if common.get('tz_tbz') != '' else None,
                 common.get('tz_jlcp') if common.get('tz_jlcp') != '' else None,
                 common.get('tz_ypf') if common.get('tz_ypf') != '' else None,
                 common.get('tz_sydrwl') if common.get('tz_sydrwl') != '' else None,
                 common.get('tz_sblhli') if common.get('tz_sblhli') != '' else None,
                 common.get('tz_xmbl') if common.get('tz_xmbl') != '' else None,
                 common.get('tz_nsyhjbh') if common.get('tz_nsyhjbh') != '' else None,
                 common.get('tz_rysm') if common.get('tz_rysm') != '' else None,
                 common.get('tz_ryws') if common.get('tz_ryws') != '' else None,
                 common.get('tz_rypf') if common.get('tz_rypf') != '' else None,
                 common.get('tz_ryxh') if common.get('tz_ryxh') != '' else None,
                 common.get('tz_ryqd') if common.get('tz_ryqd') != '' else None,
                 common.get('tz_ryty') if common.get('tz_ryty') != '' else None,
                 common.get('tz_qyhgm') if common.get('tz_qyhgm') != '' else None,
                 common.get('tz_xhaj') if common.get('tz_xhaj') != '' else None,
                 common.get('tz_shdrwl') if common.get('tz_shdrwl') != '' else None,
                 common.get('tz_ycxh') if common.get('tz_ycxh') != '' else None,
                 common.get('tz_sjfl') if common.get('tz_sjfl') != '' else None,
                 common.get('tz_wbypl') if common.get('tz_wbypl') != '' else None,
                 common.get('tz_pl') if common.get('tz_pl') != '' else None,
                 common.get('tz_sblhle') if common.get('tz_sblhle') != '' else None,
                 common.get('tz_yyhgm') if common.get('tz_yyhgm') != '' else None,
                 common.get('tz_pclddx') if common.get('tz_pclddx') != '' else None,
                 common.get('tz_ryfx') if common.get('tz_ryfx') != '' else None,
                 common.get('tz_sjxfr') if common.get('tz_sjxfr') != '' else None,
                 common.get('tz_stlsfr') if common.get('tz_stlsfr') != '' else None,
                 common.get('tz_pfkcg') if common.get('tz_pfkcg') != '' else None,
                 common.get('tz_kcysh') if common.get('tz_kcysh') != '' else None,
                 common.get('tz_rybm') if common.get('tz_rybm') != '' else None,
                 common.get('tz_yjgs') if common.get('tz_yjgs') != '' else None,
                 common.get('tz_kgyz') if common.get('tz_kgyz') != '' else None,
                 common.get('tz_xm') if common.get('tz_xm') != '' else None,
                 common.get('tz_stcz') if common.get('tz_stcz') != '' else None,
                 common.get('tz_fbfm') if common.get('tz_fbfm') != '' else None,
                 common.get('tz_etyzfmgd') if common.get('tz_etyzfmgd') != '' else None,
                 common.get('tz_syjz') if common.get('tz_syjz') != '' else None,
                 common.get('tz_zlnn') if common.get('tz_zlnn') != '' else None,
                 common.get('tz_td') if common.get('tz_td') != '' else None,
                 common.get('tz_mbbbyn') if common.get('tz_mbbbyn') != '' else None,
                 common.get('tz_yszc') if common.get('tz_yszc') != '' else None,
                 common.get('tz_kk') if common.get('tz_kk') != '' else None,
                 common.get('tz_ndfr') if common.get('tz_ndfr') != '' else None,
                 common.get('tz_dbjbj') if common.get('tz_dbjbj') != '' else None,
                 common.get('tz_dxsh') if common.get('tz_dxsh') != '' else None,
                 common.get('tz_yncs') if common.get('tz_yncs') != '' else None,
                 common.get('tz_pxcx') if common.get('tz_pxcx') != '' else None,
                 common.get('tz_lqxwhs') if common.get('tz_lqxwhs') != '' else None,
                 common.get('tz_sstt') if common.get('tz_sstt') != '' else None,
                 common.get('tz_msha') if common.get('tz_msha') != '' else None,
                 common.get('tz_hyq') if common.get('tz_hyq') != '' else None,
                 common.get('tz_jw') if common.get('tz_jw') != '' else None,
                 common.get('tz_kcyspa') if common.get('tz_kcyspa') != '' else None,
                 common.get('tz_qxdc') if common.get('tz_qxdc') != '' else None,
                 common.get('tz_jsjz') if common.get('tz_jsjz') != '' else None,
                 common.get('tz_dcsg') if common.get('tz_dcsg') != '' else None,
                 common.get('tz_ysjx') if common.get('tz_ysjx') != '' else None,
                 common.get('tz_rfzt') if common.get('tz_rfzt') != '' else None,
                 common.get('tz_wgtq') if common.get('tz_wgtq') != '' else None,
                 common.get('tz_yhywg') if common.get('tz_yhywg') != '' else None,
                 common.get('tz_dpt') if common.get('tz_dpt') != '' else None,
                 common.get('tz_lbt') if common.get('tz_lbt') != '' else None,
                 common.get('tz_kc') if common.get('tz_kc') != '' else None,
                 common.get('tz_rygm') if common.get('tz_rygm') != '' else None,
                 common.get('tz_ryxmz') if common.get('tz_ryxmz') != '' else None,
                 common.get('tz_pfgmzd') if common.get('tz_pfgmzd') != '' else None,
                 common.get('tz_zh') if common.get('tz_zh') != '' else None,
                 'COPD_grd', 125,
                 common.get('CREATE_TIME') if common.get('blh') != '' else time.strftime("%Y-%m-%d %H:%M:%S",
                                                                                         time.localtime()),
                 time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 0, table_patient_id))
            table_common_id = conn.insert_id()
            conn.commit()
            return table_common_id
        except Exception as ex:
            logging.error('[插入common异常]' + str(ex))
            conn.rollback()
            return None
    else:
        table_mzf_id = table_mzf_check[0]
        # try:
        #     logging.info('common表刷新')
        #     cur.execute(
        #         'UPDATE record_common2 SET DISEASE_CODE = %s, blh = %s, nl = %s, xingb = %s, '
        #         'csrq = %s, zy = %s, zy_qt = %s, sg = %s, tz = %s, whcd = %s, shent = %s, st_fz = %s, st_zw = %s, '
        #         'st_hm = %s, st_ss = %s, st_hh = %s, st_dm = %s, ms_qk = %s, wn = %s, sm = %s, sm_rskn = %s, '
        #         'sm_yx = %s, sm_dm = %s, sm_ss = %s, sm_hz = %s, sm_qt = %s, xb = %s, xb_cs = %s, db = %s, '
        #         'xy = %s, xy_mtpjl = %s, xy_sc = %s, jy = %s, jy_sc = %s, gxy_bs = %s, gxy_bc = %s, gxb_jzs = %s, '
        #         'gxb_bc = %s, tnb_bs = %s, tnb_bc = %s, gzss_bs = %s, gzss_bc = %s, mxzsxfb_bs = %s, fss_bs = %s, '
        #         'wsgfl_bs = %s, bxb_js = %s, xxb_js = %s, xhdb_hl = %s, ssxlxb_js = %s, hxb_js = %s, sx = %s, '
        #         'sx_ls = %s, sx_ns = %s, sx_pds = %s, sx_sxs = %s, sx_chs = %s, ss_hs = %s, ss_qzs = %s, ts = %s, '
        #         'tz_bot = %s, tz_ht = %s, tz_nt = %s, tz_bat = %s, tz_rt = %s, tz_zt = %s, sx_mlxt = %s, '
        #         'mz_f = %s, mz_che = %s, mz_chi = %s, mz_sh = %s, mz_h = %s, mz_se = %s, mz_j = %s, mz_xia = %s, '
        #         'mz_xi = %s, mz_d = %s, mz_yl = %s, mz_wl = %s, tz_phz = %s, tz_qxz = %s, tz_yaxz = %s, '
        #         'tz_yixz = %s, tz_tsz = %s, tz_srz = %s, tz_xyz = %s, tz_qyz = %s, tz_tbz = %s, tz_jlcp = %s, '
        #         'tz_ypf = %s, tz_sydrwl = %s, tz_sblhli = %s, tz_xmbl = %s, tz_nsyhjbh = %s, tz_rysm = %s, '
        #         'tz_ryws = %s, tz_rypf = %s, tz_ryxh = %s, tz_ryqd = %s, tz_ryty = %s, tz_qyhgm = %s, '
        #         'tz_xhaj = %s, tz_shdrwl = %s, tz_ycxh = %s, tz_sjfl = %s, tz_wbypl = %s, tz_pl = %s, '
        #         'tz_sblhle = %s, tz_yyhgm = %s, tz_pclddx = %s, tz_ryfx = %s, tz_sjxfr = %s, tz_stlsfr = %s, '
        #         'tz_pfkcg = %s, tz_kcysh = %s, tz_rybm = %s, tz_yjgs = %s, tz_kgyz = %s, tz_xm = %s, tz_stcz = %s, '
        #         'tz_fbfm = %s, tz_etyzfmgd = %s, tz_syjz = %s, tz_zlnn = %s, tz_td = %s, tz_mbbbyn = %s, '
        #         'tz_yszc = %s, tz_kk = %s, tz_ndfr = %s, tz_dbjbj = %s, tz_dxsh = %s, tz_yncs = %s, tz_pxcx = %s, '
        #         'tz_lqxwhs = %s, tz_sstt = %s, tz_msha = %s, tz_hyq = %s, tz_jw = %s, tz_kcyspa = %s, '
        #         'tz_qxdc = %s, tz_jsjz = %s, tz_dcsg = %s, tz_ysjx = %s, tz_rfzt = %s, tz_wgtq = %s, '
        #         'tz_yhywg = %s, tz_dpt = %s, tz_lbt = %s, tz_kc = %s, tz_rygm = %s, tz_ryxmz = %s, tz_pfgmzd = %s, '
        #         'tz_zh = %s, UPDATE_TIME = %s WHERE ID = %s;',
        #         (
        #             'MZF' if common.get('DISEASE_CODE') == 3 else '',
        #             common.get('blh') if common.get('blh') != '' else None,
        #             common.get('nl') if common.get('nl') != '' else None,
        #             common.get('xingb') if common.get('xingb') != '' else None,
        #             common.get('csrq') if common.get('csrq') != '' else None,
        #             common.get('zy') if common.get('zy') != '' else None,
        #             common.get('zy_qt') if common.get('zy_qt') != '' else None,
        #             common.get('sg') if common.get('sg') != '' else None,
        #             common.get('tz') if common.get('tz') != '' else None,
        #             common.get('whcd') if common.get('whcd') != '' else None,
        #             common.get('st') if common.get('st') != '' else None,
        #             common.get('st_fz') if common.get('st_fz') != '' else None,
        #             common.get('st_zw') if common.get('st_zw') != '' else None,
        #             common.get('st_hw') if common.get('st_hw') != '' else None,
        #             common.get('st_ss') if common.get('st_ss') != '' else None,
        #             common.get('st_hh') if common.get('st_hh') != '' else None,
        #             common.get('st_dw') if common.get('st_dw') != '' else None,
        #             common.get('ms_qk') if common.get('ms_qk') != '' else None,
        #             common.get('wn') if common.get('wn') != '' else None,
        #             common.get('sm') if common.get('sm') != '' else None,
        #             common.get('sm_rskn') if common.get('sm_rskn') != '' else None,
        #             common.get('sm_yx') if common.get('sm_yx') != '' else None,
        #             common.get('sm_dm') if common.get('sm_dm') != '' else None,
        #             common.get('sm_ss') if common.get('sm_ss') != '' else None,
        #             common.get('sm_hz') if common.get('sm_hz') != '' else None,
        #             common.get('sm_qt') if common.get('sm_qt') != '' else None,
        #             common.get('xb') if common.get('xb') != '' else None,
        #             common.get('xb_cs') if common.get('xb_cs') != '' else None,
        #             common.get('db') if common.get('db') != '' else None,
        #             common.get('xy') if common.get('xy') != '' else None,
        #             common.get('xy_mtpjl') if common.get('xy_mtpjl') != '' else None,
        #             common.get('xy_sc') if common.get('xy_sc') != '' else None,
        #             common.get('jy') if common.get('jy') != '' else None,
        #             common.get('jy_sc') if common.get('jy_sc') != '' else None,
        #             common.get('gxy_bs') if common.get('gxy_bs') != '' else None,
        #             common.get('gxy_bc') if common.get('gxy_bc') != '' else None,
        #             common.get('gxb_jzs') if common.get('gxb_jzs') != '' else None,
        #             common.get('gxb_bc') if common.get('gxb_bc') != '' else None,
        #             common.get('tnb_bs') if common.get('tnb_bs') != '' else None,
        #             common.get('tnb_bc') if common.get('tnb_bc') != '' else None,
        #             common.get('gzss_bs') if common.get('gzss_bs') != '' else None,
        #             common.get('gzss_bc') if common.get('gzss_bc') != '' else None,
        #             common.get('mxzsxfb_bs') if common.get('mxzsxfb_bs') != '' else None,
        #             common.get('fss_bs') if common.get('fss_bs') != '' else None,
        #             common.get('wsgfl_bs') if common.get('wsgfl_bs') != '' else None,
        #             common.get('bxb_js') if common.get('bxb_js') != '' else None,
        #             common.get('xxb_js') if common.get('xxb_js') != '' else None,
        #             common.get('xhdb_hl') if common.get('xhdb_hl') != '' else None,
        #             common.get('ssxlxb_js') if common.get('ssxlxb_js') != '' else None,
        #             common.get('hxb_js') if common.get('hxb_js') != '' else None,
        #             common.get('sx') if common.get('sx') != '' else None,
        #             common.get('sx_ls') if common.get('sx_ls') != '' else None,
        #             common.get('sx_ns') if common.get('sx_ns') != '' else None,
        #             common.get('sx_pds') if common.get('sx_pds') != '' else None,
        #             common.get('sx_sxs') if common.get('sx_sxs') != '' else None,
        #             common.get('sx_chs') if common.get('sx_chs') != '' else None,
        #             common.get('ss_hs') if common.get('ss_hs') != '' else None,
        #             common.get('ss_qzs') if common.get('ss_qzs') != '' else None,
        #             common.get('ts') if common.get('ts') != '' else None,
        #             common.get('tz_bot') if common.get('tz_bot') != '' else None,
        #             common.get('tz_ht') if common.get('tz_ht') != '' else None,
        #             common.get('tz_nt') if common.get('tz_nt') != '' else None,
        #             common.get('tz_bat') if common.get('tz_bat') != '' else None,
        #             common.get('tz_rt') if common.get('tz_rt') != '' else None,
        #             common.get('tz_zt') if common.get('tz_zt') != '' else None,
        #             common.get('sx_mlxt') if common.get('sx_mlxt') != '' else None,
        #             common.get('mz_f') if common.get('mz_f') != '' else None,
        #             common.get('mz_che') if common.get('mz_che') != '' else None,
        #             common.get('mz_chi') if common.get('mz_chi') != '' else None,
        #             common.get('mz_sh') if common.get('mz_sh') != '' else None,
        #             common.get('mz_h') if common.get('mz_h') != '' else None,
        #             common.get('mz_se') if common.get('mz_se') != '' else None,
        #             common.get('mz_j') if common.get('mz_j') != '' else None,
        #             common.get('mz_xia') if common.get('mz_xia') != '' else None,
        #             common.get('mz_xi') if common.get('mz_xi') != '' else None,
        #             common.get('mz_d') if common.get('mz_d') != '' else None,
        #             common.get('mz_yl') if common.get('mz_yl') != '' else None,
        #             common.get('mz_wl') if common.get('mz_wl') != '' else None,
        #             common.get('tz_phz') if common.get('tz_phz') != '' else None,
        #             common.get('tz_qxz') if common.get('tz_qxz') != '' else None,
        #             common.get('tz_yaxz') if common.get('tz_yaxz') != '' else None,
        #             common.get('tz_yixz') if common.get('tz_yixz') != '' else None,
        #             common.get('tz_tsz') if common.get('tz_tsz') != '' else None,
        #             common.get('tz_srz') if common.get('tz_srz') != '' else None,
        #             common.get('tz_xyz') if common.get('tz_xyz') != '' else None,
        #             common.get('tz_qyz') if common.get('tz_qyz') != '' else None,
        #             common.get('tz_tbz') if common.get('tz_tbz') != '' else None,
        #             common.get('tz_jlcp') if common.get('tz_jlcp') != '' else None,
        #             common.get('tz_ypf') if common.get('tz_ypf') != '' else None,
        #             common.get('tz_sydrwl') if common.get('tz_sydrwl') != '' else None,
        #             common.get('tz_sblhli') if common.get('tz_sblhli') != '' else None,
        #             common.get('tz_xmbl') if common.get('tz_xmbl') != '' else None,
        #             common.get('tz_nsyhjbh') if common.get('tz_nsyhjbh') != '' else None,
        #             common.get('tz_rysm') if common.get('tz_rysm') != '' else None,
        #             common.get('tz_ryws') if common.get('tz_ryws') != '' else None,
        #             common.get('tz_rypf') if common.get('tz_rypf') != '' else None,
        #             common.get('tz_ryxh') if common.get('tz_ryxh') != '' else None,
        #             common.get('tz_ryqd') if common.get('tz_ryqd') != '' else None,
        #             common.get('tz_ryty') if common.get('tz_ryty') != '' else None,
        #             common.get('tz_qyhgm') if common.get('tz_qyhgm') != '' else None,
        #             common.get('tz_xhaj') if common.get('tz_xhaj') != '' else None,
        #             common.get('tz_shdrwl') if common.get('tz_shdrwl') != '' else None,
        #             common.get('tz_ycxh') if common.get('tz_ycxh') != '' else None,
        #             common.get('tz_sjfl') if common.get('tz_sjfl') != '' else None,
        #             common.get('tz_wbypl') if common.get('tz_wbypl') != '' else None,
        #             common.get('tz_pl') if common.get('tz_pl') != '' else None,
        #             common.get('tz_sblhle') if common.get('tz_sblhle') != '' else None,
        #             common.get('tz_yyhgm') if common.get('tz_yyhgm') != '' else None,
        #             common.get('tz_pclddx') if common.get('tz_pclddx') != '' else None,
        #             common.get('tz_ryfx') if common.get('tz_ryfx') != '' else None,
        #             common.get('tz_sjxfr') if common.get('tz_sjxfr') != '' else None,
        #             common.get('tz_stlsfr') if common.get('tz_stlsfr') != '' else None,
        #             common.get('tz_pfkcg') if common.get('tz_pfkcg') != '' else None,
        #             common.get('tz_kcysh') if common.get('tz_kcysh') != '' else None,
        #             common.get('tz_rybm') if common.get('tz_rybm') != '' else None,
        #             common.get('tz_yjgs') if common.get('tz_yjgs') != '' else None,
        #             common.get('tz_kgyz') if common.get('tz_kgyz') != '' else None,
        #             common.get('tz_xm') if common.get('tz_xm') != '' else None,
        #             common.get('tz_stcz') if common.get('tz_stcz') != '' else None,
        #             common.get('tz_fbfm') if common.get('tz_fbfm') != '' else None,
        #             common.get('tz_etyzfmgd') if common.get('tz_etyzfmgd') != '' else None,
        #             common.get('tz_syjz') if common.get('tz_syjz') != '' else None,
        #             common.get('tz_zlnn') if common.get('tz_zlnn') != '' else None,
        #             common.get('tz_td') if common.get('tz_td') != '' else None,
        #             common.get('tz_mbbbyn') if common.get('tz_mbbbyn') != '' else None,
        #             common.get('tz_yszc') if common.get('tz_yszc') != '' else None,
        #             common.get('tz_kk') if common.get('tz_kk') != '' else None,
        #             common.get('tz_ndfr') if common.get('tz_ndfr') != '' else None,
        #             common.get('tz_dbjbj') if common.get('tz_dbjbj') != '' else None,
        #             common.get('tz_dxsh') if common.get('tz_dxsh') != '' else None,
        #             common.get('tz_yncs') if common.get('tz_yncs') != '' else None,
        #             common.get('tz_pxcx') if common.get('tz_pxcx') != '' else None,
        #             common.get('tz_lqxwhs') if common.get('tz_lqxwhs') != '' else None,
        #             common.get('tz_sstt') if common.get('tz_sstt') != '' else None,
        #             common.get('tz_msha') if common.get('tz_msha') != '' else None,
        #             common.get('tz_hyq') if common.get('tz_hyq') != '' else None,
        #             common.get('tz_jw') if common.get('tz_jw') != '' else None,
        #             common.get('tz_kcyspa') if common.get('tz_kcyspa') != '' else None,
        #             common.get('tz_qxdc') if common.get('tz_qxdc') != '' else None,
        #             common.get('tz_jsjz') if common.get('tz_jsjz') != '' else None,
        #             common.get('tz_dcsg') if common.get('tz_dcsg') != '' else None,
        #             common.get('tz_ysjx') if common.get('tz_ysjx') != '' else None,
        #             common.get('tz_rfzt') if common.get('tz_rfzt') != '' else None,
        #             common.get('tz_wgtq') if common.get('tz_wgtq') != '' else None,
        #             common.get('tz_yhywg') if common.get('tz_yhywg') != '' else None,
        #             common.get('tz_dpt') if common.get('tz_dpt') != '' else None,
        #             common.get('tz_lbt') if common.get('tz_lbt') != '' else None,
        #             common.get('tz_kc') if common.get('tz_kc') != '' else None,
        #             common.get('tz_rygm') if common.get('tz_rygm') != '' else None,
        #             common.get('tz_ryxmz') if common.get('tz_ryxmz') != '' else None,
        #             common.get('tz_pfgmzd') if common.get('tz_pfgmzd') != '' else None,
        #             common.get('tz_zh') if common.get('tz_zh') != '' else None,
        #             time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), table_common_id))
        #     conn.commit()
        #     return table_mzf_id
        return table_mzf_id
        # except Exception as ex:
        #     logging.error('[更新common异常]' + str(ex))
        #     conn.rollback()
        #     return None


if __name__ == '__main__':
    flag = True
    # 调用API，获取数据
    logging.info('调用API，获取数据')
    data = get_data()
    # 数据入库
    logging.info('数据入库')
    conn = get_conn()
    cur = conn.cursor()
    if data is not None:
        for record in data:
            user = record.get('user')
            table_patient_id = user_to_db(cur=cur, user=user)
            if table_patient_id is not None:
                common = record.get('common')
                if len(common) == 442:
                    table_common_id = common_to_db(cur=cur, common=common, table_patient_id=table_patient_id)
                    if table_common_id is not None:
                        table_mzf_id = mzf_to_db(cur=cur, common=common, table_common_id=table_common_id)
                        print(table_mzf_id)
                        if table_common_id is None:
                            flag = False
                            break
                    else:
                        flag = False
                        break
                else:
                    logging.warning('[数据长度异常，非442]该数据长度为：' + str(len(common)))
            else:
                flag = False
                break
    cur.close()
    conn.close()
    if flag:
        logging.info('数据同步完成，同步完成时间：' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    else:
        logging.error('数据同步失败，请检查数据')
