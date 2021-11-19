#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymysql
import time
import logging
import configparser
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


# def isVaildDate(date):
#     try:
#         if ":" in date:
#             time.strptime(date, "%Y-%m-%d %H:%M:%S")
#         else:
#             time.strptime(date, "%Y-%m-%d")
#         return True
#     except:
#         return False


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
    cur.execute('SELECT ID,UPDATE_TIME FROM record_common2 where PATIENT_ID = %s;', table_patient_id)
    table_common_check = cur.fetchone()
    if table_common_check is None:
        try:
            logging.info('common表入库')
            cur.execute(
                'INSERT INTO record_common2(ID, BLLX, SUBJECT_ID, DISEASE_CODE, yljg_bh, yljg_mc, bscjrq, blh, nl, '
                'xingb, csrq, zy, zy_qt, sg, tz, whcd, shent, st_fz, st_zw, st_hm, st_ss, st_hh, st_dm, ms_qk, wn, sm, '
                'sm_rskn, sm_yx, sm_dm, sm_ss, sm_hz, sm_qt, xb, xb_cs, db, db_cs, db_zdgy, db_zdtjbt, db_zdnnbs, '
                'db_zdzrbcx, xy, xy_mtpjl, xy_sc, jy, jy_sc, gxy_bs, gxy_bc, gxb_jzs, gxb_bc, tnb_bs, tnb_bc, gzss_bs, '
                'gzss_bc, mxzsxfb_bs, fss_bs, wsgfl_bs, bxb_js, xxb_js, xhdb_hl, ssxlxb_js, hxb_js, sx, sx_ls, sx_ns, '
                'sx_pds, sx_sxs, st_wxs, sx_chs, ss_hs, ss_qzs, ts, tz_bot, tz_ht, tz_nt, tz_bat, tz_rt, tz_zt, '
                'sx_mlxt, mz_f, mz_che, mz_chi, mz_sh, mz_h, mz_se, mz_j, mz_xia, mz_xi, mz_d, mz_yl, mz_wl, tz_phz, '
                'tz_qxz, tz_yaxz, tz_yixz, tz_tsz, tz_srz, tz_xyz, tz_qyz, tz_tbz, tz_jlcp, tz_ypf, tz_sydrwl, '
                'tz_sblhli, tz_xmbl, tz_nsyhjbh, tz_rysm, tz_ryws, tz_rypf, tz_ryxh, tz_ryqd, tz_ryty, tz_qyhgm, '
                'tz_xhaj, tz_shdrwl, tz_ycxh, tz_sjfl, tz_wbypl, tz_pl, tz_sblhle, tz_yyhgm, tz_pclddx, tz_ryfx, '
                'tz_sjxfr, tz_stlsfr, tz_pfkcg, tz_kcysh, tz_rybm, tz_yjgs, tz_kgyz, tz_xm, tz_stcz, tz_fbfm, '
                'tz_etyzfmgd, tz_syjz, tz_zlnn, tz_td, tz_mbbbyn, tz_yszc, tz_kk, tz_ndfr, tz_dbjbj, tz_dxsh, tz_yncs, '
                'tz_pxcx, tz_lqxwhs, tz_sstt, tz_msha, tz_hyq, tz_jw, tz_kcyspa, tz_qxdc, tz_jsjz, tz_dcsg, tz_ysjx, '
                'tz_rfzt, tz_wgtq, tz_yhywg, tz_dpt, tz_lbt, tz_kc, tz_rygm, tz_ryxmz, tz_pfgmzd, tz_zh, USER_ID, '
                'ORG_ID, CREATE_TIME, UPDATE_TIME, STATUS, PATIENT_ID)'
                'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',
                (None, 0, 105, 'MZF' if common.get('DISEASE_CODE') == 3 else '', 125, '浙江省中医院',
                 common.get('bscjrq') if common.get('bscjrq') != '' else None,
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
                 common.get('db_cs') if common.get('db_cs') != '' else None,
                 common.get('db_zdgy') if common.get('db_zdgy') != '' else None,
                 common.get('db_zdtjbt') if common.get('db_zdtjbt') != '' else None,
                 common.get('db_zdnnbs') if common.get('db_zdnnbs') != '' else None,
                 common.get('db_zdzrbcx') if common.get('db_zdzrbcx') != '' else None,
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
                 common.get('st_wxs') if common.get('st_wxs') != '' else None,
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
                 common.get('CREATE_TIME'), common.get('UPDATE_TIME'), 0, table_patient_id))
            table_common_id = conn.insert_id()
            conn.commit()
            return table_common_id
        except Exception as ex:
            logging.error('[插入common异常]' + str(ex))
            print(common)
            conn.rollback()
            return None
    else:
        table_common_id = table_common_check[0]
        common_update_time = table_common_check[1]
        # if str(common_update_time) != str(common.get('UPDATE_TIME')):
        if 1 == 1:
            try:
                logging.info('common表刷新')
                cur.execute(
                    'UPDATE record_common2 SET DISEASE_CODE = %s, bscjrq = %s, blh = %s, nl = %s, xingb = %s, '
                    'csrq = %s, zy = %s, zy_qt = %s, sg = %s, tz = %s, whcd = %s, shent = %s, st_fz = %s, st_zw = %s, '
                    'st_hm = %s, st_ss = %s, st_hh = %s, st_dm = %s, ms_qk = %s, wn = %s, sm = %s, sm_rskn = %s, '
                    'sm_yx = %s, sm_dm = %s, sm_ss = %s, sm_hz = %s, sm_qt = %s, xb = %s, xb_cs = %s, db = %s, '
                    'db_cs = %s, db_zdgy = %s, db_zdtjbt = %s, db_zdnnbs = %s, db_zdzrbcx = %s, xy = %s, '
                    'xy_mtpjl = %s, xy_sc = %s, jy = %s, jy_sc = %s, gxy_bs = %s, gxy_bc = %s, gxb_jzs = %s, '
                    'gxb_bc = %s, tnb_bs = %s, tnb_bc = %s, gzss_bs = %s, gzss_bc = %s, mxzsxfb_bs = %s, fss_bs = %s, '
                    'wsgfl_bs = %s, bxb_js = %s, xxb_js = %s, xhdb_hl = %s, ssxlxb_js = %s, hxb_js = %s, sx = %s, '
                    'sx_ls = %s, sx_ns = %s, sx_pds = %s, sx_sxs = %s, st_wxs = %s, sx_chs = %s, ss_hs = %s, '
                    'ss_qzs = %s, ts = %s, tz_bot = %s, tz_ht = %s, tz_nt = %s, tz_bat = %s, tz_rt = %s, tz_zt = %s, '
                    'sx_mlxt = %s, mz_f = %s, mz_che = %s, mz_chi = %s, mz_sh = %s, mz_h = %s, mz_se = %s, mz_j = %s, '
                    'mz_xia = %s, mz_xi = %s, mz_d = %s, mz_yl = %s, mz_wl = %s, tz_phz = %s, tz_qxz = %s, '
                    'tz_yaxz = %s, tz_yixz = %s, tz_tsz = %s, tz_srz = %s, tz_xyz = %s, tz_qyz = %s, tz_tbz = %s, '
                    'tz_jlcp = %s, tz_ypf = %s, tz_sydrwl = %s, tz_sblhli = %s, tz_xmbl = %s, tz_nsyhjbh = %s, '
                    'tz_rysm = %s, tz_ryws = %s, tz_rypf = %s, tz_ryxh = %s, tz_ryqd = %s, tz_ryty = %s, '
                    'tz_qyhgm = %s, tz_xhaj = %s, tz_shdrwl = %s, tz_ycxh = %s, tz_sjfl = %s, tz_wbypl = %s, '
                    'tz_pl = %s, tz_sblhle = %s, tz_yyhgm = %s, tz_pclddx = %s, tz_ryfx = %s, tz_sjxfr = %s, '
                    'tz_stlsfr = %s, tz_pfkcg = %s, tz_kcysh = %s, tz_rybm = %s, tz_yjgs = %s, tz_kgyz = %s, '
                    'tz_xm = %s, tz_stcz = %s, tz_fbfm = %s, tz_etyzfmgd = %s, tz_syjz = %s, tz_zlnn = %s, tz_td = %s, '
                    'tz_mbbbyn = %s, tz_yszc = %s, tz_kk = %s, tz_ndfr = %s, tz_dbjbj = %s, tz_dxsh = %s, '
                    'tz_yncs = %s, tz_pxcx = %s, tz_lqxwhs = %s, tz_sstt = %s, tz_msha = %s, tz_hyq = %s, tz_jw = %s, '
                    'tz_kcyspa = %s, tz_qxdc = %s, tz_jsjz = %s, tz_dcsg = %s, tz_ysjx = %s, tz_rfzt = %s, '
                    'tz_wgtq = %s, tz_yhywg = %s, tz_dpt = %s, tz_lbt = %s, tz_kc = %s, tz_rygm = %s, tz_ryxmz = %s, '
                    'tz_pfgmzd = %s, tz_zh = %s, CREATE_TIME = %s, UPDATE_TIME = %s WHERE ID = %s AND SUBJECT_ID = 105 '
                    'AND ORG_ID = 125;',
                    (
                        'MZF' if common.get('DISEASE_CODE') == 3 else '',
                        common.get('bscjrq') if common.get('bscjrq') != '' else None,
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
                        common.get('db_cs') if common.get('db_cs') != '' else None,
                        common.get('db_zdgy') if common.get('db_zdgy') != '' else None,
                        common.get('db_zdtjbt') if common.get('db_zdtjbt') != '' else None,
                        common.get('db_zdnnbs') if common.get('db_zdnnbs') != '' else None,
                        common.get('db_zdzrbcx') if common.get('db_zdzrbcx') != '' else None,
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
                        common.get('st_wxs') if common.get('st_wxs') != '' else None,
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
                        common.get('CREATE_TIME'), common.get('UPDATE_TIME'), table_common_id))
                conn.commit()
                return table_common_id
            except Exception as ex:
                logging.error('[更新common异常]' + str(ex))
                conn.rollback()
                return None
        else:
            logging.info('common数据为最新版本，无需更新')
            return table_common_id


def mzf_to_db(cur, common, table_common_id):
    cur.execute('SELECT ID,UPDATE_TIME FROM record_mzf2 where COMMON_ID = %s;', table_common_id)
    table_mzf_check = cur.fetchone()
    if table_mzf_check is None:
        try:
            logging.info('mzf表入库')
            cur.execute(
                'INSERT INTO record_mzf2(ID, COMMON_ID, gwys_xy, gwys_hymxtdjb, gwys_ymfzjzs, gwys_jzzkqwryzdq, '
                'gwys_csjcfcgz, gwys_ffhxhxdgr, gwys_jzzqhhldq, gwys_wssaqf, gwys_yyzkjc, copdzd, ywzl, '
                'ywzl_dyxrzqgkzjsaba, ywzl_dyxrzqgkzjsama, ywzl_dyxrzqgkzjlaba, ywzl_dyxrzqgkzjlama, '
                'ywzl_dyxrzqgkzjsfgl, ywzl_xrjssfgl, ywzl_lhxrzjicslaba, ywzl_lhxrzjicslabalama, ywzl_lhxrzjlabalama, '
                'ywzl_lhxrzjsfgl, ywzl_kfjssfgl, ywzl_kfcjlsfgl, ywzl_htlywsfgl, fywzl, fywzl_jy, fywzl_ywjzym, '
                'fywzl_cjzlgym, fywzl_cjzfyym, fywzl_ywfkfzl, fywzl_ywjtyl, fywzl_jtylsc, fywzl_ywwchxj, '
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
                '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',
                (None, table_common_id,
                 common.get('gwys_xy') if common.get('gwys_xy') != '' else None,
                 common.get('gwys_hymxtdjb') if common.get('gwys_hymxtdjb') != '' else None,
                 common.get('gwys_ymfzjzs') if common.get('gwys_ymfzjzs') != '' else None,
                 common.get('gwys_jzzkqwryzdq') if common.get('gwys_jzzkqwryzdq') != '' else None,
                 common.get('gwys_csjcfcgz') if common.get('gwys_csjcfcgz') != '' else None,
                 common.get('gwys_ffhxhxdgr') if common.get('gwys_ffhxhxdgr') != '' else None,
                 common.get('gwys_jzzqhhldq') if common.get('gwys_jzzqhhldq') != '' else None,
                 common.get('gwys_wssaqf') if common.get('gwys_wssaqf') != '' else None,
                 common.get('gwys_yyzkjc') if common.get('gwys_yyzkjc') != '' else None,
                 common.get('copdzd') if common.get('copdzd') != '' else None,
                 common.get('ywzl') if common.get('ywzl') != '' else None,
                 common.get('ywzl_dyxrzqgkzjsaba') if common.get('ywzl_dyxrzqgkzjsaba') != '' else None,
                 common.get('ywzl_dyxrzqgkzjsana') if common.get('ywzl_dyxrzqgkzjsana') != '' else None,
                 common.get('ywzl_dyxrzqgkzjlaba') if common.get('ywzl_dyxrzqgkzjlaba') != '' else None,
                 common.get('ywzl_dyxrzqgkzjlama') if common.get('ywzl_dyxrzqgkzjlama') != '' else None,
                 common.get('ywzl_dyxrzqgkzjsfgl') if common.get('ywzl_dyxrzqgkzjsfgl') != '' else None,
                 common.get('ywzl_xrjssfgl') if common.get('ywzl_xrjssfgl') != '' else None,
                 common.get('ywzl_lhxrzjicslaba') if common.get('ywzl_lhxrzjicslaba') != '' else None,
                 common.get('ywzl_lhxrzjicslabalama') if common.get('ywzl_lhxrzjicslabalama') != '' else None,
                 common.get('ywzl_lhxrzjlabalama') if common.get('ywzl_lhxrzjlabalama') != '' else None,
                 common.get('ywzl_lhxrzjsfgl') if common.get('ywzl_lhxrzjsfgl') != '' else None,
                 common.get('ywzl_kfjssfgl') if common.get('ywzl_kfjssfgl') != '' else None,
                 common.get('ywzl_kfcjlsfgl') if common.get('ywzl_kfcjlsfgl') != '' else None,
                 common.get('ywzl_htlywsfgl') if common.get('ywzl_htlywsfgl') != '' else None,
                 common.get('fywzl') if common.get('fywzl') != '' else None,
                 common.get('fywzl_jy') if common.get('fywzl_jy') != '' else None,
                 common.get('fywzl_ywjzym') if common.get('fywzl_ywjzym') != '' else None,
                 common.get('fywzl_cjzlgym') if common.get('fywzl_cjzlgym') != '' else None,
                 common.get('fywzl_cjzfyym') if common.get('fywzl_cjzfyym') != '' else None,
                 common.get('fywzl_ywfkfzl') if common.get('fywzl_ywfkfzl') != '' else None,
                 common.get('fywzl_ywjtyl') if common.get('fywzl_ywjtyl') != '' else None,
                 common.get('fywzl_jtylsc') if common.get('fywzl_jtylsc') != '' else None,
                 common.get('fywzl_ywwchxj') if common.get('fywzl_ywwchxj') != '' else None,
                 common.get('fywzl_wchxjsc') if common.get('fywzl_wchxjsc') != '' else None,
                 common.get('fywzl_dxzqgkzj') if common.get('fywzl_dxzqgkzj') != '' else None,
                 common.get('fywzl_dxzqgkzjcs') if common.get('fywzl_dxzqgkzjcs') != '' else None,
                 common.get('fywzl_jzjz') if common.get('fywzl_jzjz') != '' else None,
                 common.get('fywzl_jzjzcs') if common.get('fywzl_jzjzcs') != '' else None,
                 common.get('fywzl_zyzl') if common.get('fywzl_zyzl') != '' else None,
                 common.get('fywzl_zyzlcs') if common.get('fywzl_zyzlcs') != '' else None,
                 common.get('hxxtjb') if common.get('hxxtjb') != '' else None,
                 common.get('hxxtjb_zqgkz') if common.get('hxxtjb_zqgkz') != '' else None,
                 common.get('hxxtjb_zqgxc') if common.get('hxxtjb_zqgxc') != '' else None,
                 common.get('hxxtjb_gmxby') if common.get('hxxtjb_gmxby') != '' else None,
                 common.get('hxxtjb_fbdy') if common.get('hxxtjb_fbdy') != '' else None,
                 common.get('hxxtjb_fss') if common.get('hxxtjb_fss') != '' else None,
                 common.get('hxxtjb_fjzxwh') if common.get('hxxtjb_fjzxwh') != '' else None,
                 common.get('hxxtjb_fa') if common.get('hxxtjb_fa') != '' else None,
                 common.get('hxxtjb_smhxztzhz') if common.get('hxxtjb_smhxztzhz') != '' else None,
                 common.get('hxxtjb_hxsj') if common.get('hxxtjb_hxsj') != '' else None,
                 common.get('hxxtjb_fjh') if common.get('hxxtjb_fjh') != '' else None,
                 common.get('hxxtjb_fdmgy') if common.get('hxxtjb_fdmgy') != '' else None,
                 common.get('hxxtjb_sc') if common.get('hxxtjb_sc') != '' else None,
                 common.get('xxgjb') if common.get('xxgjb') != '' else None,
                 common.get('xxgjb_gxy') if common.get('xxgjb_gxy') != '' else None,
                 common.get('xxgjb_gxb') if common.get('xxgjb_gxb') != '' else None,
                 common.get('xxgjb_jxxgnbq') if common.get('xxgjb_jxxgnbq') != '' else None,
                 common.get('xxgjb_fyxxzb') if common.get('xxgjb_fyxxzb') != '' else None,
                 common.get('xxgjb_mxxgnbq') if common.get('xxgjb_mxxgnbq') != '' else None,
                 common.get('xxgjb_fc') if common.get('xxgjb_fc') != '' else None,
                 common.get('xxgjb_yszcdzz') if common.get('xxgjb_yszcdzz') != '' else None,
                 common.get('xxgjb_sxzb') if common.get('xxgjb_sxzb') != '' else None,
                 common.get('xxgjb_sc') if common.get('xxgjb_sc') != '' else None,
                 common.get('nfmxtjb') if common.get('nfmxtjb') != '' else None,
                 common.get('nfmxtjb_tnb') if common.get('nfmxtjb_tnb') != '' else None,
                 common.get('nfmxtjb_gzss') if common.get('nfmxtjb_gzss') != '' else None,
                 common.get('nfmxtjb_sc') if common.get('nfmxtjb_sc') != '' else None,
                 common.get('xhxtjb') if common.get('xhxtjb') != '' else None,
                 common.get('xhxtjb_wsgfl') if common.get('xhxtjb_wsgfl') != '' else None,
                 common.get('xhxtjb_bm') if common.get('xhxtjb_bm') != '' else None,
                 common.get('xhxtjb_yzxcb') if common.get('xhxtjb_yzxcb') != '' else None,
                 common.get('xhxtjb_gyh') if common.get('xhxtjb_gyh') != '' else None,
                 common.get('xhxtjb_sc') if common.get('xhxtjb_sc') != '' else None,
                 common.get('qtxtjb') if common.get('qtxtjb') != '' else None,
                 common.get('qtxtjb_yyz') if common.get('qtxtjb_yyz') != '' else None,
                 common.get('qtxtjb_qtbwzl') if common.get('qtxtjb_qtbwzl') != '' else None,
                 common.get('qtxtjb_sgnbq') if common.get('qtxtjb_sgnbq') != '' else None,
                 common.get('qtxtjb_gljrza') if common.get('qtxtjb_gljrza') != '' else None,
                 common.get('qtxtjb_xkjx') if common.get('qtxtjb_xkjx') != '' else None,
                 common.get('qtxtjb_swywygms') if common.get('qtxtjb_swywygms') != '' else None,
                 common.get('qtxtjb_xbss') if common.get('qtxtjb_xbss') != '' else None,
                 common.get('xypc') if common.get('xypc') != '' else None,
                 common.get('yl') if common.get('yl') != '' else None,
                 common.get('xycsnl') if common.get('xycsnl') != '' else None,
                 common.get('xyjsnl') if common.get('xyjsnl') != '' else None,
                 common.get('pjmtxys') if common.get('pjmtxys') != '' else None,
                 common.get('xysc') if common.get('xysc') != '' else None,
                 common.get('jynl') if common.get('jynl') != '' else None,
                 common.get('jysc') if common.get('jysc') != '' else None,
                 common.get('esyjc') if common.get('esyjc') != '' else None,
                 common.get('esyjc_e14') if common.get('esyjc_e14') != '' else None,
                 common.get('esyjc_a14') if common.get('esyjc_a14') != '' else None,
                 common.get('esyjcsj') if common.get('esyjcsj') != '' else None,
                 common.get('swrlzf') if common.get('swrlzf') != '' else None,
                 common.get('mnsnqnyw') if common.get('mnsnqnyw') != '' else None,
                 common.get('snzfqncxsj') if common.get('snzfqncxsj') != '' else None,
                 common.get('snzfqnsyqj') if common.get('snzfqnsyqj') != '' else None,
                 common.get('zfqnfhcs') if common.get('zfqnfhcs') != '' else None,
                 common.get('zfqnfhcs_kz') if common.get('zfqnfhcs_kz') != '' else None,
                 common.get('zfqnfhcs_fdmj') if common.get('zfqnfhcs_fdmj') != '' else None,
                 common.get('zfqnfhcs_yyj') if common.get('zfqnfhcs_yyj') != '' else None,
                 common.get('zfqnfhcs_pqs') if common.get('zfqnfhcs_pqs') != '' else None,
                 common.get('zfqnfhpc') if common.get('zfqnfhpc') != '' else None,
                 common.get('qtfszf') if common.get('qtfszf') != '' else None,
                 common.get('qtfszf_trq') if common.get('qtfszf_trq') != '' else None,
                 common.get('qtfszf_mb') if common.get('qtfszf_mb') != '' else None,
                 common.get('qtfszf_yhq') if common.get('qtfszf_yhq') != '' else None,
                 common.get('mnsnzfyw') if common.get('mnsnzfyw') != '' else None,
                 common.get('snzfcxsj') if common.get('snzfcxsj') != '' else None,
                 common.get('snzfsyqj') if common.get('snzfsyqj') != '' else None,
                 common.get('snzffhcs') if common.get('snzffhcs') != '' else None,
                 common.get('snzffhcs_kz') if common.get('snzffhcs_kz') != '' else None,
                 common.get('snzffhcs_fdmj') if common.get('snzffhcs_fdmj') != '' else None,
                 common.get('snzffhcs_yyj') if common.get('snzffhcs_yyj') != '' else None,
                 common.get('snzffhcs_pqs') if common.get('snzffhcs_pqs') != '' else None,
                 common.get('snzffhcsqt') if common.get('snzffhcsqt') != '' else None,
                 common.get('snzffhpc') if common.get('snzffhpc') != '' else None,
                 common.get('fcjc') if common.get('fcjc') != '' else None,
                 common.get('mnfcjcys') if common.get('mnfcjcys') != '' else None,
                 common.get('fcjccxsj') if common.get('fcjccxsj') != '' else None,
                 common.get('fcjcfhcs') if common.get('fcjcfhcs') != '' else None,
                 common.get('fcjcfhcs_kz') if common.get('fcjcfhcs_kz') != '' else None,
                 common.get('fcjcfhcs_fdmj') if common.get('fcjcfhcs_fdmj') != '' else None,
                 common.get('fcjcfhcs_fhf') if common.get('fcjcfhcs_fhf') != '' else None,
                 common.get('fcjcfhcs_fctk') if common.get('fcjcfhcs_fctk') != '' else None,
                 common.get('fcjcfhcs_qxz') if common.get('fcjcfhcs_qxz') != '' else None,
                 common.get('fcjcfhpc') if common.get('fcjcfhpc') != '' else None,
                 common.get('ydqtjc') if common.get('ydqtjc') != '' else None,
                 common.get('mnydqtjcys') if common.get('mnydqtjcys') != '' else None,
                 common.get('ydqtjccxsj') if common.get('ydqtjccxsj') != '' else None,
                 common.get('ydqtjcfhcs') if common.get('ydqtjcfhcs') != '' else None,
                 common.get('ydqtjcfhcs_kz') if common.get('ydqtjcfhcs_kz') != '' else None,
                 common.get('ydqtjcfhcs_fdmj') if common.get('ydqtjcfhcs_fdmj') != '' else None,
                 common.get('ydqtjcfhcs_fhf') if common.get('ydqtjcfhcs_fhf') != '' else None,
                 common.get('ydqtjcfhcs_fctk') if common.get('ydqtjcfhcs_fctk') != '' else None,
                 common.get('ydqtjcfhcs_qxz') if common.get('ydqtjcfhcs_qxz') != '' else None,
                 common.get('ydqtjcfhpc') if common.get('ydqtjcfhpc') != '' else None,
                 common.get('zjskljc') if common.get('zjskljc') != '' else None,
                 common.get('mnzjskljcys') if common.get('mnzjskljcys') != '' else None,
                 common.get('zjskljccxsj') if common.get('zjskljccxsj') != '' else None,
                 common.get('zjskljcfhcs') if common.get('zjskljcfhcs') != '' else None,
                 common.get('zjskljcfhcs_kz') if common.get('zjskljcfhcs_kz') != '' else None,
                 common.get('zjskljcfhcs_fdmj') if common.get('zjskljcfhcs_fdmj') != '' else None,
                 common.get('zjskljcfhcs_fhf') if common.get('zjskljcfhcs_fhf') != '' else None,
                 common.get('zjskljcfhcs_fctk') if common.get('zjskljcfhcs_fctk') != '' else None,
                 common.get('zjskljcfhcs_qxz') if common.get('zjskljcfhcs_qxz') != '' else None,
                 common.get('zjskljcfhpc') if common.get('zjskljcfhpc') != '' else None,
                 common.get('ydkljc') if common.get('ydkljc') != '' else None,
                 common.get('mnydkljcys') if common.get('mnydkljcys') != '' else None,
                 common.get('ydkljccxsj') if common.get('ydkljccxsj') != '' else None,
                 common.get('ydkljcfhcs') if common.get('ydkljcfhcs') != '' else None,
                 common.get('ydkljcfhcs_kz') if common.get('ydkljcfhcs_kz') != '' else None,
                 common.get('ydkljcfhcs_fdmz') if common.get('ydkljcfhcs_fdmz') != '' else None,
                 common.get('ydkljcfhcs_fhf') if common.get('ydkljcfhcs_fhf') != '' else None,
                 common.get('ydkljcfhcs_fctk') if common.get('ydkljcfhcs_fctk') != '' else None,
                 common.get('ydkljcfhcs_qxz') if common.get('ydkljcfhcs_qxz') != '' else None,
                 common.get('ydkljcfhpc') if common.get('ydkljcfhpc') != '' else None,
                 common.get('cakljc') if common.get('cakljc') != '' else None,
                 common.get('mncakljcys') if common.get('mncakljcys') != '' else None,
                 common.get('cakljccxsj') if common.get('cakljccxsj') != '' else None,
                 common.get('cakljcfhcs') if common.get('cakljcfhcs') != '' else None,
                 common.get('cakljcfhcs_kz') if common.get('cakljcfhcs_kz') != '' else None,
                 common.get('cakljcfhcs_fdmz') if common.get('cakljcfhcs_fdmz') != '' else None,
                 common.get('cakljcfhcs_fhf') if common.get('cakljcfhcs_fhf') != '' else None,
                 common.get('cakljcfhcs_fctk') if common.get('cakljcfhcs_fctk') != '' else None,
                 common.get('cakljcfhcs_qxz') if common.get('cakljcfhcs_qxz') != '' else None,
                 common.get('cakljcfhpc') if common.get('cakljcfhpc') != '' else None,
                 common.get('ynffmxqdjb') if common.get('ynffmxqdjb') != '' else None,
                 common.get('hxdjbjzs') if common.get('hxdjbjzs') != '' else None,
                 common.get('ynnhxdgr') if common.get('ynnhxdgr') != '' else None,
                 common.get('ynnhxdgrcs') if common.get('ynnhxdgrcs') != '' else None,
                 common.get('zg') if common.get('zg') != '' else None,
                 common.get('psyg') if common.get('psyg') != '' else None,
                 common.get('ynpjgmcs') if common.get('ynpjgmcs') != '' else None,
                 common.get('bbzz_ht') if common.get('bbzz_ht') != '' else None,
                 common.get('bbzz_qt') if common.get('bbzz_qt') != '' else None,
                 common.get('bbzz_bs') if common.get('bbzz_bs') != '' else None,
                 common.get('bbzz_hxbm') if common.get('bbzz_hxbm') != '' else None,
                 common.get('ks') if common.get('ks') != '' else None,
                 common.get('kscd') if common.get('kscd') != '' else None,
                 common.get('kssx') if common.get('kssx') != '' else None,
                 common.get('ksjzys') if common.get('ksjzys') != '' else None,
                 common.get('ksjzys_gm') if common.get('ksjzys_gm') != '' else None,
                 common.get('ksjzys_tyhxxtyw') if common.get('ksjzys_tyhxxtyw') != '' else None,
                 common.get('ksjzys_ll') if common.get('ksjzys_ll') != '' else None,
                 common.get('ksjzys_lkq') if common.get('ksjzys_lkq') != '' else None,
                 common.get('ksjzys_yd') if common.get('ksjzys_yd') != '' else None,
                 common.get('ksjzys_ys') if common.get('ksjzys_ys') != '' else None,
                 common.get('ksjzys_jh') if common.get('ksjzys_jh') != '' else None,
                 common.get('ksjzys_gmy') if common.get('ksjzys_gmy') != '' else None,
                 common.get('ksjzys_jl') if common.get('ksjzys_jl') != '' else None,
                 common.get('ksjzys_hjys') if common.get('ksjzys_hjys') != '' else None,
                 common.get('kslxx') if common.get('kslxx') != '' else None,
                 common.get('ksjjx') if common.get('ksjjx') != '' else None,
                 common.get('ksjjx_c') if common.get('ksjjx_c') != '' else None,
                 common.get('ksjjx_x') if common.get('ksjjx_x') != '' else None,
                 common.get('ksjjx_q') if common.get('ksjjx_q') != '' else None,
                 common.get('ksjjx_d') if common.get('ksjjx_d') != '' else None,
                 common.get('ksjjx_jjzh') if common.get('ksjjx_jjzh') != '' else None,
                 common.get('ksjccxsj') if common.get('ksjccxsj') != '' else None,
                 common.get('ksfffzsj') if common.get('ksfffzsj') != '' else None,
                 common.get('kt') if common.get('kt') != '' else None,
                 common.get('tzz') if common.get('tzz') != '' else None,
                 common.get('tl') if common.get('tl') != '' else None,
                 common.get('ts_h') if common.get('ts_h') != '' else None,
                 common.get('ts_b') if common.get('ts_b') != '' else None,
                 common.get('ts_hbxj') if common.get('ts_hbxj') != '' else None,
                 common.get('ts_tzdx') if common.get('ts_tzdx') != '' else None,
                 common.get('ktjccxsj') if common.get('ktjccxsj') != '' else None,
                 common.get('ktfffzsj') if common.get('ktfffzsj') != '' else None,
                 common.get('cxzz') if common.get('cxzz') != '' else None,
                 common.get('cxywfzpl') if common.get('cxywfzpl') != '' else None,
                 common.get('cxfzpl') if common.get('cxfzpl') != '' else None,
                 common.get('cxyfys_gm') if common.get('cxyfys_gm') != '' else None,
                 common.get('cxyfys_ll') if common.get('cxyfys_ll') != '' else None,
                 common.get('cxyfys_lkq') if common.get('cxyfys_lkq') != '' else None,
                 common.get('cxyfys_tw') if common.get('cxyfys_tw') != '' else None,
                 common.get('cxyfys_ys') if common.get('cxyfys_ys') != '' else None,
                 common.get('cxyfys_jh') if common.get('cxyfys_jh') != '' else None,
                 common.get('cxyfys_gmy') if common.get('cxyfys_gmy') != '' else None,
                 common.get('cxyfys_jl') if common.get('cxyfys_jl') != '' else None,
                 common.get('cxyfys_hjys') if common.get('cxyfys_hjys') != '' else None,
                 common.get('cxjjx') if common.get('cxjjx') != '' else None,
                 common.get('cxfffzsj') if common.get('cxfffzsj') != '' else None,
                 common.get('fl') if common.get('fl') != '' else None,
                 common.get('zhdhlx') if common.get('zhdhlx') != '' else None,
                 common.get('yxsr') if common.get('yxsr') != '' else None,
                 common.get('em') if common.get('em') != '' else None,
                 common.get('xm') if common.get('xm') != '' else None,
                 common.get('qd') if common.get('qd') != '' else None,
                 common.get('hr') if common.get('hr') != '' else None,
                 common.get('xp') if common.get('xp') != '' else None,
                 common.get('xp_lfwlzc') if common.get('xp_lfwlzc') != '' else None,
                 common.get('xp_ljxzk') if common.get('xp_ljxzk') != '' else None,
                 common.get('fbct') if common.get('fbct') != '' else None,
                 common.get('fbct_mxzqgy') if common.get('fbct_mxzqgy') != '' else None,
                 common.get('fbct_fqz') if common.get('fbct_fqz') != '' else None,
                 common.get('fbct_fdp') if common.get('fbct_fdp') != '' else None,
                 common.get('szqfev1fvc') if common.get('szqfev1fvc') != '' else None,
                 common.get('szqfev1yjz') if common.get('szqfev1yjz') != '' else None,
                 common.get('szqmmef') if common.get('szqmmef') != '' else None,
                 common.get('szhfev1fvc') if common.get('szhfev1fvc') != '' else None,
                 common.get('szhfev1yjz') if common.get('szhfev1yjz') != '' else None,
                 common.get('szhmmef') if common.get('szhmmef') != '' else None,
                 common.get('hqflspef') if common.get('hqflspef') != '' else None,
                 common.get('mxzsxfjb') if common.get('mxzsxfjb') != '' else None,
                 common.get('qlsxjb') if common.get('qlsxjb') != '' else None,
                 common.get('qlsxzb') if common.get('qlsxzb') != '' else None,
                 common.get('kss') if common.get('kss') != '' else None,
                 common.get('xrzqgkzyw') if common.get('xrzqgkzyw') != '' else None,
                 common.get('kfzqgkzyw') if common.get('kfzqgkzyw') != '' else None,
                 common.get('xrjs') if common.get('xrjs') != '' else None,
                 common.get('qsjs') if common.get('qsjs') != '' else None,
                 common.get('qty') if common.get('qty') != '' else None,
                 common.get('mlst') if common.get('mlst') != '' else None,
                 common.get('zyzl') if common.get('zyzl') != '' else None,
                 common.get('hxknpf') if common.get('hxknpf') != '' else None,
                 common.get('ydnl6mwt') if common.get('ydnl6mwt') != '' else None,
                 common.get('hxjbpf') if common.get('hxjbpf') != '' else None,
                 common.get('kcnyht') if common.get('kcnyht') != '' else None,
                 common.get('shzlpf') if common.get('shzlpf') != '' else None,
                 common.get('sx_pd') if common.get('sx_pd') != '' else None,
                 common.get('sx_sx') if common.get('sx_sx') != '' else None,
                 common.get('sx_jz') if common.get('sx_jz') != '' else None,
                 common.get('sx_wx') if common.get('sx_wx') != '' else None,
                 common.get('sx_ch') if common.get('sx_ch') != '' else None,
                 common.get('ss_d') if common.get('ss_d') != '' else None,
                 common.get('ss_h') if common.get('ss_h') != '' else None,
                 common.get('ss_z') if common.get('ss_z') != '' else None,
                 common.get('ss_a') if common.get('ss_a') != '' else None,
                 common.get('ss_ybyd') if common.get('ss_ybyd') != '' else None,
                 common.get('sst_lh') if common.get('sst_lh') != '' else None,
                 common.get('sst_jy') if common.get('sst_jy') != '' else None,
                 common.get('sst_l') if common.get('sst_l') != '' else None,
                 common.get('sst_lan') if common.get('sst_lan') != '' else None,
                 common.get('ts_bai') if common.get('ts_bai') != '' else None,
                 common.get('ts_hu') if common.get('ts_hu') != '' else None,
                 common.get('ts_he') if common.get('ts_he') != '' else None,
                 common.get('tz_bo') if common.get('tz_bo') != '' else None,
                 common.get('tz_h') if common.get('tz_h') != '' else None,
                 common.get('tz_n') if common.get('tz_n') != '' else None,
                 common.get('tz_ba') if common.get('tz_ba') != '' else None,
                 common.get('stgzcd') if common.get('stgzcd') != '' else None,
                 common.get('stl') if common.get('stl') != '' else None,
                 common.get('stwz') if common.get('stwz') != '' else None,
                 common.get('sxml') if common.get('sxml') != '' else None,
                 common.get('CREATE_TIME'), common.get('UPDATE_TIME'), 0))
            table_mzf_id = conn.insert_id()
            conn.commit()
            return table_mzf_id
        except Exception as ex:
            logging.error('[插入mzf异常]' + str(ex))
            print(common)
            conn.rollback()
            return None
    else:
        table_mzf_id = table_mzf_check[0]
        mzf_update_time = table_mzf_check[1]
        # if str(mzf_update_time) != str(common.get('UPDATE_TIME')):
        if 1 == 1:
            try:
                logging.info('mzf表刷新')
                cur.execute(
                    'UPDATE record_mzf2 SET gwys_xy = %s, gwys_hymxtdjb = %s, gwys_ymfzjzs = %s, gwys_jzzkqwryzdq = %s, '
                    'gwys_csjcfcgz = %s, gwys_ffhxhxdgr = %s, gwys_jzzqhhldq = %s, gwys_wssaqf = %s, gwys_yyzkjc = %s, '
                    'copdzd = %s, ywzl = %s, ywzl_dyxrzqgkzjsaba = %s, ywzl_dyxrzqgkzjsama = %s, '
                    'ywzl_dyxrzqgkzjlaba = %s, ywzl_dyxrzqgkzjlama = %s, ywzl_dyxrzqgkzjsfgl = %s, ywzl_xrjssfgl = %s, '
                    'ywzl_lhxrzjicslaba = %s, ywzl_lhxrzjicslabalama = %s, ywzl_lhxrzjlabalama = %s, '
                    'ywzl_lhxrzjsfgl = %s, ywzl_kfjssfgl = %s, ywzl_kfcjlsfgl = %s, ywzl_htlywsfgl = %s, fywzl = %s, '
                    'fywzl_jy = %s, fywzl_ywjzym = %s, fywzl_cjzlgym = %s, fywzl_cjzfyym = %s, fywzl_ywfkfzl = %s, '
                    'fywzl_ywjtyl = %s, fywzl_jtylsc = %s, fywzl_ywwchxj = %s, fywzl_wchxjsc = %s, '
                    'fywzl_dxzqgkzj = %s, fywzl_dxzqgkzjcs = %s, fywzl_jzjz = %s, fywzl_jzjzcs = %s, fywzl_zyzl = %s, '
                    'fywzl_zyzlcs = %s, hxxtjb = %s, hxxtjb_zqgkz = %s, hxxtjb_zqgxc = %s, hxxtjb_gmxby = %s, '
                    'hxxtjb_fbdy = %s, hxxtjb_fss = %s, hxxtjb_fjzxwh = %s, hxxtjb_fa = %s, hxxtjb_smhxztzhz = %s, '
                    'hxxtjb_hxsj = %s, hxxtjb_fjh = %s, hxxtjb_fdmgy = %s, hxxtjb_sc = %s, xxgjb = %s, xxgjb_gxy = %s, '
                    'xxgjb_gxb = %s, xxgjb_jxxgnbq = %s, xxgjb_fyxxzb = %s, xxgjb_mxxgnbq = %s, xxgjb_fc = %s, '
                    'xxgjb_yszcdzz = %s, xxgjb_sxzb = %s, xxgjb_sc = %s, nfmxtjb = %s, nfmxtjb_tnb = %s, '
                    'nfmxtjb_gzss = %s, nfmxtjb_sc = %s, xhxtjb = %s, xhxtjb_wsgfl = %s, xhxtjb_bm = %s, '
                    'xhxtjb_yzxcb = %s, xhxtjb_gyh = %s, xhxtjb_sc = %s, qtxtjb = %s, qtxtjb_yyz = %s, '
                    'qtxtjb_qtbwzl = %s, qtxtjb_sgnbq = %s, qtxtjb_gljrza = %s, qtxtjb_xkjx = %s, '
                    'qtxtjb_swywygms = %s, qtxtjb_xbss = %s, xypc = %s, yl = %s, xycsnl = %s, xyjsnl = %s, '
                    'pjmtxys = %s, xysc = %s, jynl = %s, jysc = %s, esyjc = %s, esyjc_e14 = %s, esyjc_a14 = %s, '
                    'esyjcsj = %s, swrlzf = %s, mnsnqnyw = %s, snzfqncxsj = %s, snzfqnsyqj = %s, zfqnfhcs = %s, '
                    'zfqnfhcs_kz = %s, zfqnfhcs_fdmj = %s, zfqnfhcs_yyj = %s, zfqnfhcs_pqs = %s, zfqnfhpc = %s, '
                    'qtfszf = %s, qtfszf_trq = %s, qtfszf_mb = %s, qtfszf_yhq = %s, mnsnzfyw = %s, snzfcxsj = %s, '
                    'snzfsyqj = %s, snzffhcs = %s, snzffhcs_kz = %s, snzffhcs_fdmj = %s, snzffhcs_yyj = %s, '
                    'snzffhcs_pqs = %s, snzffhcsqt = %s, snzffhpc = %s, fcjc = %s, mnfcjcys = %s, fcjccxsj = %s, '
                    'fcjcfhcs = %s, fcjcfhcs_kz = %s, fcjcfhcs_fdmj = %s, fcjcfhcs_fhf = %s, fcjcfhcs_fctk = %s, '
                    'fcjcfhcs_qxz = %s, fcjcfhpc = %s, ydqtjc = %s, mnydqtjcys = %s, ydqtjccxsj = %s, ydqtjcfhcs = %s, '
                    'ydqtjcfhcs_kz = %s, ydqtjcfhcs_fdmj = %s, ydqtjcfhcs_fhf = %s, ydqtjcfhcs_fctk = %s, '
                    'ydqtjcfhcs_qxz = %s, ydqtjcfhpc = %s, zjskljc = %s, mnzjskljcys = %s, zjskljccxsj = %s, '
                    'zjskljcfhcs = %s, zjskljcfhcs_kz = %s, zjskljcfhcs_fdmj = %s, zjskljcfhcs_fhf = %s, '
                    'zjskljcfhcs_fctk = %s, zjskljcfhcs_qxz = %s, zjskljcfhpc = %s, ydkljc = %s, mnydkljcys = %s, '
                    'ydkljccxsj = %s, ydkljcfhcs = %s, ydkljcfhcs_kz = %s, ydkljcfhcs_fdmz = %s, ydkljcfhcs_fhf = %s, '
                    'ydkljcfhcs_fctk = %s, ydkljcfhcs_qxz = %s, ydkljcfhpc = %s, cakljc = %s, mncakljcys = %s, '
                    'cakljccxsj = %s, cakljcfhcs = %s, cakljcfhcs_kz = %s, cakljcfhcs_fdmz = %s, cakljcfhcs_fhf = %s, '
                    'cakljcfhcs_fctk = %s, cakljcfhcs_qxz = %s, cakljcfhpc = %s, ynffmxqdjb = %s, hxdjbjzs = %s, '
                    'ynnhxdgr = %s, ynnhxdgrcs = %s, zg = %s, psyg = %s, ynpjgmcs = %s, bbzz_ht = %s, bbzz_qt = %s, '
                    'bbzz_bs = %s, bbzz_hxbm = %s, ks = %s, kscd = %s, kssx = %s, ksjzys = %s, ksjzys_gm = %s, '
                    'ksjzys_tyhxxtyw = %s, ksjzys_ll = %s, ksjzys_lkq = %s, ksjzys_yd = %s, ksjzys_ys = %s, '
                    'ksjzys_jh = %s, ksjzys_gmy = %s, ksjzys_jl = %s, ksjzys_hjys = %s, kslxx = %s, ksjjx = %s, '
                    'ksjjx_c = %s, ksjjx_x = %s, ksjjx_q = %s, ksjjx_d = %s, ksjjx_jjzh = %s, ksjccxsj = %s, '
                    'ksfffzsj = %s, kt = %s, tzz = %s, tl = %s, ts_h = %s, ts_b = %s, ts_hbxj = %s, ts_tzdx = %s, '
                    'ktjccxsj = %s, ktfffzsj = %s, cxzz = %s, cxywfzpl = %s, cxfzpl = %s, cxyfys_gm = %s, '
                    'cxyfys_ll = %s, cxyfys_lkq = %s, cxyfys_tw = %s, cxyfys_ys = %s, cxyfys_jh = %s, cxyfys_gmy = %s, '
                    'cxyfys_jl = %s, cxyfys_hjys = %s, cxjjx = %s, cxfffzsj = %s, fl = %s, zhdhlx = %s, yxsr = %s, '
                    'em = %s, xm = %s, qd = %s, hr = %s, xp = %s, xp_lfwlzc = %s, xp_ljxzk = %s, fbct = %s, '
                    'fbct_mxzqgy = %s, fbct_fqz = %s, fbct_fdp = %s, szqfev1fvc = %s, szqfev1yjz = %s, szqmmef = %s, '
                    'szhfev1fvc = %s, szhfev1yjz = %s, szhmmef = %s, hqflspef = %s, mxzsxfjb = %s, qlsxjb = %s, '
                    'qlsxzb = %s, kss = %s, xrzqgkzyw = %s, kfzqgkzyw = %s, xrjs = %s, qsjs = %s, qty = %s, mlst = %s, '
                    'zyzl = %s, hxknpf = %s, ydnl6mwt = %s, hxjbpf = %s, kcnyht = %s, shzlpf = %s, sx_pd = %s, '
                    'sx_sx = %s, sx_jz = %s, sx_wx = %s, sx_ch = %s, ss_d = %s, ss_h = %s, ss_z = %s, ss_a = %s, '
                    'ss_ybyd = %s, sst_lh = %s, sst_jy = %s, sst_l = %s, sst_lan = %s, ts_bai = %s, ts_hu = %s, '
                    'ts_he = %s, tz_bo = %s, tz_h = %s, tz_n = %s, tz_ba = %s, stgzcd = %s, stl = %s, stwz = %s, '
                    'sxml = %s, CREATE_TIME = %s, UPDATE_TIME = %s WHERE ID = %s;',
                    (
                        common.get('gwys_xy') if common.get('gwys_xy') != '' else None,
                        common.get('gwys_hymxtdjb') if common.get('gwys_hymxtdjb') != '' else None,
                        common.get('gwys_ymfzjzs') if common.get('gwys_ymfzjzs') != '' else None,
                        common.get('gwys_jzzkqwryzdq') if common.get('gwys_jzzkqwryzdq') != '' else None,
                        common.get('gwys_csjcfcgz') if common.get('gwys_csjcfcgz') != '' else None,
                        common.get('gwys_ffhxhxdgr') if common.get('gwys_ffhxhxdgr') != '' else None,
                        common.get('gwys_jzzqhhldq') if common.get('gwys_jzzqhhldq') != '' else None,
                        common.get('gwys_wssaqf') if common.get('gwys_wssaqf') != '' else None,
                        common.get('gwys_yyzkjc') if common.get('gwys_yyzkjc') != '' else None,
                        common.get('copdzd') if common.get('copdzd') != '' else None,
                        common.get('ywzl') if common.get('ywzl') != '' else None,
                        common.get('ywzl_dyxrzqgkzjsaba') if common.get('ywzl_dyxrzqgkzjsaba') != '' else None,
                        common.get('ywzl_dyxrzqgkzjsana') if common.get('ywzl_dyxrzqgkzjsana') != '' else None,
                        common.get('ywzl_dyxrzqgkzjlaba') if common.get('ywzl_dyxrzqgkzjlaba') != '' else None,
                        common.get('ywzl_dyxrzqgkzjlama') if common.get('ywzl_dyxrzqgkzjlama') != '' else None,
                        common.get('ywzl_dyxrzqgkzjsfgl') if common.get('ywzl_dyxrzqgkzjsfgl') != '' else None,
                        common.get('ywzl_xrjssfgl') if common.get('ywzl_xrjssfgl') != '' else None,
                        common.get('ywzl_lhxrzjicslaba') if common.get('ywzl_lhxrzjicslaba') != '' else None,
                        common.get('ywzl_lhxrzjicslabalama') if common.get('ywzl_lhxrzjicslabalama') != '' else None,
                        common.get('ywzl_lhxrzjlabalama') if common.get('ywzl_lhxrzjlabalama') != '' else None,
                        common.get('ywzl_lhxrzjsfgl') if common.get('ywzl_lhxrzjsfgl') != '' else None,
                        common.get('ywzl_kfjssfgl') if common.get('ywzl_kfjssfgl') != '' else None,
                        common.get('ywzl_kfcjlsfgl') if common.get('ywzl_kfcjlsfgl') != '' else None,
                        common.get('ywzl_htlywsfgl') if common.get('ywzl_htlywsfgl') != '' else None,
                        common.get('fywzl') if common.get('fywzl') != '' else None,
                        common.get('fywzl_jy') if common.get('fywzl_jy') != '' else None,
                        common.get('fywzl_ywjzym') if common.get('fywzl_ywjzym') != '' else None,
                        common.get('fywzl_cjzlgym') if common.get('fywzl_cjzlgym') != '' else None,
                        common.get('fywzl_cjzfyym') if common.get('fywzl_cjzfyym') != '' else None,
                        common.get('fywzl_ywfkfzl') if common.get('fywzl_ywfkfzl') != '' else None,
                        common.get('fywzl_ywjtyl') if common.get('fywzl_ywjtyl') != '' else None,
                        common.get('fywzl_jtylsc') if common.get('fywzl_jtylsc') != '' else None,
                        common.get('fywzl_ywwchxj') if common.get('fywzl_ywwchxj') != '' else None,
                        common.get('fywzl_wchxjsc') if common.get('fywzl_wchxjsc') != '' else None,
                        common.get('fywzl_dxzqgkzj') if common.get('fywzl_dxzqgkzj') != '' else None,
                        common.get('fywzl_dxzqgkzjcs') if common.get('fywzl_dxzqgkzjcs') != '' else None,
                        common.get('fywzl_jzjz') if common.get('fywzl_jzjz') != '' else None,
                        common.get('fywzl_jzjzcs') if common.get('fywzl_jzjzcs') != '' else None,
                        common.get('fywzl_zyzl') if common.get('fywzl_zyzl') != '' else None,
                        common.get('fywzl_zyzlcs') if common.get('fywzl_zyzlcs') != '' else None,
                        common.get('hxxtjb') if common.get('hxxtjb') != '' else None,
                        common.get('hxxtjb_zqgkz') if common.get('hxxtjb_zqgkz') != '' else None,
                        common.get('hxxtjb_zqgxc') if common.get('hxxtjb_zqgxc') != '' else None,
                        common.get('hxxtjb_gmxby') if common.get('hxxtjb_gmxby') != '' else None,
                        common.get('hxxtjb_fbdy') if common.get('hxxtjb_fbdy') != '' else None,
                        common.get('hxxtjb_fss') if common.get('hxxtjb_fss') != '' else None,
                        common.get('hxxtjb_fjzxwh') if common.get('hxxtjb_fjzxwh') != '' else None,
                        common.get('hxxtjb_fa') if common.get('hxxtjb_fa') != '' else None,
                        common.get('hxxtjb_smhxztzhz') if common.get('hxxtjb_smhxztzhz') != '' else None,
                        common.get('hxxtjb_hxsj') if common.get('hxxtjb_hxsj') != '' else None,
                        common.get('hxxtjb_fjh') if common.get('hxxtjb_fjh') != '' else None,
                        common.get('hxxtjb_fdmgy') if common.get('hxxtjb_fdmgy') != '' else None,
                        common.get('hxxtjb_sc') if common.get('hxxtjb_sc') != '' else None,
                        common.get('xxgjb') if common.get('xxgjb') != '' else None,
                        common.get('xxgjb_gxy') if common.get('xxgjb_gxy') != '' else None,
                        common.get('xxgjb_gxb') if common.get('xxgjb_gxb') != '' else None,
                        common.get('xxgjb_jxxgnbq') if common.get('xxgjb_jxxgnbq') != '' else None,
                        common.get('xxgjb_fyxxzb') if common.get('xxgjb_fyxxzb') != '' else None,
                        common.get('xxgjb_mxxgnbq') if common.get('xxgjb_mxxgnbq') != '' else None,
                        common.get('xxgjb_fc') if common.get('xxgjb_fc') != '' else None,
                        common.get('xxgjb_yszcdzz') if common.get('xxgjb_yszcdzz') != '' else None,
                        common.get('xxgjb_sxzb') if common.get('xxgjb_sxzb') != '' else None,
                        common.get('xxgjb_sc') if common.get('xxgjb_sc') != '' else None,
                        common.get('nfmxtjb') if common.get('nfmxtjb') != '' else None,
                        common.get('nfmxtjb_tnb') if common.get('nfmxtjb_tnb') != '' else None,
                        common.get('nfmxtjb_gzss') if common.get('nfmxtjb_gzss') != '' else None,
                        common.get('nfmxtjb_sc') if common.get('nfmxtjb_sc') != '' else None,
                        common.get('xhxtjb') if common.get('xhxtjb') != '' else None,
                        common.get('xhxtjb_wsgfl') if common.get('xhxtjb_wsgfl') != '' else None,
                        common.get('xhxtjb_bm') if common.get('xhxtjb_bm') != '' else None,
                        common.get('xhxtjb_yzxcb') if common.get('xhxtjb_yzxcb') != '' else None,
                        common.get('xhxtjb_gyh') if common.get('xhxtjb_gyh') != '' else None,
                        common.get('xhxtjb_sc') if common.get('xhxtjb_sc') != '' else None,
                        common.get('qtxtjb') if common.get('qtxtjb') != '' else None,
                        common.get('qtxtjb_yyz') if common.get('qtxtjb_yyz') != '' else None,
                        common.get('qtxtjb_qtbwzl') if common.get('qtxtjb_qtbwzl') != '' else None,
                        common.get('qtxtjb_sgnbq') if common.get('qtxtjb_sgnbq') != '' else None,
                        common.get('qtxtjb_gljrza') if common.get('qtxtjb_gljrza') != '' else None,
                        common.get('qtxtjb_xkjx') if common.get('qtxtjb_xkjx') != '' else None,
                        common.get('qtxtjb_swywygms') if common.get('qtxtjb_swywygms') != '' else None,
                        common.get('qtxtjb_xbss') if common.get('qtxtjb_xbss') != '' else None,
                        common.get('xypc') if common.get('xypc') != '' else None,
                        common.get('yl') if common.get('yl') != '' else None,
                        common.get('xycsnl') if common.get('xycsnl') != '' else None,
                        common.get('xyjsnl') if common.get('xyjsnl') != '' else None,
                        common.get('pjmtxys') if common.get('pjmtxys') != '' else None,
                        common.get('xysc') if common.get('xysc') != '' else None,
                        common.get('jynl') if common.get('jynl') != '' else None,
                        common.get('jysc') if common.get('jysc') != '' else None,
                        common.get('esyjc') if common.get('esyjc') != '' else None,
                        common.get('esyjc_e14') if common.get('esyjc_e14') != '' else None,
                        common.get('esyjc_a14') if common.get('esyjc_a14') != '' else None,
                        common.get('esyjcsj') if common.get('esyjcsj') != '' else None,
                        common.get('swrlzf') if common.get('swrlzf') != '' else None,
                        common.get('mnsnqnyw') if common.get('mnsnqnyw') != '' else None,
                        common.get('snzfqncxsj') if common.get('snzfqncxsj') != '' else None,
                        common.get('snzfqnsyqj') if common.get('snzfqnsyqj') != '' else None,
                        common.get('zfqnfhcs') if common.get('zfqnfhcs') != '' else None,
                        common.get('zfqnfhcs_kz') if common.get('zfqnfhcs_kz') != '' else None,
                        common.get('zfqnfhcs_fdmj') if common.get('zfqnfhcs_fdmj') != '' else None,
                        common.get('zfqnfhcs_yyj') if common.get('zfqnfhcs_yyj') != '' else None,
                        common.get('zfqnfhcs_pqs') if common.get('zfqnfhcs_pqs') != '' else None,
                        common.get('zfqnfhpc') if common.get('zfqnfhpc') != '' else None,
                        common.get('qtfszf') if common.get('qtfszf') != '' else None,
                        common.get('qtfszf_trq') if common.get('qtfszf_trq') != '' else None,
                        common.get('qtfszf_mb') if common.get('qtfszf_mb') != '' else None,
                        common.get('qtfszf_yhq') if common.get('qtfszf_yhq') != '' else None,
                        common.get('mnsnzfyw') if common.get('mnsnzfyw') != '' else None,
                        common.get('snzfcxsj') if common.get('snzfcxsj') != '' else None,
                        common.get('snzfsyqj') if common.get('snzfsyqj') != '' else None,
                        common.get('snzffhcs') if common.get('snzffhcs') != '' else None,
                        common.get('snzffhcs_kz') if common.get('snzffhcs_kz') != '' else None,
                        common.get('snzffhcs_fdmj') if common.get('snzffhcs_fdmj') != '' else None,
                        common.get('snzffhcs_yyj') if common.get('snzffhcs_yyj') != '' else None,
                        common.get('snzffhcs_pqs') if common.get('snzffhcs_pqs') != '' else None,
                        common.get('snzffhcsqt') if common.get('snzffhcsqt') != '' else None,
                        common.get('snzffhpc') if common.get('snzffhpc') != '' else None,
                        common.get('fcjc') if common.get('fcjc') != '' else None,
                        common.get('mnfcjcys') if common.get('mnfcjcys') != '' else None,
                        common.get('fcjccxsj') if common.get('fcjccxsj') != '' else None,
                        common.get('fcjcfhcs') if common.get('fcjcfhcs') != '' else None,
                        common.get('fcjcfhcs_kz') if common.get('fcjcfhcs_kz') != '' else None,
                        common.get('fcjcfhcs_fdmj') if common.get('fcjcfhcs_fdmj') != '' else None,
                        common.get('fcjcfhcs_fhf') if common.get('fcjcfhcs_fhf') != '' else None,
                        common.get('fcjcfhcs_fctk') if common.get('fcjcfhcs_fctk') != '' else None,
                        common.get('fcjcfhcs_qxz') if common.get('fcjcfhcs_qxz') != '' else None,
                        common.get('fcjcfhpc') if common.get('fcjcfhpc') != '' else None,
                        common.get('ydqtjc') if common.get('ydqtjc') != '' else None,
                        common.get('mnydqtjcys') if common.get('mnydqtjcys') != '' else None,
                        common.get('ydqtjccxsj') if common.get('ydqtjccxsj') != '' else None,
                        common.get('ydqtjcfhcs') if common.get('ydqtjcfhcs') != '' else None,
                        common.get('ydqtjcfhcs_kz') if common.get('ydqtjcfhcs_kz') != '' else None,
                        common.get('ydqtjcfhcs_fdmj') if common.get('ydqtjcfhcs_fdmj') != '' else None,
                        common.get('ydqtjcfhcs_fhf') if common.get('ydqtjcfhcs_fhf') != '' else None,
                        common.get('ydqtjcfhcs_fctk') if common.get('ydqtjcfhcs_fctk') != '' else None,
                        common.get('ydqtjcfhcs_qxz') if common.get('ydqtjcfhcs_qxz') != '' else None,
                        common.get('ydqtjcfhpc') if common.get('ydqtjcfhpc') != '' else None,
                        common.get('zjskljc') if common.get('zjskljc') != '' else None,
                        common.get('mnzjskljcys') if common.get('mnzjskljcys') != '' else None,
                        common.get('zjskljccxsj') if common.get('zjskljccxsj') != '' else None,
                        common.get('zjskljcfhcs') if common.get('zjskljcfhcs') != '' else None,
                        common.get('zjskljcfhcs_kz') if common.get('zjskljcfhcs_kz') != '' else None,
                        common.get('zjskljcfhcs_fdmj') if common.get('zjskljcfhcs_fdmj') != '' else None,
                        common.get('zjskljcfhcs_fhf') if common.get('zjskljcfhcs_fhf') != '' else None,
                        common.get('zjskljcfhcs_fctk') if common.get('zjskljcfhcs_fctk') != '' else None,
                        common.get('zjskljcfhcs_qxz') if common.get('zjskljcfhcs_qxz') != '' else None,
                        common.get('zjskljcfhpc') if common.get('zjskljcfhpc') != '' else None,
                        common.get('ydkljc') if common.get('ydkljc') != '' else None,
                        common.get('mnydkljcys') if common.get('mnydkljcys') != '' else None,
                        common.get('ydkljccxsj') if common.get('ydkljccxsj') != '' else None,
                        common.get('ydkljcfhcs') if common.get('ydkljcfhcs') != '' else None,
                        common.get('ydkljcfhcs_kz') if common.get('ydkljcfhcs_kz') != '' else None,
                        common.get('ydkljcfhcs_fdmz') if common.get('ydkljcfhcs_fdmz') != '' else None,
                        common.get('ydkljcfhcs_fhf') if common.get('ydkljcfhcs_fhf') != '' else None,
                        common.get('ydkljcfhcs_fctk') if common.get('ydkljcfhcs_fctk') != '' else None,
                        common.get('ydkljcfhcs_qxz') if common.get('ydkljcfhcs_qxz') != '' else None,
                        common.get('ydkljcfhpc') if common.get('ydkljcfhpc') != '' else None,
                        common.get('cakljc') if common.get('cakljc') != '' else None,
                        common.get('mncakljcys') if common.get('mncakljcys') != '' else None,
                        common.get('cakljccxsj') if common.get('cakljccxsj') != '' else None,
                        common.get('cakljcfhcs') if common.get('cakljcfhcs') != '' else None,
                        common.get('cakljcfhcs_kz') if common.get('cakljcfhcs_kz') != '' else None,
                        common.get('cakljcfhcs_fdmz') if common.get('cakljcfhcs_fdmz') != '' else None,
                        common.get('cakljcfhcs_fhf') if common.get('cakljcfhcs_fhf') != '' else None,
                        common.get('cakljcfhcs_fctk') if common.get('cakljcfhcs_fctk') != '' else None,
                        common.get('cakljcfhcs_qxz') if common.get('cakljcfhcs_qxz') != '' else None,
                        common.get('cakljcfhpc') if common.get('cakljcfhpc') != '' else None,
                        common.get('ynffmxqdjb') if common.get('ynffmxqdjb') != '' else None,
                        common.get('hxdjbjzs') if common.get('hxdjbjzs') != '' else None,
                        common.get('ynnhxdgr') if common.get('ynnhxdgr') != '' else None,
                        common.get('ynnhxdgrcs') if common.get('ynnhxdgrcs') != '' else None,
                        common.get('zg') if common.get('zg') != '' else None,
                        common.get('psyg') if common.get('psyg') != '' else None,
                        common.get('ynpjgmcs') if common.get('ynpjgmcs') != '' else None,
                        common.get('bbzz_ht') if common.get('bbzz_ht') != '' else None,
                        common.get('bbzz_qt') if common.get('bbzz_qt') != '' else None,
                        common.get('bbzz_bs') if common.get('bbzz_bs') != '' else None,
                        common.get('bbzz_hxbm') if common.get('bbzz_hxbm') != '' else None,
                        common.get('ks') if common.get('ks') != '' else None,
                        common.get('kscd') if common.get('kscd') != '' else None,
                        common.get('kssx') if common.get('kssx') != '' else None,
                        common.get('ksjzys') if common.get('ksjzys') != '' else None,
                        common.get('ksjzys_gm') if common.get('ksjzys_gm') != '' else None,
                        common.get('ksjzys_tyhxxtyw') if common.get('ksjzys_tyhxxtyw') != '' else None,
                        common.get('ksjzys_ll') if common.get('ksjzys_ll') != '' else None,
                        common.get('ksjzys_lkq') if common.get('ksjzys_lkq') != '' else None,
                        common.get('ksjzys_yd') if common.get('ksjzys_yd') != '' else None,
                        common.get('ksjzys_ys') if common.get('ksjzys_ys') != '' else None,
                        common.get('ksjzys_jh') if common.get('ksjzys_jh') != '' else None,
                        common.get('ksjzys_gmy') if common.get('ksjzys_gmy') != '' else None,
                        common.get('ksjzys_jl') if common.get('ksjzys_jl') != '' else None,
                        common.get('ksjzys_hjys') if common.get('ksjzys_hjys') != '' else None,
                        common.get('kslxx') if common.get('kslxx') != '' else None,
                        common.get('ksjjx') if common.get('ksjjx') != '' else None,
                        common.get('ksjjx_c') if common.get('ksjjx_c') != '' else None,
                        common.get('ksjjx_x') if common.get('ksjjx_x') != '' else None,
                        common.get('ksjjx_q') if common.get('ksjjx_q') != '' else None,
                        common.get('ksjjx_d') if common.get('ksjjx_d') != '' else None,
                        common.get('ksjjx_jjzh') if common.get('ksjjx_jjzh') != '' else None,
                        common.get('ksjccxsj') if common.get('ksjccxsj') != '' else None,
                        common.get('ksfffzsj') if common.get('ksfffzsj') != '' else None,
                        common.get('kt') if common.get('kt') != '' else None,
                        common.get('tzz') if common.get('tzz') != '' else None,
                        common.get('tl') if common.get('tl') != '' else None,
                        common.get('ts_h') if common.get('ts_h') != '' else None,
                        common.get('ts_b') if common.get('ts_b') != '' else None,
                        common.get('ts_hbxj') if common.get('ts_hbxj') != '' else None,
                        common.get('ts_tzdx') if common.get('ts_tzdx') != '' else None,
                        common.get('ktjccxsj') if common.get('ktjccxsj') != '' else None,
                        common.get('ktfffzsj') if common.get('ktfffzsj') != '' else None,
                        common.get('cxzz') if common.get('cxzz') != '' else None,
                        common.get('cxywfzpl') if common.get('cxywfzpl') != '' else None,
                        common.get('cxfzpl') if common.get('cxfzpl') != '' else None,
                        common.get('cxyfys_gm') if common.get('cxyfys_gm') != '' else None,
                        common.get('cxyfys_ll') if common.get('cxyfys_ll') != '' else None,
                        common.get('cxyfys_lkq') if common.get('cxyfys_lkq') != '' else None,
                        common.get('cxyfys_tw') if common.get('cxyfys_tw') != '' else None,
                        common.get('cxyfys_ys') if common.get('cxyfys_ys') != '' else None,
                        common.get('cxyfys_jh') if common.get('cxyfys_jh') != '' else None,
                        common.get('cxyfys_gmy') if common.get('cxyfys_gmy') != '' else None,
                        common.get('cxyfys_jl') if common.get('cxyfys_jl') != '' else None,
                        common.get('cxyfys_hjys') if common.get('cxyfys_hjys') != '' else None,
                        common.get('cxjjx') if common.get('cxjjx') != '' else None,
                        common.get('cxfffzsj') if common.get('cxfffzsj') != '' else None,
                        common.get('fl') if common.get('fl') != '' else None,
                        common.get('zhdhlx') if common.get('zhdhlx') != '' else None,
                        common.get('yxsr') if common.get('yxsr') != '' else None,
                        common.get('em') if common.get('em') != '' else None,
                        common.get('xm') if common.get('xm') != '' else None,
                        common.get('qd') if common.get('qd') != '' else None,
                        common.get('hr') if common.get('hr') != '' else None,
                        common.get('xp') if common.get('xp') != '' else None,
                        common.get('xp_lfwlzc') if common.get('xp_lfwlzc') != '' else None,
                        common.get('xp_ljxzk') if common.get('xp_ljxzk') != '' else None,
                        common.get('fbct') if common.get('fbct') != '' else None,
                        common.get('fbct_mxzqgy') if common.get('fbct_mxzqgy') != '' else None,
                        common.get('fbct_fqz') if common.get('fbct_fqz') != '' else None,
                        common.get('fbct_fdp') if common.get('fbct_fdp') != '' else None,
                        common.get('szqfev1fvc') if common.get('szqfev1fvc') != '' else None,
                        common.get('szqfev1yjz') if common.get('szqfev1yjz') != '' else None,
                        common.get('szqmmef') if common.get('szqmmef') != '' else None,
                        common.get('szhfev1fvc') if common.get('szhfev1fvc') != '' else None,
                        common.get('szhfev1yjz') if common.get('szhfev1yjz') != '' else None,
                        common.get('szhmmef') if common.get('szhmmef') != '' else None,
                        common.get('hqflspef') if common.get('hqflspef') != '' else None,
                        common.get('mxzsxfjb') if common.get('mxzsxfjb') != '' else None,
                        common.get('qlsxjb') if common.get('qlsxjb') != '' else None,
                        common.get('qlsxzb') if common.get('qlsxzb') != '' else None,
                        common.get('kss') if common.get('kss') != '' else None,
                        common.get('xrzqgkzyw') if common.get('xrzqgkzyw') != '' else None,
                        common.get('kfzqgkzyw') if common.get('kfzqgkzyw') != '' else None,
                        common.get('xrjs') if common.get('xrjs') != '' else None,
                        common.get('qsjs') if common.get('qsjs') != '' else None,
                        common.get('qty') if common.get('qty') != '' else None,
                        common.get('mlst') if common.get('mlst') != '' else None,
                        common.get('zyzl') if common.get('zyzl') != '' else None,
                        common.get('hxknpf') if common.get('hxknpf') != '' else None,
                        common.get('ydnl6mwt') if common.get('ydnl6mwt') != '' else None,
                        common.get('hxjbpf') if common.get('hxjbpf') != '' else None,
                        common.get('kcnyht') if common.get('kcnyht') != '' else None,
                        common.get('shzlpf') if common.get('shzlpf') != '' else None,
                        common.get('sx_pd') if common.get('sx_pd') != '' else None,
                        common.get('sx_sx') if common.get('sx_sx') != '' else None,
                        common.get('sx_jz') if common.get('sx_jz') != '' else None,
                        common.get('sx_wx') if common.get('sx_wx') != '' else None,
                        common.get('sx_ch') if common.get('sx_ch') != '' else None,
                        common.get('ss_d') if common.get('ss_d') != '' else None,
                        common.get('ss_h') if common.get('ss_h') != '' else None,
                        common.get('ss_z') if common.get('ss_z') != '' else None,
                        common.get('ss_a') if common.get('ss_a') != '' else None,
                        common.get('ss_ybyd') if common.get('ss_ybyd') != '' else None,
                        common.get('sst_lh') if common.get('sst_lh') != '' else None,
                        common.get('sst_jy') if common.get('sst_jy') != '' else None,
                        common.get('sst_l') if common.get('sst_l') != '' else None,
                        common.get('sst_lan') if common.get('sst_lan') != '' else None,
                        common.get('ts_bai') if common.get('ts_bai') != '' else None,
                        common.get('ts_hu') if common.get('ts_hu') != '' else None,
                        common.get('ts_he') if common.get('ts_he') != '' else None,
                        common.get('tz_bo') if common.get('tz_bo') != '' else None,
                        common.get('tz_h') if common.get('tz_h') != '' else None,
                        common.get('tz_n') if common.get('tz_n') != '' else None,
                        common.get('tz_ba') if common.get('tz_ba') != '' else None,
                        common.get('stgzcd') if common.get('stgzcd') != '' else None,
                        common.get('stl') if common.get('stl') != '' else None,
                        common.get('stwz') if common.get('stwz') != '' else None,
                        common.get('sxml') if common.get('sxml') != '' else None,
                        common.get('CREATE_TIME'), common.get('UPDATE_TIME'), table_mzf_id))
                conn.commit()
                return table_mzf_id
            except Exception as ex:
                logging.error('[更新mzf异常]' + str(ex))
                conn.rollback()
                return None
        else:
            logging.info('mzf数据为最新版本，无需更新')
            return table_mzf_id


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
            # if record.get('user').get('name') == '翁文梅':
            # print(record)
            user = record.get('user')
            table_patient_id = user_to_db(cur=cur, user=user)
            if table_patient_id is not None:
                common = record.get('common')
                if len(common) == 452:
                    table_common_id = common_to_db(cur=cur, common=common, table_patient_id=table_patient_id)
                    if table_common_id is not None:
                        table_mzf_id = mzf_to_db(cur=cur, common=common, table_common_id=table_common_id)
                        if table_mzf_id is None:
                            flag = False
                            break
                    else:
                        flag = False
                        break
                else:
                    logging.warning('[数据长度异常，非452]该数据长度为：' + str(len(common)))
                    print(record)
            else:
                flag = False
                break
    cur.close()
    conn.close()
    if flag:
        logging.info('数据同步完成，同步完成时间：' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    else:
        logging.error('数据同步失败，请检查数据')
    print('程序10秒后自动退出，可手动关闭窗口')
    time.sleep(10)
