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
        logging.info('受试者' + str(hzbh) + '基本信息刷新')
        table_patient_id = table_patient_check[0]
        return table_patient_id


if __name__ == '__main__':
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
            table_patient_id = user_to_db(cur, user)
            if table_patient_id is not None:
                print()
    cur.close()
    conn.close()
    logging.info('数据同步完成，同步时间：' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
