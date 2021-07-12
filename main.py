#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymysql
import time
import logging
import configparser
import datetime
import requests
import hashlib

# 第一步，创建一个logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Log等级总开关

# 第二步，创建一个handler，用于写入日志文件
logfile = 'logs/logs.txt'
fh = logging.FileHandler(logfile, mode='a', encoding='utf-8')
fh.setLevel(logging.ERROR)  # 用于写到file的等级开关

# 第三步，再创建一个handler,用于输出到控制台
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)  # 输出到console的log等级的开关

# 第四步，定义handler的输出格式
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# 第五步，将logger添加到handler里面
logger.addHandler(fh)
logger.addHandler(ch)


def get_data():
    config = configparser.RawConfigParser()
    config.read('configs/request.ini')
    t = time.time()
    timestamp = str(int(t))
    md5_time = hashlib.md5(timestamp.encode("utf8")).hexdigest()
    key = config.get('key', 'key')
    auth_code = hashlib.md5((md5_time + key).encode("utf8")).hexdigest()
    r = requests.get(url=config.get('url', 'url'),
                     headers={'timestamp': str(timestamp), 'authCode': str(auth_code)})
    print('接口返回结果：' + r.text)


if __name__ == '__main__':
    get_data()
