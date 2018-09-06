# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 10:22:42 2017

@author: Qian
"""

import os
import json
import time
import logging
import requests
from my_modules import mysqlconn
from tophatterproductapi import TophatterProductAPI as prodapi

# 日志
logger = logging.getLogger("Tophater home_page.py")
logger.setLevel(logging.ERROR)
# 日志格式
fmt = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
# 文件日志, DEBUG级别
today = time.strftime("%Y%m%d")
log_dir = '/root/log/' + today
if not os.path.exists(log_dir):
    os.mkdir(log_dir)
fh = logging.FileHandler(os.path.join(log_dir, 'tp_home_page.log'))
fh.setLevel(logging.ERROR)
fh.setFormatter(fmt)
# 将相应的handler添加在logger对象中
logger.addHandler(fh)


def __update_sql(dict, table_name, primary_key):
    """返回sql语句用于更新数据库"""
    keys = []
    data = []
    for i in dict:
        keys.append(i)
        data.append(dict[i])
    # update_sql_string
    string2 = "update " + table_name + " set "
    j = 0
    for i in range(len(keys)):
        if keys[i] not in primary_key:
            if j:
                string2 += ", "
            string2 += str(keys[i]) + "='" + str(data[i]) + "'"
            j = 1
    string2 += " where "
    for i in range(len(primary_key)):
        if i:
            string2 += " and "
        string2 += str(primary_key[i]) + "='" + str(dict[primary_key[i]]) + "'"
    if string2.__contains__("'curdate()'"):
        string2 = string2.replace("'curdate()'", "curdate()")
    return string2


def update2db(conn, dict, table_name, primary_key):
    """将prod的数据更新到数据库，先试用insert来更新，如果报错pymysql.err.IntegrityError，改用update来更新"""
    cur = conn.cursor()
    sql = __update_sql(dict, table_name, primary_key)
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def get_auct_data():
    url = 'https://ps.pndsn.com/v2/subscribe/sub-c-4cb8cb8e-9e32-11e7-a3e4-2e10596cd186/lot_announcements,lot.59736841,lot.59733281,lot.59735399,lot.59734159,lot.59737073,lot.59738585,lot.59736393,lot.59735675/0?heartbeat=300&tt=15101071663553325&tr=4&uuid=anon-28e9b0ac-42f6-4eda-ab91-06a9ad31d376&pnsdk=PubNub-JS-Web/4.14.0&instanceid=pn-32b2aeec-da35-4dc0-84c2-296d62aaca8e&requestid=f31cf25e-f96f-4a12-ad2c-7032379bb5fc'
    headers = {"Host": "ps.pndsn.com",
               "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:56.0) Gecko/20100101 Firefox/56.0",
               "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
               "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
               "Accept-Encoding": "gzip, deflate, br",
               "Connection": "keep-alive",
               "Upgrade-Insecure-Requests": "1"}
    while True:
        try:
            page = requests.get(url, headers=headers)
            break
        except Exception as e:
            continue
    data = json.loads(page.text)

    product = []
    for i in range(len(data['m'])):
        if data['m'][i]['d']['slots'][0]['elements'][1].get('timer') == 'closed':
            dict ={}
            if data['m'][i]['d']['slots'][0]['elements'][1]['parameters']:
                dict['id'] = data['m'][i]['d']['slots'][0]['id']
                dict['lot_id'] = data['m'][i]['d']['slots'][0]['elements'][1]['lot_id']
                dict['product_parent_id'] = data['m'][i]['d']['slots'][0]['elements'][1]['product_parent_id']
                # 将title字符串中的'和"两个符号全部替换成^符号
                dict['title'] = data['m'][i]['d']['slots'][0]['elements'][10]['text'].replace('"', '^').replace("'", "^")
                dict['hammer_price'] = int(data['m'][i]['d']['slots'][0]['elements'][1]['parameters']['hammer_price']['USD'].strip('$').replace(",", ""))
                dict['num_bids'] = int(data['m'][i]['d']['slots'][0]['elements'][4]['parameters']['num_bids'])
                dict['timer_ends_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(data['m'][i]['d']['slots'][0]['timer_ends_at']))
            elif data['m'][i]['d']['slots'][0]['elements'][1]['parameters'] == {}:
                dict['id'] = data['m'][i]['d']['slots'][0]['id']
                dict['lot_id'] = data['m'][i]['d']['slots'][0]['elements'][1]['lot_id']
                dict['product_parent_id'] = data['m'][i]['d']['slots'][0]['elements'][1]['product_parent_id']
                # 将title字符串中的'和"两个符号全部替换成^符号
                dict['title'] = data['m'][i]['d']['slots'][0]['elements'][10]['text'].replace('"', '^').replace("'", "^")
                dict['hammer_price'] = 0
                dict['num_bids'] = int(data['m'][i]['d']['slots'][0]['elements'][4]['parameters']['num_bids'])
                dict['timer_ends_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(data['m'][i]['d']['slots'][0]['timer_ends_at']))
            product.append(dict)

    return product


# 删除之前的在运行home_page.py的进程
try:
    pid = os.getpid()
    cmd = "kill -9 `ps -aux | grep -v %i | grep python3 | grep home_page.py | awk '{print $2}'`" % pid
    os.system(cmd)
except:
    pass

i = 0
while True:
    t1 = time.time()
    if i >= 30:
        break
    try:
        id_list = []
        # 连接数据库;通过api获取数据;将数据存入数据库;补全数据信息(此步已另用程序完成)
        conn = mysqlconn.mysqlconn()
        conn.set_charset('utf8')
        product = get_auct_data()
        for dict in product:
            try:
                # 将获得的信息存入数据库
                mysqlconn.update2db(conn, dict, 'home_page', ['id'])
                id_list.append(dict['lot_id'])
            except:
                logger.exception("db_error")
        conn.close()
    except:
        logger.exception("main_error")
    if time.time() - t1 < 300:
        time.sleep(300 - (time.time() - t1))
    print(i, time.time() - t1)
    i += 1
    # break
