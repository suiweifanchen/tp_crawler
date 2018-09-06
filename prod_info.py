# -*- coding: utf-8 -*-
"""
Created at: 18-2-22 上午5:45

@Author: Qian
"""

import os
import json
import time
import queue
import random
import logging
import requests
import threading
from my_modules import mysqlconn

#################################################
# 删除之前的在运行home_page/prod_info.py的进程
try:
    pid = os.getpid()
    cmd = "kill -9 `ps -aux | grep -v %i | grep python3 | grep home_page/prod_info.py | awk '{print $2}'`" % pid
    os.system(cmd)
except:
    pass

#################################################
# 日志
logger = logging.getLogger("Tophatter Product_info")
logger.setLevel(logging.DEBUG)
# 日志格式
fmt = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
# 文件日志, DEBUG级别
fh = logging.FileHandler(os.path.join(os.path.dirname(__file__), 'prod_info.log'))
fh.setLevel(logging.DEBUG)
fh.setFormatter(fmt)
# 控制台日志, DEBUG级别
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(fmt)
# 将相应的handler添加在logger对象中
logger.addHandler(fh)
logger.addHandler(ch)

#################################################
id_queue = queue.Queue()
page_queue = queue.Queue()
lock = threading.Lock()
exit_flag = 0


#################################################
def _create_proxies_pool():
    conn = mysqlconn.mysqlconn()
    cur = conn.cursor()
    cur.execute("select * from ip_proxy where https='yes' and error_num<6 and state<>'dead'")
    ip_list = cur.fetchall()
    conn.close()

    pool = []
    for i in range(len(ip_list)):
        pool.append([i, {"https": "http://" + ip_list[i][0] + ":" + ip_list[i][1]}, 0])
    return pool


def create_id_queue():
    conn = mysqlconn.mysqlconn()
    cur = conn.cursor()
    cur.execute("select distinct lot_id,timer_ends_at from home_page where shipping_price is NULL and timer_ends_at>'2018-02-12 00:00:00' order by timer_ends_at desc;")
    id_list = cur.fetchall()
    conn.close()
    for i in id_list:
        id_queue.put(i[0])


class RequestThread(threading.Thread):

    proxies_pool = _create_proxies_pool()

    def __init__(self, thread_id):
        super(RequestThread, self).__init__()
        self.threadID = thread_id
        self.name = "request_thread"
        self.api = "https://tophatter.com/api/v1/lots/"

    def run(self):
        while not id_queue.empty():
            try:
                if page_queue.qsize() > 160:
                    time.sleep(10)

                lock.acquire()
                self.lot_id = id_queue.get()
                lock.release()

                url = self.api + self.lot_id
                page = self.get_page(url)

                if page:
                    lock.acquire()
                    page_queue.put((page, self.lot_id))
                    lock.release()
                    logger.info(self.lot_id + ' Request Success')
                else:
                    logger.info(self.lot_id + ' Request Failure')
            except:
                logger.exception('RequestThread Error ' + self.lot_id)
                pass

    def get_page(self, url):
        count = 0
        while count < 3:
            proxies = self._get_proxies()
            try:
                page = requests.get(url, proxies=proxies[1], timeout=10)
                if page.status_code == 200:
                    return page
                else:
                    count += 1
                    self._proxies_fail(proxies)
            except:
                count += 1
                self._proxies_fail(proxies)

        logger.error('Get Page Error   ' + url)
        return 0

    def _proxies_fail(self, proxies):
        lock.acquire()
        if proxies in self.proxies_pool:
            index = self.proxies_pool.index(proxies)
            self.proxies_pool[index][2] += 1
            if self.proxies_pool[index][2] > 5:
                self.proxies_pool.remove(proxies)
                ip, port = proxies[1]['https'].split('/')[-1].split(':')
                proxies = {'ip': ip, 'port': port, 'error_num':6}
                conn = mysqlconn.mysqlconn()
                mysqlconn.db_update(conn, proxies, ['ip', 'port'], 'ip_proxy')
                conn.close()
        if len(self.proxies_pool) < 16:
            RequestThread.proxies_pool = _create_proxies_pool()
        lock.release()

    @staticmethod
    def _get_proxies():
        return random.choice(RequestThread.proxies_pool)


class ParseThread(threading.Thread):

    def __init__(self, thread_id):
        super(ParseThread, self).__init__()
        self.threadID = thread_id
        self.name = "parse_thread"

    def run(self):
        while not exit_flag:
            if not page_queue.empty():
                try:
                    lock.acquire()
                    page, lot_id = page_queue.get()
                    lock.release()
                    data = json.loads(page.text)
                    self.handle_data(data, lot_id)
                    logger.info(lot_id + ' Parse Success')
                except:
                    logger.exception('ParseThread Error')
                    pass
            else:
                time.sleep(1)

    def handle_data(self, data, lot_id):
        dct = {}
        dct['lot_id'] = lot_id
        dct['seller_id'] = data['user_id']
        dct['seller_sold'] = data['seller_lots_sold']
        dct['shipping_price'] = data['shipping_price']

        lock.acquire()
        conn = mysqlconn.mysqlconn()
        mysqlconn.db_update(conn, dct, ['lot_id'], 'home_page')
        conn.close()
        lock.release()


if __name__ == '__main__':
    logger.info('Start')
    create_id_queue()
    max_thread_num = 6
    request_thread_num = min(id_queue.qsize(), len(RequestThread.proxies_pool), max_thread_num)
    parse_thread_num = 1

    logger.info('Start Threads')
    threads = []
    for i in range(request_thread_num):
        thread = RequestThread(i)
        thread.setDaemon(True)
        thread.start()
        logger.info('RequestThread-' + str(i) + ' Started')
        threads.append(thread)
    for i in range(parse_thread_num):
        thread = ParseThread(i)
        thread.setDaemon(True)
        thread.start()
        logger.info('ParseThread-' + str(i)+ ' Started')
        threads.append(thread)

    while not id_queue.empty():
        time.sleep(1)
    while not page_queue.empty():
        time.sleep(1)
    exit_flag = 1

    for i in range(len(threads)):
        threads[i].join()
        logger.info('Thread-' + str(i) + ' Joined')

    logger.info('End')
    pass
