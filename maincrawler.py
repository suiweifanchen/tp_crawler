# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 11:47:14 2017

@author: Qian
"""

import time
import random
from my_modules import mysqlconn
from tophatterproductapi import TophatterProductAPI
from tophattersellercrawler import TophatterSellerCrawler


class MainCrawler:
    """
    聚合TophatterProductAPI和TophatterSellerCrawler,实现对每个seller的所有商品信息的获取
    TophatterSellerCrawler能够获取seller所有商品和对应销量
    TophatterProductAPI能够获取单个商品的所有信息
    用法：初始化不需要提供任何参数，自动更新seller_id_list;
         使用loop方法,会进行两层循环，大循环对seller_id_list进行循环，遍历所有seller，使用TophatterSellerCrawler获得seller所有产品。
                                小循环对每个selller的产品id列表进行循环，遍历所有产品，使用TophatterProductAPI获得产品信息。小循环结束会更新信息到db
    改进方向：异步，使用TophatterProductAPI网络IO耗时很大,占程序运行时间最大。
    """
    
    def __init__(self):
        """初始化卖家列表，后续需要更新get_seller_id_list方法，暂时只提供一个seller"""
        self.seller_id_list = self.get_seller_id_list()
        self.product_id = []
        self.product_data = []
        self.error = []

    def get_seller_id_list(self):
        id_list = ['14997818', '8405278', '10769600', '5794087', '5591521', '3032733', '8655286', '11344706',
                   '11165897', '5934167', '6858850', '7592858', '5677249', '8405278', '4360004', '5794087', '5195682',
                   '4889170', '1173420', '5591521', '5434308', '4246555', '1363400', '3402600', '7693262', '2802741']
        return list(set(id_list))

    # ['14997818', '8405278', '10769600', '5794087', '5591521', '3032733', '8655286', '11344706',
    #              '11165897', '5934167', '6858850', '7592858', '5677249', '8405278', '4360004', '5794087', '5195682',
    #              '4889170', '1173420', '5591521', '5434308', '4246555', '1363400', '3402600', '7693262', '2802741']


    def loop(self):
        for seller_id in self.seller_id_list:
            self.product_id = []
            self.product_data = []

            try:
                sellercrawler = TophatterSellerCrawler(seller_id)
                sellercrawler.loop()
            except Exception as e:
                self.error.append(('seller_id', seller_id, e))
                break
            bought_num = [i[0] for i in sellercrawler.info]
            self.product_id = [i[1] for i in sellercrawler.info]

            j = 1  # 计数，测试为了看结果
            for i in self.product_id:
                try:
                    time.sleep(random.uniform(0.5, 1.5))
                    prod = TophatterProductAPI(i)
                except Exception as e:
                    self.error.append(('product_id', i, e))
                    break
                dict = {}
                try:
                    dict['id'] = prod.productdata['id']
                    dict['title'] = prod.productdata['title']
                    dict['main_image'] = prod.productdata['main_image']
                    dict['user_id'] = prod.productdata['user_id']
                    dict['seller_name'] = prod.productdata['seller_name']
                    dict['currency_code'] = prod.productdata['currency_code']
                    dict['buy_now_price'] = prod.productdata['buy_now_price']
                    dict['retail_price'] = prod.productdata['retail_price']
                    dict['shipping_price'] = prod.productdata['shipping_price']
                    dict['seller_lots_sold'] = prod.productdata['seller_lots_sold']
                    dict['alerts_count'] = prod.productdata['alerts_count']
                    dict['views_count'] = prod.productdata['views_count']
                    dict['created_at'] = prod.productdata['lot_upsells'][0]['created_at'].split('T')[0]
                    dict['updated_at'] = prod.productdata['lot_upsells'][0]['updated_at'].split('T')[0]
                    dict['bought_num'] = bought_num[j-1]
                    dict['record_at'] = 'curdate()'
                except KeyError:
                    dict['id'] = prod.productdata['id']
                    dict['title'] = prod.productdata['title']
                    dict['main_image'] = prod.productdata['main_image']
                    dict['user_id'] = prod.productdata['user_id']
                    dict['seller_name'] = prod.productdata['seller_name']
                    dict['currency_code'] = prod.productdata['currency_code']
                    dict['buy_now_price'] = prod.productdata['buy_now_price']
                    dict['retail_price'] = prod.productdata['retail_price']
                    dict['shipping_price'] = prod.productdata['shipping_price']
                    dict['seller_lots_sold'] = prod.productdata['seller_lots_sold']
                    dict['alerts_count'] = prod.productdata['alerts_count']
                    dict['views_count'] = prod.productdata['views_count']
                    dict['created_at'] = 'NULL'
                    dict['updated_at'] = 'NULL'
                    dict['bought_num'] = bought_num[j-1]
                    dict['record_at'] = 'curdate()'
                except Exception as e:
                    self.error.append((e, prod.productdata))
                # 将数据中的None换成NULL
                for key in dict:
                    if dict[key] is None:
                        dict[key] = 'NULL'
                # 将title和seller_name中的'和"符号换成^符号
                dict['title'].replace('"', '^').replace("'", "^")
                dict['seller_name'].replace('"', '^').replace("'", "^")
                self.product_data.append((j, dict))
                j = j + 1

            # 将product_data中的数据更新到数据库中
            self.conn = mysqlconn.mysqlconn()
            self.conn.set_charset('utf8')
            for item in self.product_data:
                try:
                    mysqlconn.update2db(self.conn, item[1], 'product', ['id', 'record_at'])
                except Exception as e:
                    self.error.append((e, item[0], item[1]))
            self.conn.close()


if __name__ == '__main__':
    m = MainCrawler()
    m.loop()
    with open('main_error.txt', 'a') as f:
        for e in m.error:
            f.writelines(str(e) + '\n')
        f.writelines('\n\n')
