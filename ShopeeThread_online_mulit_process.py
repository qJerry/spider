# python 2.79
#-*- coding:utf-8 -*-
#爬取虾皮网的商品
#版本2：用多进程处理

from Queue import Queue

import random

import threading
import requests
import json
import datetime
import time
import sys
import pymysql.cursors
import hashlib
from selenium import webdriver
from multiprocessing import Process, Queue, Pool
import multiprocessing


reload(sys)
sys.setdefaultencoding('utf8')
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.170 Safari/537.36'}

nowDay = datetime.datetime.now()
sevenDayAgo = time.mktime((nowDay - datetime.timedelta(days=7)).timetuple())

cateNameMap = {}
subCateMap = {}
subSubCateMap = {}
baseUrl = 'https://shopee.tw/'
headMap = {}
requestLogger = open("requestLog", "a")
producerLogger = open('producerLogger', 'a')
runLog = open('runLogger', 'a')

def getByHeaders(url,headers):
    list = requests.get(url,headers=headers)
    listJson = json.loads(list.text)
    return listJson

def postByHeaders(url,json,headers):
    list = requests.post(url,json=json,headers=headers)
    return list.json()

def get(url):
    list = requests.get(url)
    listJson = json.loads(list.text)
    return listJson

#获取分类生成字典
def getCategoryToMap(url):
    categoryInfo = requests.get(url)
    cateJson = json.loads(categoryInfo.text)
    for cate in cateJson:
        cateNameMap[cate.get('main').get('catid')] = cate.get('main').get('display_name')
        for sub in cate.get('sub'):
            subCateMap[sub.get('catid')] = sub.get('display_name')
            for subSub in sub.get('sub_sub'):
                subSubCateMap[subSub.get('catid')] = subSub.get('display_name')
#获取运费
def getFeight(url):
    ship = get(url)
    logistics = ship.get('logistics')
    startFeight = long(logistics[0].get('cost')) / 100000
    endFeight = long(logistics[len(logistics)-1].get('cost')) / 100000
    if(startFeight == endFeight) : return startFeight
    else : return  bytes(startFeight) +'-' + bytes(endFeight)
#获取分类
def getCategory(cateName,subCatId,subSubCatId,subCateMap,subSubCateMap):
    if (subCateMap.get(subCatId) is not None):
        cateName = cateName + '>' + subCateMap.get(int(subCatId))
    if (subSubCateMap.get(subSubCatId) is not None):
        cateName = cateName + '>' + subSubCateMap.get(int(subSubCatId))
    return cateName
# 获取7天评论数
def getSevenDiscuss(url):
    reqTime = time.time()
    # discuss = get(url)
    # sevenDiscussNum = len([item for item in discuss.get('comments') if item.get('ctime') > sevenDayAgo])
    # return sevenDiscussNum
    len_ = 0
    pagenums = [0, 100, 200, 300, 400, 500]
    for index in range(len(pagenums) - 1):
        offset_ = pagenums[index]
        limit_ = pagenums[index + 1]
        discuss = get(url + '&offset=' + bytes(offset_) + '&limit=' + bytes(limit_) + '&flag=1&filter=0')
        comment = discuss.get('comments')
        len1 = len([item for item in comment if item.get('ctime') > sevenDayAgo])
        if (len1 == 0):
            break
        len_ += len1
        if comment[len(comment)-1].get('ctime') < sevenDayAgo:
            return len_
    print ('*****************seven Discuss use time :' + bytes(time.time() - reqTime))
    return len_

# 模拟浏览器打开网页
def openChrome() :
    driver = webdriver.Chrome('chromedriver.exe')
    driver.get(baseUrl)
    return driver.get_cookies()

#Produer
class Producer(multiprocessing.Process) :

    def __init__(self,pageUrl,cateId,queue,cookie,token,cateNameMap,subCateMap,subSubCateMap):
        multiprocessing.Process.__init__(self)
        self.data = queue
        self.cateId = cateId
        self.pageUrl = pageUrl
        self.cookie = cookie
        self.token = token
        self.cateNameMap = cateNameMap
        self.subCateMap = subCateMap
        self.subSubCateMap = subSubCateMap

    def run(self):
        i = 0
        # 获取列表
        while True:
            try:
                requestStartTime = time.time()
                listUrl = baseUrl + 'api/v2/search_items/?by=relevancy&match_id=' + bytes(
                    self.cateId) + '&limit=100&newest=' + bytes(
                    i) + '&order=desc&page_type=search'
                header = {
                    'cookie': self.cookie,
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.170 Safari/537.36',
                    'content-type': 'charset=utf8',
                    'x-api-source':'pc',
                    'x-requested-with':'XMLHttpRequest'
                }
                response = getByHeaders(listUrl, header)
                list = response.get('items')
                # print 'len：' + bytes(len(list))
                if (i <= 8000 and len(list) == 0):
                    runLog.write('this cate :' + bytes(self.cateId) + ' has run api with 0 result but not reach 8000, try again, ' + listUrl + '\n')
                    continue
                if (len(list) == 0):
                    runLog.write('this cate :' + bytes(self.cateId) + ' has run api end, and the listUrl is :' + listUrl + '\n')
                    break
                jsons = {}
                array = []
                for item in list:
                    shopId = item.get('shopid')
                    itemId = item.get('itemid')
                    jsonItem = {}
                    jsonItem["itemid"] = itemId
                    jsonItem["shopid"] = shopId
                    array.append(jsonItem)
                jsons["item_shop_ids"] = array
                header = {
                    'referer': self.pageUrl,
                    'cookie': self.cookie,
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.170 Safari/537.36',
                    'x-csrftoken': self.token,
                    'content-type': 'charset=utf8'
                }
                res = postByHeaders('https://shopee.tw/api/v1/items/', jsons, header)
                print ('*****************request use time :' + bytes(time.time() - requestStartTime))
                requestLogger.write('*****************request use time :' + bytes(time.time() - requestStartTime) + '\n')
                for elm in res:
                    PostStartTime = time.time()
                    if elm.get('shopid') is None and elm.get('itemid') is None: continue
                    else:
                        # 商品名称
                        name = elm.get('name')
                        # 详情链接
                        detailUrl = baseUrl + name + '-i.' + bytes(elm.get('shopid')) + '.' + bytes(
                            elm.get('itemid'))
                        code = hashlib.md5(detailUrl.encode('utf-8')).hexdigest()
                        # sql = "SELECT id from base_shopee_product_info where code = '{}' limit 1".format(code)
                        # cursor.execute(sql)
                        # row_1 = cursor.fetchone()
                        # if row_1 is not None:
                        #     print '{} exists !!!'.format(code)
                        #     continue
                        # else:
                        # 分类
                        categoryName = getCategory(self.cateNameMap.get(elm.get('catid')), elm.get('sub_catid'),elm.get('third_catid'), self.subCateMap, self.subSubCateMap)
                        # 价格
                        price = long(elm.get('price')) / 100000
                        # 销售量
                        soleCount = elm.get('sold')
                        # 物流成本·
                        freight = getFeight(baseUrl + 'api/v0/shop/' + bytes(elm.get('shopid')) + '/item/' + bytes(elm.get('itemid')) + '/shipping_fee/')
                        # 总评论
                        discussCount = elm.get('rating_count')[0]
                        # 7天评论数
                        sevenDiscussNum = getSevenDiscuss(baseUrl + 'api/v1/comment_list/?item_id=' + bytes(elm.get('itemid')) + '&shop_id=' + bytes(elm.get('shopid')))
                        array = {}
                        array['name'] = name
                        array['detailUrl'] = detailUrl
                        array['categoryName'] = categoryName
                        array['price'] = price
                        array['soleCount'] = soleCount
                        array['freight'] = freight
                        array['discussCount'] = discussCount
                        array['sevenDiscussNum'] = sevenDiscussNum
                        array['code'] = code
                        self.data.put(array)
                        print 'start producer...'
                        time.sleep(random.randrange(2) / 3)
                    print ('*****************producer use time :' + bytes(time.time() - PostStartTime))
                    producerLogger.write('*****************producer use time :' + bytes(time.time() - PostStartTime) + '\n')
            except Exception, e:
                print 'err by producer .........\n', e
            finally:
                print 'finally ...'
            i = i + 100

class Consumer(multiprocessing.Process):

    def __init__(self, queue):
        multiprocessing.Process.__init__(self)
        self.data = queue

    def run(self):
        connection = pymysql.connect(host='tsaoko-clu-1.cluster-c7anlmn5zuin.ap-northeast-1.rds.amazonaws.com',
                                     port=3306,
                                     user='tsaoko', password='Tsaoko2018go!', db='tsaoko', charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        # 通过cursor创建游标
        cursor = connection.cursor()
        while(True):
            array = self.data.get()
            if(array is None):
                cursor.close()
                connection.close()
                break
            print 'start consume...'
            try:
                sql = "INSERT INTO `base_shopee_product_info` (`title`, `real_price`, `freight`, `sold_count`, `rating_count`, `categories`, `recent_comment_count`, `url`, `code`) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}' )".format(
                    array.get('name').encode('utf-8'), array.get('price'), bytes(array.get('freight')).encode('utf-8'),
                    bytes(array.get('soleCount')).encode('utf-8'), bytes(array.get('discussCount')).encode('utf-8'),
                    array.get('categoryName').encode('utf-8'), bytes(array.get('sevenDiscussNum')).encode('utf-8'), array.get('detailUrl').encode('utf-8'),
                    array.get('code').encode('utf-8'))
                sql = sql + "ON DUPLICATE KEY UPDATE update_time='{}', real_price='{}',freight='{}',sold_count='{}',rating_count='{}',categories='{}',recent_comment_count='{}'".format(nowDay,array.get('price'),bytes(array.get('freight')).encode('utf-8'),
                bytes(array.get('soleCount')).encode('utf-8'), bytes(array.get('discussCount')).encode('utf-8'),
                array.get('categoryName').encode('utf-8'), bytes(array.get('sevenDiscussNum')).encode('utf-8'))
                cursor.execute(sql)
                connection.commit()
                print 'success to commit'
            except Exception, e:
                print 'sql err .........\n', e
            time.sleep(random.randrange(2))
        cursor.close()
        connection.close()


if __name__ == '__main__':
    print 'start...'
    startTime = time.time()
    getCategoryToMap(baseUrl + 'api/v1/category_list/')

    chromeCookie = openChrome()
    token = [item.get('value') for item in chromeCookie if item.get('name') == 'csrftoken'][0]
    cookie = ';'.join(
        ['{}={}'.format(item.get('name'), item.get('value')) for item in chromeCookie])


    # create queue
    queue = multiprocessing.Queue(100)

    processed = []

    for (cateId, name) in cateNameMap.items():
        pageUrl = baseUrl + name + '-cat.' + bytes(cateId)
        processed.append(Producer(pageUrl,cateId,queue,cookie,token,cateNameMap,subCateMap,subSubCateMap))

    processed.append(Consumer(queue))

    # start processes
    for i in range(len(processed)):
        processed[i].start()

    # join processes
    for i in range(len(processed)):
        if(i == len(processed)-1):
            queue.put(None)
        processed[i].join()


    print ('end,use time :'+ bytes(time.time()-startTime))
    f = open("proxyTime", "w")
    f.write('end,use time :'+ bytes(time.time()-startTime) + '\n')
