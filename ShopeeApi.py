# python 2.79
###
##  Date:2018/05/17
##  Author:Jerry
##  description:first use webdriver open the webpage,and get the token and cookie,which be used to requests the api.
##
#-*- coding:utf-8 -*-

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

driver = webdriver.Chrome(sys.argv[1])
connection = pymysql.connect(host='',port=3306,user='',password='',db='',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
#通过cursor创建游标
cursor = connection.cursor()
reload(sys)
sys.setdefaultencoding('utf8')
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.170 Safari/537.36'}

nowDay = datetime.datetime.now()
sevenDayAgo = time.mktime((nowDay - datetime.timedelta(days=7)).timetuple())

cateNameMap = {}
subCateMap = {}
subSubCateMap = {}
baseUrl = 'https://shopee.tw/'
driver.get(baseUrl)
cookie = ';'.join(['{}={}'.format(item.get('name'), item.get('value')) for item in driver.get_cookies()])
token = [item.get('value') for item in driver.get_cookies() if item.get('name') == 'csrftoken'][0]

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
def getCategory(cateName,subCatId,subSubCatId):
    if (subCateMap.get(subCatId) is not None):
        cateName = cateName + '>' + subCateMap.get(int(subCatId))
    if (subSubCateMap.get(subSubCatId) is not None):
        cateName = cateName + '>' + subSubCateMap.get(int(subSubCatId))
    return cateName
# 获取7天评论数
def getSevenDiscuss(url):
    # discuss = get(url)
    # sevenDiscussNum = len([item for item in discuss.get('comments') if item.get('ctime') > sevenDayAgo])
    # return sevenDiscussNum
    len_ = 0
    pagenums = [0, 100, 200, 300, 400, 500]
    for index in range(len(pagenums) - 1):
        offset_ = pagenums[index]
        limit_ = pagenums[index + 1]
        discuss = get(url + '&offset=' + bytes(offset_) + '&limit=' + bytes(limit_) + '&flag=1&filter=0')
        len_ += len([item for item in discuss.get('comments') if item.get('ctime') > sevenDayAgo])
    print(len_)
    return len_

#Produer
class Producer(threading.Thread) :

    def __init__(self,pageUrl,cateId,queue):
        threading.Thread.__init__(self)
        self.data = queue
        self.cateId = cateId
        self.pageUrl = pageUrl

    def run(self):
        i = 0
        # 获取列表
        while True:
            try:
                # print 'now start by ' + bytes(i)
                listUrl = baseUrl + 'api/v2/search_items/?by=relevancy&match_id=' + bytes(
                    self.cateId) + '&limit=100&newest=' + bytes(
                    i) + '&order=desc&page_type=search'
                response = getByHeaders(listUrl, headers)
                list = response.get('items')
                # print 'len：' + bytes(len(list))
                if (i <= 8000 and len(list) == 0): continue;
                if (len(list) == 0): break
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
                    'Referer': self.pageUrl,
                    'Cookie': cookie,
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.170 Safari/537.36',
                    'x-csrftoken': token,
                    'content-type': 'charset=utf8'
                }
                res = postByHeaders('https://shopee.tw/api/v1/items/', jsons, header)
                for elm in res:
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
                        categoryName = getCategory(cateNameMap.get(elm.get('catid')), elm.get('sub_catid'),elm.get('third_catid'))
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
                        time.sleep(random.randrange(3) / 3)
            except Exception, e:
                print 'err .........\n', e
            finally:
                print 'finally ...'
            i = i + 100

class Consumer(threading.Thread):

    def __init__(self, queue):
        threading.Thread.__init__(self)

        self.data = queue

    def run(self):
        while(True):
            array = self.data.get()
            if(array is None): break;
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
            time.sleep(random.randrange(3))


if __name__ == '__main__':
    print 'start...'
    startTime = time.time()
    getCategoryToMap(baseUrl + 'api/v1/category_list/')
    queue = Queue(100)
    threadList = []
    for (cateId, name) in cateNameMap.items():
        pageUrl = baseUrl + name + '-cat.' + bytes(cateId)
        producer = Producer(pageUrl,cateId,queue)
        producer.start()
        threadList.append(producer)

    consumer = Consumer(queue)
    consumer.start()

    for t in threadList:
        t.join()

    queue.put(None)
    consumer.join()

    print ('end,use time :'+ bytes(time.time()-startTime))
    cursor.close();
    connection.close();