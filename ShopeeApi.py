# python 2.79
###
##  Date:2018/05/17
##  Author:Jerry
##  description:first use webdriver open the webpage,and get the token and cookie,which be used to requests the api.
##
#-*- coding:utf-8 -*-

import requests
import json
import datetime
import time
import sys
import pymysql.cursors
import hashlib
from selenium import webdriver
# from pyvirtualdisplay import Display

# display = Display(visible=0, size=(800, 600))
# display.start()
# driver = webdriver.Firefox()
driver = webdriver.Chrome(sys.argv[1])
connection = pymysql.connect(host='tsaoko-clu-1.cluster-c7anlmn5zuin.ap-northeast-1.rds.amazonaws.com',port=3306,user='tsaoko',password='Tsaoko2018go!',db='tsaoko',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
#通过cursor创建游标
cursor = connection.cursor()
reload(sys)
sys.setdefaultencoding('utf8')
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.170 Safari/537.36'}

today = datetime.datetime.now()
sevenDayAgo = time.mktime((today - datetime.timedelta(days=1)).timetuple())

cateNameMap = {}
subCateMap = {}
subSubCateMap = {}

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
#时间戳比较
def compareTime(standardTime,inputTime):
    if(inputTime <= standardTime):
        return 0
    return 1
#获取运费
def getFeight(url):
    ship = get(url)
    logistics = ship.get('logistics')
    return long(logistics[0].get('cost')) / 100000
#获取分类
def getCategory(cateName,subCatId,subSubCatId):
    if (subCateMap.get(subCatId) is not None):
        cateName = cateName + '>' + subCateMap.get(int(subCatId))
    if (subSubCateMap.get(subSubCatId) is not None):
        cateName = cateName + '>' + subSubCateMap.get(int(subSubCatId))
    return cateName
# 获取7天评论数
def getSevenDiscuss(url):
    sevenDiscussNum = 0
    j = 0
    while True:
        #评论接口
        discuss = get(url + bytes(j))
        discussList = discuss.get('comments')
        if (len(discussList) == 0): break
        ctime = discussList[len(discussList) - 1].get('ctime')
        sevenDiscussNum += len(discussList)
        if (compareTime(sevenDayAgo, ctime) == 0): break
        else: j = j + 1
    return sevenDiscussNum

if __name__ == '__main__':
    print 'start...'

    baseUrl = 'https://shopee.tw/'

    getCategoryToMap(baseUrl + 'api/v1/category_list/')

    count = 8000
    driver.get(baseUrl)
    cookie = ';'.join(['{}={}'.format(item.get('name'), item.get('value')) for item in driver.get_cookies()])
    token = [item.get('value') for item in driver.get_cookies() if item.get('name') == 'csrftoken'][0]

    for (cateId,name) in cateNameMap.items():
        pageUrl = baseUrl + name + '-cat.' + bytes(cateId)
        i = 0
        # 获取列表
        while True:
            try:
                print 'now start by ' + bytes(i)
                listUrl = baseUrl + 'api/v2/search_items/?by=relevancy&match_id=' + bytes(cateId)  +'&limit=100&newest=' + bytes(
                    i) + '&order=desc&page_type=search'
                response = getByHeaders(listUrl, headers)
                list = response.get('items')
                print 'len：' + bytes(len(list))
                if (i <= count and len(list) == 0): continue;
                if(len(list) == 0) : break
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
                headers = {
                    'Referer': pageUrl,
                    'Cookie': cookie,
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.170 Safari/537.36',
                    'x-csrftoken': token,
                    'content-type': 'charset=utf8'
                }
                res = postByHeaders('https://shopee.tw/api/v1/items/', jsons, headers)
                for elm in res:
                    # 分类
                    categoryName = getCategory(cateNameMap.get(elm.get('catid')), elm.get('sub_catid'),
                                               elm.get('third_catid'))
                    # 商品名称
                    name = elm.get('name')
                    # 价格
                    price = long(elm.get('price')) / 100000
                    # 销售量
                    soleCount = elm.get('sold')
                    # 物流成本
                    freight = getFeight(
                        baseUrl + 'api/v0/shop/' + bytes(shopId) + '/item/' + bytes(itemId) + '/shipping_fee/')
                    # 总评论
                    discussCount = elm.get('rating_count')[0]
                    # 详情链接
                    detailUrl = baseUrl + name + '-i.' + bytes(elm.get('shopid')) + '.' + bytes(elm.get('itemid'))
                    # 7天评论数
                    sevenDiscussNum = getSevenDiscuss(
                        baseUrl + 'api/v1/comment_list/?item_id=' + bytes(elm.get('itemid')) + '&shop_id=' + bytes(
                            elm.get('shopid')) + '&limit=10&flag=1&filter=0' + '&offset=')
                    # print categoryName + "--标题--" + name + '--价格--' + bytes(price) + '--销售量--' + bytes(
                    #     soleCount) + '--评论数--' + bytes(discussCount) + '--物流成本--' + bytes(
                    #     freight) + '--7天评论数--' + bytes(sevenDiscussNum) + '--链接--' + detailUrl
                    try:
                        code = hashlib.md5(detailUrl.encode('utf-8')).hexdigest()
                        sql = "SELECT id from base_shopee_product_info where code = '{}' limit 1".format(code)
                        cursor.execute(sql)
                        row_1 = cursor.fetchone()
                        if row_1 is None:
                            sql = "INSERT INTO `base_shopee_product_info` (`title`, `real_price`, `freight`, `sold_count`, `rating_count`, `categories`, `recent_comment_count`, `url`, `code`) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(
                                name.encode('utf-8'), price, bytes(freight).encode('utf-8'),
                                bytes(soleCount).encode('utf-8'), bytes(discussCount).encode('utf-8'),
                                categoryName.encode('utf-8'), bytes(sevenDiscussNum).encode('utf-8'), detailUrl,
                                code.encode('utf-8'))
                            # print sql
                            cursor.execute(sql)
                            connection.commit()
                        else:
                            print '{} exists !!!'.format(code)
                    except Exception, e:
                        print 'sql err .........\n', e
            except Exception, e:
                print 'err .........\n', e
            finally:
                print 'finally ...'
            i = i + 100

    driver.quit()
    print 'end...'