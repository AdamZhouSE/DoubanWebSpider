"""
爬取豆瓣某小组的内容
在讨论页面爬取所有的帖子，然后进入帖子中爬取其中的具体内容，将所有信息存入mongodb中
支持断点续传，停止时会保存当前爬取到的页数和最新的日期，避免重复爬取
"""
import requests
from lxml import etree
from requests.exceptions import RequestException
import csv
import time
from fake_useragent import UserAgent
import sys
import os
import pymongo

MONGO_URL = "mongodb://localhost:27017/"
MONGO_DB = "douban"
MONGO_COL = "info"
mongo_client = pymongo.MongoClient(MONGO_URL)
mongo_db = mongo_client[MONGO_DB]
mongo_col = mongo_db[MONGO_COL]

# 小组链接
group_url = "https://www.douban.com/group/692739/discussion?start="
# 存储当前页面，支持断点续传
cur_page = 0
# 已经爬取结束的日期
latest_date = "10-30 23:59"


def get_page(url):
    """
    获取页面内容
    :param url: 当前页面链接
    :return: 网页的html样式
    """
    # 休眠2秒，防屏蔽
    time.sleep(2)
    # 使用User-Agent伪装成合法的用户信息
    ua = UserAgent(verify_ssl=False)
    headers = {
        'User-Agent': ua.random}
    try:
        # 爬取网页的html样式
        html = requests.get(url, headers=headers)
        # 判读是否被屏蔽
        if html.status_code == 200:
            print("success")
            return html.content.decode('utf-8')
        else:
            print("failure:", html.status_code)
            print(cur_page)
            record_web_info()
            # ip被封后，休眠5小时重新开始爬虫
            print("Restart the programme in 5 hours.")
            time.sleep(18000)
            restart_program()
    except RequestException:
        print("failure:RequestException")
        print(cur_page)
        record_web_info()
        print("Restart the programme in 10 seconds.")
        # 遇到网络问题，休眠10秒重启程序
        time.sleep(10)
        restart_program()


def parse_page(html):
    """
    解析页面内容格式，获取帖子的标题，作者，回帖数，发布时间，回复时间和内容
    :param html: 当前页面的html样式
    :return:
    """
    # 存储每一条帖子的信息
    info = []
    data = etree.HTML(html)
    tr_nodes = data.xpath('.//*[@id="content"]/div/div[1]/div[2]/table/tr')[1:]
    for node in tr_nodes:
        title = node.xpath('td/a/text()')[0].strip()
        author = node.xpath('td/a/text()')[1].strip()
        count = node.xpath('td[3]/text()')
        # 没有回应时数组为空，补0
        if len(count) == 0:
            count.append(0)
        else:
            count[0] = int(count[0])
        reply_time = node.xpath('td[4]/text()')[0]
        # 获取帖子的链接，爬取帖子的发布时间和具体内容
        article_url = node.xpath('td/a')[0].attrib["href"]
        release_time, content = parse_article(article_url)
        print(title, author, count[0], release_time, reply_time, content)
        info.append([title, author, count[0], release_time, reply_time, content])
    print("page:", info)
    return info


def parse_article(article_url):
    html = get_page(article_url)
    data = etree.HTML(html)
    release_time = data.xpath('//*[@id="topic-content"]/div[2]/h3/span[2]/text()')[0]
    content = data.xpath('//*[@id="link-report"]/div/div/p/text()')
    # 将list合并为字符串
    content = "".join(content)
    # 去除换行符
    content = content.replace('\n', '')
    return release_time, content


def get_all(num):
    """
    :param num:
    :return:
    """
    global cur_page
    start = cur_page
    page_num = 0
    for i in range(start, num, 25):
        url = group_url + str(i)
        html = get_page(url)
        # 判断是否爬到最后一页，内容为空
        if get_last(html):
            break
        info = parse_page(html)
        save_to_mongo(info)
        # if i == 0:
        #     save_as_file(info)
        # else:
        #     write_to_file(info)
        cur_page = cur_page + 25
        print(cur_page)
        record_web_info()
        page_num += 1
        # 每爬20页内容，休眠
        if page_num == 20:
            print("pause 150 seconds")
            time.sleep(150)
            page_num = 0
    print("Get all data.")
    # 爬取结束，将页面重置为0，并休眠24小时后重新爬取
    cur_page = 0
    record_web_info()
    time.sleep(86400)
    restart_program()


def record_web_info():
    """
    实现断点续传
    当遇到问题时，记录当前爬取到的页面，重启时从该页面继续爬取
    记录当前时间，再次爬取时不会爬取超过当前内容的部分
    :return:
    """
    with open("web_info", "w") as f:
        s = str(cur_page) + '\n' + str(time.strftime("%m-%d %H:%M", time.localtime()))
        f.write(s)


def get_web_info():
    global cur_page
    global latest_date
    with open("web_info", "r") as f:
        s = f.readline()
        cur_page = int(s[0:len(s) - 1])
        s = f.readline()
        latest_date = s
        print(cur_page)


def save_to_mongo(info):
    global cur_page
    for one_post in info:
        record = {'title': one_post[0],
                  'author': one_post[1],
                  'count': one_post[2],
                  'release_time': one_post[3],
                  'reply_time': one_post[4],
                  'content': one_post[5]}
        # 比爬完信息的日期大，或者数据库中不存在此条信息,存入数据
        if one_post[4] > latest_date or \
                mongo_col.count_documents(
                    {'title': one_post[0], 'author': one_post[1], 'release_time': one_post[3]}) == 0:
            print("insert one record to mongodb:", record)
            mongo_col.insert_one(record)
        # 有比当前日期大的日期，说明已经重复爬取，休眠2小时后重头爬取
        else:
            print("Data repeated. Restart the programme in 2 hours.")
            cur_page = 0
            record_web_info()
            time.sleep(7200)
            restart_program()


# def save_as_file(info):
#     with open("test.csv", "w") as f:
#         writer = csv.writer(f)
#         writer.writerow(["title", "author", "count", "release_time", "reply_time", "content"])
#         writer.writerows(info)
#
#
# def write_to_file(info):
#     with open("test.csv", "a+") as f:
#         writer = csv.writer(f)
#         writer.writerows(info)


def get_last(html):
    """
    检测是否爬取结束，即空页，空页中帖子数为0
    :param html:
    :return:
    """
    data = etree.HTML(html)
    tr_nodes = data.xpath('.//*[@id="content"]/div/div[1]/div[2]/table/tr')
    print(len(tr_nodes))
    if len(tr_nodes) == 0:
        return True
    return False


def restart_program():
    print('ready to restart program......')
    python = sys.executable
    os.execl(python, python, *sys.argv)


if __name__ == '__main__':
    get_web_info()
    try:
        print("Start the web spider.")
        get_all(25000)
    except KeyboardInterrupt:
        print("cur_page", cur_page)
        record_web_info()
        sys.exit()

    # try:
    #     url = "https://www.douban.com/group/692739/discussion?start=0"
    #     parse_page(get_page(url))
    # except KeyboardInterrupt:
    #     print(cur_page)
    #     sys.exit()
