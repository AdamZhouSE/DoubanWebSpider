import requests
from lxml import etree
from requests.exceptions import RequestException
import csv
import time
import random
from fake_useragent import UserAgent
import sys
import os
group_url = "https://www.douban.com/group/692739/discussion?start="
# 存储当前页面，支持断点续传
cur_page = 0


def record_page_no():
    with open("page_no", "w") as f:
        f.write(str(cur_page))


def get_page_no():
    global cur_page
    with open("page_no", "r") as f:
        cur_page = int(f.read())
        print(cur_page)


def get_page(url):
    """
    :param url: 当前页面链接
    :return: 网页的html样式
    """
    # 休眠1秒，防屏蔽
    time.sleep(2)
    # 使用User-Agent伪装成合法的用户信息
    ua = UserAgent(verify_ssl=False)
    headers = {
        'User-Agent': ua.random}
    print(headers)
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
            record_page_no()
            sys.exit()
    except RequestException:
        print("failure:RequestException")
        print(cur_page)
        record_page_no()
        # 遇到网络问题，休眠10秒重启程序
        time.sleep(10)
        restart_program()


def parse_page(html):
    """
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


def save_as_file(info):
    with open("test.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "author", "count", "release_time", "reply_time", "content"])
        writer.writerows(info)


def write_to_file(info):
    with open("test.csv", "a+") as f:
        writer = csv.writer(f)
        writer.writerows(info)


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
        if i == 0:
            save_as_file(info)
        else:
            write_to_file(info)
        cur_page = cur_page + 25
        print(cur_page)
        record_page_no()
        page_num += 1
        # 每爬20页内容，休眠
        if page_num == 20:
            print("pause 150 seconds")
            time.sleep(150)
            page_num = 0


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
    get_page_no()
    try:
        get_all(25000)
    except KeyboardInterrupt:
        print(cur_page)
        record_page_no()
        sys.exit()

    # try:
    #     url = "https://www.douban.com/group/692739/discussion?start=0"
    #     parse_page(get_page(url))
    # except KeyboardInterrupt:
    #     print(cur_page)
    #     sys.exit()
