import requests
from lxml import etree
import pandas as pd
import re
from threading import Thread
import os
from queue import Queue
import time


ABSDIR_PATH = os.path.abspath(os.path.dirname(__file__))
DEFAULT_CSV_PATH = os.path.join(ABSDIR_PATH, "5i5j.csv")


def timer(fun):
    def warper(*args, **kwargs):
        print("程序开始启动...")
        time1 = time.time()
        fun(*args, **kwargs)
        time2 = time.time()
        print("运行结束, 运行时常 => ", time2 - time1)
    return warper


class Storage:
    def __init__(self, *columns):
        #excel 或者 csv的列名
        self.storage_path = 0   #看存储文件是否存在
        self.data = []                    #存的数据
        self.columns = [i for i in columns]

    def add(self, data=None):
        self.data.append(data)

    def storage(self, path, data,mode="Excel"):
        header = False
        if self.storage_path == 0:
            if not os.path.exists(path):
                header = True
            self.storage_path = 1
        if mode == "Excel":
            excel_data = pd.DataFrame(data=data, columns=self.columns)
            excel_data.to_excel(path, index_label=False, index=False, encoding='utf-8', header=header, mode="a+")
            print(f"存储excel文件完成  ==> {path}")
        elif mode == "CSV":
            pandas_data = pd.DataFrame(data=data, columns=self.columns)
            pandas_data.to_csv(path, index_label=False, index=False, encoding='utf-8', header=header, mode="a+")
            print(f"存储csv文件完成  ==> {path}")
        else:
            pass
            self.storage_path = 1

class Clean:
    """xpath_clean"""
    @classmethod
    def xpath(cls, html, **kwargs):
        html_xpath = etree.HTML(html)
        data = {}
        for k, v in kwargs.items():
            data[k] = html_xpath.xpath(v)
        return data


class Sipider5i5j:
    def __init__(self, min_price, max_price,
                 domain="https://bj.5i5j.com/",
                 storagePath=DEFAULT_CSV_PATH,
                 storageMode='CSV',
                 max_list_page=5000):
        """
        min_price: 最低价格,
        max_price: 最高价格
        storagePath: 保存路径
        storageMode, 保存方式 两种 Excel 或者 CSV
        """
        self.domain = domain if domain[-1] != '/' else domain[:-1]
        self.max_list_page = max_list_page
        self.detail_q = Queue()
        self.list_page_stop = 0           #生产者 是否结束表示  消费者收到表示改变 并且队列为空则停止
        self.datail_page_stop = 0
        self.start_url = f"{self.domain}/zufang/b{min_price}e{max_price}o3r1r2r3r10u1u2"
        self.DB = Storage(
            "title", "房源ID", "价格", "支付方式", "户型", "楼层", "面积", "朝向", "小区", "楼型", "电梯",
            "供暖", "出租方式", "看房时间", "区域", "户型结构", "中介费", "腾空时间", "地铁", "服务费", "配套设施",
            "亮点", "户型介绍", "交通", "周边设施", "起租日期", "可签约至", "租售", "url"
        )
        self.storagePath = storagePath
        self.storageMode = storageMode
        self.headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                    "Cookie": "HMF_CI=3a3e735027edec65dfba67ffd0e1f8345b8c040262c09ca5bc2395f86d2f24317f; _ga=GA1.2.2056239572.1623115085; _dx_uzZo5y=c53cf618bf60d72c44f93e412efd6c98bdf7079edbd75c1124964b3b30c7086673c5a656; smidV2=202106080918052c70d505f1e2f42032b625b11d1800f500c740419b8d02580; gr_user_id=61309069-623d-425b-b3cd-9e6d3e519f32; yfx_c_g_u_id_10000001=_ck21060809181512179838583229918; __TD_deviceId=KL931NIAER3OBL4V; __tea_cookie_tokens_221025=%257B%2522web_id%2522%253A%25226972406977310377509%2522%252C%2522ssid%2522%253A%25227a003abf-c4ae-45ea-b5dd-07036b3cef82%2522%252C%2522user_unique_id%2522%253A%25226972406977310377509%2522%252C%2522timestamp%2522%253A1623390007188%257D; yfx_mr_n_10000001=baidu%3A%3Amarket_type_ocpc%3A%3A%3A%3Abaidu_ppc%3A%3A%3A%3A%3A%3A%25E5%258C%2597%25E4%25BA%25AC%25E6%2588%25BF%25E5%25A4%25A9%25E4%25B8%258B%25E5%259C%25B0%25E4%25BA%25A7%3A%3Awww.baidu.com%3A%3A92091967304%3A%3A%3A%3A%25E4%25BA%258C%25E6%2589%258B%25E6%2588%25BF%25E7%25AB%259E%25E5%2593%2581%25E8%25AF%258D%3A%3A%25E6%2588%25BF%25E5%25A4%25A9%25E4%25B8%258B%3A%3A36%3A%3Apmf_from_adv%3A%3Abj.5i5j.com%2Fershoufang%2F; yfx_key_10000001=; yfx_mr_f_n_10000001=baidu%3A%3Amarket_type_ocpc%3A%3A%3A%3Abaidu_ppc%3A%3A%3A%3A%3A%3A%25E5%258C%2597%25E4%25BA%25AC%25E6%2588%25BF%25E5%25A4%25A9%25E4%25B8%258B%25E5%259C%25B0%25E4%25BA%25A7%3A%3Awww.baidu.com%3A%3A92091967304%3A%3A%3A%3A%25E4%25BA%258C%25E6%2589%258B%25E6%2588%25BF%25E7%25AB%259E%25E5%2593%2581%25E8%25AF%258D%3A%3A%25E6%2588%25BF%25E5%25A4%25A9%25E4%25B8%258B%3A%3A36%3A%3Apmf_from_adv%3A%3Abj.5i5j.com%2Fershoufang%2F; PHPSESSID=u7hii9br1bld3cv24kkjj40lpq; domain=bj; historyCity=%5B%22%5Cu5317%5Cu4eac%22%5D; pc_pmf_group_bj=c06dd55fcad35682b98e9e44bbdfde4e403fe416c1ee50a643fdd58c7d9543b6a%3A2%3A%7Bi%3A0%3Bs%3A15%3A%22pc_pmf_group_bj%22%3Bi%3A1%3Bs%3A154%3A%22%7B%22pmf_group%22%3A%22baidu%22%2C%22pmf_medium%22%3A%22ppzq%22%2C%22pmf_plan%22%3A%22%5Cu5de6%5Cu4fa7%5Cu6807%5Cu9898%22%2C%22pmf_unit%22%3A%22%5Cu6807%5Cu9898%22%2C%22pmf_keyword%22%3A%22%5Cu6807%5Cu9898%22%2C%22pmf_account%22%3A%22160%22%7D%22%3B%7D; baidu_OCPC_pc=7ec231ff9b700749887322edaff1a007813ca3668b791e02e38ce503395aa14ea%3A2%3A%7Bi%3A0%3Bs%3A13%3A%22baidu_OCPC_pc%22%3Bi%3A1%3Bs%3A178%3A%22%22http%3A%5C%2F%5C%2Fbjh.5i5j.com%5C%2F%3Fpmf_group%3Dbaidu%26pmf_medium%3Dppzq%26pmf_plan%3D%25E5%25B7%25A6%25E4%25BE%25A7%25E6%25A0%2587%25E9%25A2%2598%26pmf_unit%3D%25E6%25A0%2587%25E9%25A2%2598%26pmf_keyword%3D%25E6%25A0%2587%25E9%25A2%2598%26pmf_account%3D160%22%22%3B%7D; _gid=GA1.2.1299598005.1625016868; 8fcfcf2bd7c58141_gr_session_id=1391d417-c42b-4252-9622-cbb764abe421; 8fcfcf2bd7c58141_gr_session_id_1391d417-c42b-4252-9622-cbb764abe421=true; _Jo0OQK=75160DEF080430262C18FC3C431600FF3642224520260E07059DA9BEBCA74EB49E2783DFC147A089CC22F45361BC9D5ED0E6ED6B65EA93B9D40501F284C1168CC6B26107A0B847CA53375B268E06EC955BB75B268E06EC955BB9D992FB153179892GJ1Z1ZQ==; Hm_lvt_94ed3d23572054a86ed341d64b267ec6=1624408592,1624519846,1625016868,1625017045; zufang_BROWSES=90124948%2C90045919%2C90076561%2C90153637%2C90238177%2C90239949%2C90231987%2C501495142%2C90051789%2C90212408%2C90174795%2C90126850%2C90045091%2C90163454%2C501478442; yfx_f_l_v_t_10000001=f_t_1623115095206__r_t_1625017046796__v_t_1625023039310__r_c_4; _gat=1; Hm_lpvt_94ed3d23572054a86ed341d64b267ec6=1625023051",
                    "Upgrade-Insecure-Requests": '1',
                    "Sec-Fetch-User": '?1',
                }

    def get_html(self, url, string):
        html = requests.get(url, headers=self.headers).text
        wscckey = re.findall(r'<HTML><HEAD><script>window\.location\.href="(.*?)";</script></HEAD><BODY>', html)
        if wscckey:
            url = wscckey[0]
            html = requests.get(url, headers=self.headers).text
        print(f"长在抓取{string} : {url}")
        return html

    def get_detail_urls(self, html, xpath):
        """提取列表页中详情页的url 将域名补充完整"""
        detail_urls = Clean.xpath(html, **xpath)
        detail_urls = [f"{self.domain}{i}" for i in detail_urls["detail_urls"]]
        return detail_urls

    def parse_detail(self, html, url):
        """解析详情页数据"""
        xpath = {
            "title": 'string(//h1[@class="house-tit"])',
            "房源ID": 'string(//span[@class="del-houseid"])',
            '价格': 'string(//p[@class="de-price"]/span)',
            '支付方式': 'string(//span[@class="yafu "])',
            '户型': 'string(//div[@class="jlyoubai fl jlyoubai1"]//p[@class="houseinfor1"])',
            '楼层': 'string(//div[@class="jlyoubai fl jlyoubai1"]//p[@class="houseinfor2"])',
            "面积": 'string(//div[@class="jlyoubai fl jlyoubai2"]//div[@class="jlquannei"]//p[@class="houseinfor1"])',
            "朝向": 'string(//div[@class="jlyoubai fl jlyoubai3"]//div[@class="jlquannei"]//p[@class="houseinfor1"])',
            "配套设施": '//ul[@class="fysty"]//text()',
            "亮点": 'string(//ul[@class="fytese"]/child::li[1]/label)',
            "户型介绍": 'string(//ul[@class="fytese"]/child::li[2]/label)',
            '交通': 'string(//ul[@class="fytese"]/child::li[3]/label)',
            '周边设施': 'string(//ul[@class="fytese"]/child::li[4]/label)',
            "小区信息": 'string(//ul[@class="fytese"]/child::li[4]/label)',
            "租售": '//div[@class="zushous"]//text()'
        }
        data = Clean.xpath(html, **xpath)
        data['租售'] = re.sub(r'\s+', "\n", '\n'.join(data['租售'])).replace("无", "无\n").replace('：\n', ': ')
        data["url"] = url
        data2 = data['租售'].split('\n')
        for i in data2:
            if ":" in i:
                k, v = i.split(":")
                data[k] = v
        data['户型'] = re.sub('\s', '', data["户型"])
        data['支付方式'] = data['支付方式'].replace("(支付方式：", '').replace(")", '')
        data['房源ID'] = data["房源ID"].replace("房源ID：", "")
        data['配套设施'] = re.sub(r'\s+', '',",".join(data["配套设施"]))[1:-1]
        d = {}
        for i in self.DB.columns:
            d[i] = data.get(i, 'None')
        return d

    def clean_detail(self, detail_url):
        """处理详情页   1. 获取网页源码  2. 解析网页源码  3. 将解析后的数据加入带存储列表"""
        detail_html = self.get_html(detail_url, "房源信息页")
        house_data = self.parse_detail(detail_html, url=detail_url)
        self.DB.add(house_data)

    def get_detail_urs(self):
        """生产者  抓取列表页 生产详情页url种子"""
        i = 1
        while True:
            print(f"正在抓取第{i}页")
            list_url = f'{self.start_url}n{i}/'     #每一页列表页数据
            list_html = self.get_html(list_url, "列表页")    #获取列表页数据
            detail_urls = self.get_detail_urls(list_html, {"detail_urls": '//h3[@class="listTit"]/a/@href'})  #解析列表页 获取详情页url
            [self.detail_q.put(j)  for j in detail_urls]
            if not detail_urls or (i > self.max_list_page):    #如果没有detail  则退出循环结束生产者
                break
            i += 1
        #生产者结束
        self.list_page_stop = 1
        print("列表页访问完毕")

    def down_detail_data(self):
        """多线程下载详情页数据并且保存  消费者"""
        print("消费者启动")
        while True:
            ts = []
            for i in range(100):
                if not self.detail_q.empty():
                    detail_url = self.detail_q.get()
                    ts.append(Thread(target=self.clean_detail, args=(detail_url, )))
            [t.start() for t in ts]
            [t.join() for t in ts]
            #队列为空 并且 生产者结束  推出循环
            if self.list_page_stop == 1 and self.detail_q.empty():
                print("生产者结束, 队列为空")
                print("爬取结束")
                break
        self.datail_page_stop = 1

    def storage(self):
        print("存储启动")
        while True:
            data = []
            for i in range(200):
                if self.DB.data:
                    data.append(self.DB.data.pop())
            self.DB.storage(self.storagePath, data, mode=self.storageMode)
            if not self.DB.data and self.datail_page_stop == 1:
                break
            time.sleep(2)
        print("存储完成")

    def run(self):
        t1 = Thread(target=self.get_detail_urs)
        t2 = Thread(target=self.down_detail_data)
        t3 = Thread(target=self.storage)
        t1.start()
        t2.start()
        t3.start()
        t1.join()
        t2.join()
        t3.join()


@timer
def start_spider(spider_setting):
    #最低价格  最高价格  保存路径(默认为当前执行路径)  保存文件格式 (Excel 与 csv 默认 csv)  换省份改一下domain就行
    Sipider5i5j(
        min_price=spider_setting["最低价"],
        max_price=spider_setting["最高价"],
        domain=spider_setting["域名"],
        storagePath=spider_setting["存储路径"],
        storageMode=spider_setting["保存文件格式"],
        max_list_page=spider_setting["抓取页码数量"]
    ).run()


def get_domain():
    """返回所有城市的域名以及"""
    html = requests.get("https://cd.5i5j.com/").text
    domain_data = Clean.xpath(html, **{"domain_urls": '//ul[@class="city-list clearfix font-family-Normal font-samll"]//ul[@class="clearfix city-group"]//li/a/@href',
                                       "domain_name": '//ul[@class="city-list clearfix font-family-Normal font-samll"]//ul[@class="clearfix city-group"]//li/a//text()'})
    temp_num = len(domain_data["domain_urls"])
    domains = {}
    for i in range(temp_num):
        domains[domain_data["domain_name"][i]] = domain_data["domain_urls"][i]
    return domains


def main():
    # 最低价格  最高价格  保存路径(默认为当前执行路径)  保存文件格式 (Excel 与 csv 默认 csv)  换省份改一下domain就行
    print(ABSDIR_PATH)
    domains = get_domain()
    hours = {i: v for i, v in enumerate(domains.keys())}
    print("可以抓取以下城市的租房信息: \n输入你想抓取的城市的房源: 输入数字即可 默认0直接回车", )
    for k, v in hours.items():
        print(f"{k}. {v}")
    domain = input("请输入: ")
    domain = domains[hours[int(domain)]] if domain else domains[hours[0]]
    min_price = input("请输入最小的房租价格: 默认500直接回车")
    max_price = input("请输入最大的房租价格: 默认5000直接回车")
    print("保存文件格式 1. CSV  2. Excel  (都能用excel打开, excel存储url上限为65555)  输入数字即可 默认直接回车")
    try:
        storageMode = int(input("选择保存格式"))
        if storageMode == 1:
            storageMode = "CSV"
        elif storageMode == 2:
            storageMode = "Excel"
        else:
            storageMode = "CSV"
    except:
        storageMode = "CSV"
    print(r"""
        如果为CSV格式  路径为  XXXX/XXX/XXX.CSV 例如 C:\Users\xq8\Desktop\新建文件夹 (2)\5i5j.csv
        如果为Excel格式  路径为  XXXX/XXX/XXX.xlsx 例如 C:\Users\xq8\Desktop\新建文件夹 (2)\5i5j.xlsx
        """)
    storagePath = input("请输入保存路径, 默认路径直接回车")
    if not storagePath:
        storagePath = DEFAULT_CSV_PATH
    try:
        max_list_page = int(input("请输入抓取页码数量: 默认值1000直接回车"))
    except:
        max_list_page = 1000
    spider_setting = {
        "最低价": int(min_price) if min_price else 500,
        "最高价": int(max_price) if max_price else 5000,
        "域名": domain,
        "存储路径": storagePath,  # r'C:\Users\xq8\Desktop\新建文件夹 (2)\5i5jtest.csv'
        "保存文件格式": storageMode,  # 或者csv  csv后缀为.csv
        "抓取页码数量": max_list_page
    }
    start_spider(spider_setting)
    print("运行完毕")

if __name__ == "__main__":
    main()

