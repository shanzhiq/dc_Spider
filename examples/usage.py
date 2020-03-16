import requests
from lxml import etree
# from selenium import webdriver
# from selenium.common.exceptions import TimeoutException
# from selenium.common.exceptions import WebDriverException
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.support.wait import WebDriverWait
# from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from conn_neo4j import Neo4jRunner
import threading
from loguru import logger
import random,json
delete_url='http://127.0.0.1:5555/delete'
proxypool_url = 'http://127.0.0.1:5555/random'
target_url = 'http://quote.eastmoney.com/stock_list.html'

# 改为多线程，对数据直接写入数据库，无需对文件加锁，用公司的股票代码对文件处理，注意人名的处理，避免重复节点
def get_random_proxy():
    """
    get random proxy from proxypool
    :return: proxy
    """
    url=requests.get(proxypool_url).text.strip()
    #logger.info("now url is",url)
    return url


def crawl(url):
    """
    use proxy to crawl page
    :param url: page url
    :param proxy: proxy, such as 8.8.8.8:8888
    :return: html
    """
    while True:
        try:
            proxy=get_random_proxy()
            proxies = {'http': 'http://' + proxy}
            logger.info(proxies)
            resp = requests.get(url, proxies=proxies,timeout=3)  # 设置代理，抓取每个公司的连接
            resp.encoding = resp.apparent_encoding  # 可以正确解码
            if resp.status_code==200:
                html = etree.HTML(resp.text)
                logger.info("成功获得公司信息url!!!")
                break
            else:
                continue
        except:
            logger.info("没获取到")
            continue
    return html

def save_to_neo4j(company_codes,begin,end):
    threads = ["none"] * 4
    _len = len(company_codes)
    begin=383;
    end=int((_len-383)/4)+begin
    for i in range(4):
        threads[i]= threading.Thread(target=thread_collect, args=(begin,end,company_codes))
        begin=begin+int((_len-383)/4)
        end=end+int((_len-383)/4)
        threads[i].start()
    for i in range(4):
        threads[i].join()

def thread_collect(begin,end,codes):
    neo4j_driver = Neo4jRunner()
    for i in range(begin,end):
        t=get_info(codes[i])
        # time.sleep(2)
        if t:
            try:
                logger.info(f"company info {t['jbzl']['gsmc']}")
                neo4j_driver.do_all(t)
            except Exception as e:
                logger.info(f"unable to find {codes[i]} ,company info {t}") #这是对于非上市公司的情况做的处理
        else:
            logger.info(f"unable to find {codes[i]}")
    logger.info("one Thread has finish!")

def get_info(com_code):
    # 根据url获取公司信息
    # 获取公司概况的url
    t=dict()
    times=0
    while not t and times<10: #执行超过10次就放弃，并退出
        times=times+1
        pro_url = get_random_proxy()
        com_url="http://f10.eastmoney.com/CompanySurvey/CompanySurveyAjax?code="+com_code
        #print(com_url)
        USER_AGENTS = [
            "Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1"
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
            "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
            "Opera/9.80 (Windows NT 5.1; U; zh-cn) Presto/2.9.168 Version/11.50",
            "Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0",
            "Mozilla/5.0 (Windows NT 5.2) AppleWebKit/534.30 (KHTML, like Gecko) Chrome/12.0.742.122 Safari/534.30",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0",
            "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.2)",
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
            "Mozilla/4.0 (compatible; MSIE 5.0; Windows NT)",
            "Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070309 Firefox/2.0.0.3",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070803 Firefox/1.5.0.12 "
        ]

        headers = {
            'User-agent': random.choice(USER_AGENTS),  # 设置get请求的User-Agent，用于伪装浏览器UA
        }
        proxies = {'http': 'http://' + pro_url}
        try:
            res = requests.get(com_url, headers=headers, proxies=proxies,timeout=3)
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                t = json.loads(res.text)
                # print(t)
                break
            else:
                continue
        except:
            continue
    if times>10:
        logger.info(f"unable to visit {com_url},and status is {res.status_code}") #考虑到个别网站无法访问
    return t




def main():
    """
    main method, entry point
    :return: none
    """
    proxy = get_random_proxy()
    html = crawl(target_url)
    company_all_url = html.xpath('//*[@id="quotesearch"]/ul/li/a/@href')
    code=['none']*len(company_all_url)
    for i in range(len(company_all_url)):
        s = str(str(company_all_url[i]))
        code[i]=s[(len(s) - 13):(len(s) - 5)]
    save_to_neo4j(code,0,len(code))



if __name__ == '__main__':
    main()
# 当前606