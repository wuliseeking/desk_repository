import re
from urllib import parse
import urllib
import time
from datetime import datetime
from urllib import robotparser
from collections import deque  #deque一种实现高效删除和插入的双向列表，在多线程中可同时在两端进行操作。


def link_crawler(seed_url, link_regex=None, delay=5, max_depth=10, max_urls=100, headers=None, user_agent='wswp', proxy=None, num_retries=1):
    """
    爬取符合正则表达式限制的格式的URL地址:
	根据传入的参数配置URL下载的相关信息（下载时间间隔，robots条例，用户代理）
	进入下载循环{
        1.下载（取待下载列表尾端的URL，根据robots判断是否能下载,是否需延时下载，然后下载），
	2.添加新URL（获取当前下载页面深度，没达到深度限制，解析出当前下载页面中包含满足正则的URL，标准格式化URL，将没遇到过的URL添加进记录所有遇到过的URL的列表，记录这些子URL的深度（父URL的深度+1），将与种子URL相同域名的子URL           加入待下载列表），
	3.记录下载URL的数（num_urls += 1）并判断是否达到最大下载数，是否进入下一次循环
                   }
    """
    crawl_queue = deque([seed_url])
    seen = {seed_url: 0}
    num_urls = 0
    
    rp = get_robots(seed_url)  #get_robots() 自定义函数 返回一个已解析的URL的robots.txt的内容对象
    throttle = Throttle(delay) #创建下载频率调控类实例
    headers = headers or {}
    if user_agent:
        headers['User-agent'] = user_agent
    while crawl_queue:
        url = crawl_queue.pop()           #删除待爬取队列尾端的元素，并返回这个元素的值
        
        if rp.can_fetch(user_agent, url): # 只通过种子URL页面的robots.txt 判断这个 用户代理 是否被允许下载这个URL
            throttle.wait(url)            #能下载就使相同域名的URL下载时间间隔 达到 设置的秒数
            html = download(url, headers, proxy=proxy, num_retries=num_retries)  #下载页面 返回下载页面的html文本
            #记录当前页面中所有满足正则表达式的URL
            links = []
            depth = seen[url]
            if depth != max_depth: #从原始页面，到当前页面，经过了多少个链接，也就是深度。
                if link_regex: #如果设置了URL的格式标准（正则表达式），提取当前下载的页面中满足正则表达式的URL，并添加到待抓取的URL列表中
                    links.extend(link for link in get_links(html) if re.match(link_regex, link)) #extend() 函数用于在列表末尾一次性追加另一个序列中的多个值
                    
                for link in links:
                    link = normalize(seed_url, link)  #标准化URL格式，去掉后面的fragment ,使URL变为绝对URL
                    # 当这个连接没有被放入seen队列时，把这个连接加入seen队列中，并记录它的深度。
                    if link not in seen:  #无重复URL才会被放到 下载队列crawl_queue当中
                        seen[link] = depth + 1 #记录 子URL的深度
                        if same_domain(seed_url, link):
                            crawl_queue.append(link)

            # 记录已下载页面数
            num_urls += 1
            if num_urls == max_urls:
                break
        else:
            print('Blocked by robots.txt:', url)


class Throttle:
    """
    控制同一个域名的下载时间间隔：
	类实例设置了统一的下载时间间隔and记载了已下载的URL域名的上次下载时间)，方法wait针对单个URL使其满足同域名的下载时间间隔，
	使用该功能时，先创建类实例(传入间隔的时间)，再根据某个URL调用类实例的wait(url)方法，实现针对该URL的下载时间间隔调控
	wait内在逻辑为，该URL的域名上次运行时间到当前时间不足间隔时间时，暂停直到满足时间间隔后，将当前时间作为该域名的value值
    """
    def __init__(self, delay):
        self.delay = delay
        #存储 域名(key)：上次下载时间(value)  的数据容器（字典）
        self.domains = {}
        
    def wait(self, url):
        domain = parse.urlparse(url).netloc  #解析URL然后获得它的netloc(域名)部分
        last_accessed = self.domains.get(domain)

        if self.delay > 0 and last_accessed is not None:
            sleep_secs = self.delay - (datetime.now() - last_accessed).seconds
            if sleep_secs > 0:
                time.sleep(sleep_secs)  #暂停使时间间隔满足
        self.domains[domain] = datetime.now() #存储当前时间为该域名的value值。
        


def download(url, headers, proxy, num_retries, data=None):
    '''
    返回下载页面的html文本
	可以处理代理，传输数据，头文件，并检测由于服务器问题下载失败时在规定次数内再次下载。
    '''
    print('Downloading:', url)
    request = urllib.request.Request(url, data, headers)
    opener = urllib.request.build_opener()
    if proxy:
        proxy_params = {parse.urlparse(url).scheme: proxy} #获取URL中 的协议作为 key， proxy是一个url?
        opener.add_handler(urllib.request.ProxyHandler(proxy_params))
    try:
        response = opener.open(request)
        html = response.read()
        code = response.code
    except urllib.error.URLError as e:
        print('Download error:', e.reason)
        html = ''
        if hasattr(e, 'code'):
            code = e.code
            if num_retries > 0 and 500 <= code < 600:  #如果是服务器端的报错，并且连续出错次数小于设定值，就继续尝试。
                # retry 5XX HTTP errors
                return download(url, headers, proxy, num_retries-1, data)
        else:
            code = None
    try:
        html=html.decode('utf-8') 
    except: 
        pass  
    return html 


def normalize(seed_url, link):
    """标准化URL格式，去掉后面的fragment ,如果是相对URL，则转化为绝对URL
    """
    link, _ = parse.urldefrag(link) #将URL中的fragment和前面的URL分开 
    return parse.urljoin(seed_url, link) #避免link是相对路径，把其转换为绝对路径。


def same_domain(url1, url2):
    """
	判断两个URL是否是同一个域名
    """
    return parse.urlparse(url1).netloc == parse.urlparse(url2).netloc


def get_robots(url):
    """
	初始化robots文件，返回一个已解析的URL的robots.txt的内容对象
    """
    rp = robotparser.RobotFileParser() #创建类实例 该class提供读取、解析和回答关于对应url的robots.txt问题的方法
    rp.set_url(parse.urljoin(url, '/robots.txt'))  #set_url(url) 设置与robots.txt相关联的URL,  parse.urljoin(base,url,allow_fragments=True)合并路径。
    rp.read()    #读取robots.txt中的URL 并把它传递给parser
    return rp    
    #rp.can_fetch(useragent,url) 如果根据被解析的robots.txt文件中的规则,该userAgent被允许爬取这个URL,则返回TRUE。如果不被允许，仍然爬取，就可能被封号。
        

def get_links(html):
    """
	查找html文件中的<a标签下的href属性值，以列表形式返回。
    """
    # a regular expression to extract all links from the webpage
    webpage_regex = re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE) #忽略大小写
    # list of all links from the webpage
    return webpage_regex.findall(html)  #返回html文本中所有<a元素下href属性的值

import time 
if __name__ == '__main__':
    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'
    link_crawler('http://example.webscraping.com', r'/places/default/(index|view)', delay=0, num_retries=1, user_agent='BadCrawler')
    time.sleep(5)
    link_crawler('http://example.webscraping.com/index', r'(.*?)/(index|view)', delay=0, num_retries=1, max_depth=1, user_agent=user_agent)
