import datetime
import re
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import pboc_initial_database as db
import concurrent.futures

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

# 仅保留上海市和海南省用于测试
PROVINCE_SITES = [
    {"province": "上海市", "base_url": "https://shanghai.pbc.gov.cn/fzhshanghai/113577/114832/114918/index.html"},
    {"province": "北京市", "base_url": "https://beijing.pbc.gov.cn/beijing/132030/132052/132059/index.html"},
    {"province": "天津市", "base_url": "https://tianjin.pbc.gov.cn/fzhtianjin/113682/113700/113707/index.html"},
    {"province": "重庆市", "base_url": "https://chongqing.pbc.gov.cn/chongqing/107680/107897/107909/index.html"},
    {"province": "深圳市", "base_url": "https://shenzhen.pbc.gov.cn/shenzhen/122811/122833/122840/index.html"},
    {"province": "大连市", "base_url": "https://dalian.pbc.gov.cn/dalian/123812/123830/123837/index.html"},    
    {"province": "青岛市", "base_url": "https://qingdao.pbc.gov.cn/qingdao/126166/126184/126191/index.html"},
    {"province": "厦门市", "base_url": "https://xiamen.pbc.gov.cn/xiamen/127703/127721/127728/index.html"},
    {"province": "宁波市", "base_url": "https://ningbo.pbc.gov.cn/ningbo/127076/127098/127105/index.html"},    
    {"province": "黑龙江省", "base_url": "https://haerbin.pbc.gov.cn/haerbin/112693/112776/112783/index.html"},
    {"province": "吉林省", "base_url": "https://changchun.pbc.gov.cn/changchun/124680/124698/124705/index.html"},
    {"province": "辽宁省", "base_url": "https://shenyang.pbc.gov.cn/shenyfh/108074/108127/108208/index.html"},
    {"province": "河北省", "base_url": "https://shijiazhuang.pbc.gov.cn/shijiazhuang/131442/131463/131472/index.html"},
    {"province": "河南省", "base_url": "https://zhengzhou.pbc.gov.cn/zhengzhou/124182/124200/124207/index.html"},
    {"province": "山西省", "base_url": "https://taiyuan.pbc.gov.cn/taiyuan/133960/133981/133988/index.html"},    
    {"province": "山东省", "base_url": "https://jinan.pbc.gov.cn/jinan/120967/120985/120994/index.html"},    
    {"province": "内蒙古自治区", "base_url": "https://huhehaote.pbc.gov.cn/huhehaote/129797/129815/129822/index.html"},    
    {"province": "安徽省", "base_url": "https://hefei.pbc.gov.cn/hefei/122364/122382/122389/index.html"},
    {"province": "湖北省", "base_url": "https://wuhan.pbc.gov.cn/wuhan/123472/123493/123502/index.html"},
    {"province": "湖南省", "base_url": "https://changsha.pbc.gov.cn/changsha/130011/130029/130036/index.html"},
    {"province": "海南省", "base_url": "https://haikou.pbc.gov.cn/haikou/132982/133000/133007/index.html"},    
    {"province": "江苏省", "base_url": "https://nanjing.pbc.gov.cn/nanjing/117542/117560/117567/index.html"},     
    {"province": "江西省", "base_url": "https://nanchang.pbc.gov.cn/nanchang/132372/132390/132397/index.html"},
    {"province": "浙江省", "base_url": "https://hangzhou.pbc.gov.cn/hangzhou/125268/125286/125293/index.html"},    
    {"province": "广东省", "base_url": "https://guangzhou.pbc.gov.cn/guangzhou/129142/129159/129166/index.html"},
    {"province": "福建省", "base_url": "https://fuzhou.pbc.gov.cn/fuzhou/126805/126823/126830/index.html"},
    {"province": "广西壮族自治区", "base_url": "https://nanning.pbc.gov.cn/nanning/133346/133364/133371/index.html"},    
    {"province": "贵州省", "base_url": "https://guiyang.pbc.gov.cn/guiyang/113288/113306/113313/index.html"},
    {"province": "四川省", "base_url": "https://chengdu.pbc.gov.cn/chengdu/129320/129341/129350/index.html"},
    {"province": "云南省", "base_url": "https://kunming.pbc.gov.cn/kunming/133736/133760/133767/index.html"},
    {"province": "西藏自治区", "base_url": "https://lasa.pbc.gov.cn/lasa/120480/120504/120511/index.html"},
    {"province": "陕西省", "base_url": "https://xian.pbc.gov.cn/xian/129428/129449/129458/index.html"},
    {"province": "甘肃省", "base_url": "https://lanzhou.pbc.gov.cn/lanzhou/117067/117091/117057/index.html"},
    {"province": "青海省", "base_url": "https://xining.pbc.gov.cn/xining/118239/118263/118270/index.html"},
    {"province": "宁夏回族自治区", "base_url": "https://yinchuan.pbc.gov.cn/yinchuan/119983/120001/120008/index.html"},
    {"province": "新疆维吾尔自治区", "base_url": "https://wulumuqi.pbc.gov.cn/wulumuqi/121755/121777/121784/index.html"},
]

FILE_EXTS = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".et", ".zip", ".rar")

# 特殊处理的省份（如使用不同HTML结构的省份）
SPECIAL_PROVINCES = ["北京市", "天津市", "上海市", "宁波市", "深圳市", "大连市", "青岛市", "厦门市"]

# Configure a global session for connection pooling
SESSION = requests.Session()
# Enable retries and connection pooling
adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=3)
SESSION.mount('http://', adapter)
SESSION.mount('https://', adapter)

def fetch(url):
    try:
        # Use session for connection reuse and set a reasonable timeout (5s)
        # timeout is a deadline, not a delay; too short causes failures
        r = SESSION.get(url, headers=HEADERS, timeout=5)
        if r.status_code != 200:
            print(f"Failed to fetch {url}: status {r.status_code}")
            return None
        r.encoding = r.apparent_encoding or "utf-8"
        return r.text
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def normalize_href(base, href):
    if not href or href.startswith("javascript"):
        return None
    return urljoin(base, href)

def list_pages(base_url, max_pages=100):
    """
    获取分页链接。
    参考 model1.py 的逻辑，增强对各类分页结构的兼容性。
    """
    pages = {base_url}
    first_html = fetch(base_url)
    if not first_html:
        return list(pages)
    soup = BeautifulSoup(first_html, "lxml")
    
    # 1. Try to find multiple pagination inputs (for multiple portlets)
    inputs = soup.find_all("input", attrs={"name": "article_paging_list_hidden"})
    found_any_pattern = False
    
    for inp in inputs:
        total = 1
        for attr in ("totalpage", "totalPage", "total"):
            if inp.has_attr(attr):
                try:
                    total = int(inp[attr])
                    break
                except Exception:
                    pass
        if total <= 1:
            continue
            
        # Scope search to the container of this input
        container = inp.find_parent(attrs={"opentype": "page"}) or inp.find_parent(class_="portlet")
        if not container:
            # Also try to find parent with class "portlet" if it's not a div (e.g. table)
            # The above find_parent(class_="portlet") should cover it regardless of tag
            pass
            
        if not container:
            continue
            
        links = container.select("div.list_page a[href]")
        pattern = None
        
        for a in links:
            href = normalize_href(base_url, a.get("href"))
            if not href:
                continue
            # Pattern: ...-2.html or ..._2.html
            m1 = re.search(r"(.+?)-(\d+)\.html$", href)
            m2 = re.search(r"(.*?/index)_(\d+)\.html$", href)
            
            if m1:
                pattern = re.sub(r"-(\d+)\.html$", "-%d.html", href)
            elif m2:
                pattern = re.sub(r"_(\d+)\.html$", "_%d.html", href)
            
            if pattern:
                found_any_pattern = True
                break
        
        if pattern:
            limit = min(total, max_pages)
            for i in range(2, limit + 1):
                pages.add(pattern % i)
        elif inp.has_attr("moduleid") and total > 1:
            # Fallback: Construct pattern from moduleid if no links found
            # e.g. moduleid="10983" -> 10983-2.html
            module_id = inp["moduleid"]
            pattern = urljoin(base_url, f"{module_id}-%d.html")
            limit = min(total, max_pages)
            for i in range(2, limit + 1):
                pages.add(pattern % i)
            found_any_pattern = True

    # 2. Fallback: If no portlet-specific patterns found, try global search
    # This handles pages that don't use the multi-portlet hidden input structure
    if not found_any_pattern:
        candidates = soup.select("div.list_page a[href]") or soup.find_all("a", href=True)
        pattern = None
        max_num = 1
        
        # Try to infer pattern from any link looking like page 2
        for a in candidates:
            href = normalize_href(base_url, a.get("href"))
            if not href:
                continue
            m1 = re.search(r"(.+?)-(\d+)\.html$", href)
            m2 = re.search(r"(.*?/index)_(\d+)\.html$", href)
            if m1:
                num = int(m1.group(2))
                max_num = max(max_num, num)
                pattern = re.sub(r"-(\d+)\.html$", "-%d.html", href)
            elif m2:
                num = int(m2.group(2))
                max_num = max(max_num, num)
                pattern = re.sub(r"_(\d+)\.html$", "_%d.html", href)
        
        # Also check implicit index_%d pattern
        if not pattern and base_url.endswith("index.html"):
            pattern = base_url.replace("index.html", "index_%d.html")
            
        # If we found a pattern, how many pages?
        # Try to find a global totalpage input if we missed it above (unlikely but possible)
        total = 1
        if inputs: # If we had inputs but couldn't match patterns, maybe use the max total?
            for inp in inputs:
                for attr in ("totalpage", "totalPage", "total"):
                     if inp.has_attr(attr):
                         try:
                             total = max(total, int(inp[attr]))
                         except: pass
        
        if total <= 1 and max_num > 1:
            total = max_num
            
        if pattern and total > 1:
            limit = min(total, max_pages)
            for i in range(2, limit + 1):
                pages.add(pattern % i)

    return sorted(list(pages))

def parse_standard_branch_page(soup, page_url, province_name):
    """
    解析标准格式的页面 (如海南省)
    """
    items = []
    # 查找所有 portlet (可能包含省级分行和辖内分支机构)
    portlets = soup.select('div.txtbox_2.portlet[opentype="page"]')
    
    if not portlets:
        # 尝试查找单一列表结构
        portlets = soup.select('div.txtbox_2.portlet')

    for portlet in portlets:
        # 尝试从标题判断分行名称
        title_span = portlet.find("span", class_="portlettitle2")
        title_text = title_span.get_text(strip=True) if title_span else ""
        
        branch_name = province_name + "分行" # 默认
        if "辖内" in title_text or "分支机构" in title_text:
             branch_name = "辖内分支机构"
        elif "省分行" in title_text or province_name in title_text:
             branch_name = province_name + "分行"

        # 查找列表项
        lis = portlet.select("ul.txtlist li")
        if not lis:
             # 有些页面可能直接是 ul
             lis = portlet.select("ul li")
             
        for li in lis:
            a = li.find("a", href=True)
            if not a:
                continue
            
            title = (a.get("title") or a.get_text(strip=True)).strip()
            href = normalize_href(page_url, a.get("href"))
            
            ds = li.find("span", class_="date")
            date_text = ds.get_text(strip=True) if ds else ""
            
            # 如果没有日期，尝试从文本中提取? (此处简化，若无日期则留空)
            
            items.append({
                "province": province_name,
                "branch": branch_name,
                "title": title,
                "url": href,
                "date": date_text
            })
    return items

def parse_special_branch_page(soup, page_url, province_name):
    """
    解析特殊格式的页面 (如北京市、深圳市、上海市)
    逻辑参考 model1.py，适配表格和列表两种结构
    """
    items = []
    # 北京/深圳/上海 等通常在 content_right 下
    container = soup.find("td", id="content_right")
    if not container:
        return items
    
    branch_name = province_name + "分行"
    seen_hrefs = set()
    
    # 策略1: 查找表格 (主要针对上海市等)
    tables = container.find_all("table", attrs={"width": "90%"})
    header_found = False
    for tbl in tables:
        # 检查表头
        ths = [td.get_text(strip=True) for td in tbl.find_all("td")]
        if ("公开信息名称" in ths) and ("生成日期" in ths):
            header_found = True
            continue # 跳过表头行
        
        # 如果还没找到表头，或者是表头行本身，继续
        # 注意：有些页面可能没有明确的thead，而是直接第一行tr
        # 这里逻辑简化：如果该表格包含指定表头，则该表格的后续行(或非表头行)被视为数据
        # 但 model1.py 的逻辑是：遍历所有 table，如果某个 table 是表头，标记 header_found=True。
        # 然后后续的 table (如果结构是分离的) 或者该 table 的后续行? 
        # model1.py 的逻辑其实是：遍历 content_right 下的所有 table (width=90%)
        # 如果遇到表头table，设置标志。
        # 如果标志为True，则尝试解析该table为数据行。
        # 这意味着表头和数据可能是不同的 table 标签？或者都在一个 table 里但被 find_all("table") 拆分了？
        # 通常 find_all("table") 不会拆分。除非是嵌套。
        # 假设是每行一个 table (旧式排版) 或者表头是一个 table，数据是后续 table。
        
        # 让我们照搬 model1.py 的逻辑，稍作调整以适应单一函数
        pass 

    # 重新实现 model1.py 的逻辑
    # 1. Tables
    tables = container.find_all("table", attrs={"width": "90%"})
    header_found = False
    
    for tbl in tables:
        ths = [td.get_text(strip=True) for td in tbl.find_all("td")]
        # 只有当明确发现是表头时，才标记，并跳过这一行(table)
        if ("公开信息名称" in ths) and ("生成日期" in ths):
            header_found = True
            continue
            
        # 如果还没找到表头，先跳过（防止误读前面的无关表格）
        # 除非该省份不需要表头验证（但上海需要）
        if not header_found and province_name == "上海市":
            continue
            
        a = tbl.find("a", href=True)
        if not a:
            continue
            
        title = (a.get("title") or a.get_text(strip=True)).strip()
        href = normalize_href(page_url, a.get("href"))
        
        # 尝试找日期
        tds = tbl.find_all("td")
        date_text = ""
        # 找到包含链接的 td 的索引，日期通常在它后面
        link_td_index = -1
        for i, td in enumerate(tds):
            if td.find("a", href=True):
                link_td_index = i
                break
        if link_td_index >= 0 and link_td_index + 1 < len(tds):
            date_text = tds[link_td_index + 1].get_text(strip=True)
            
        if href and title and (href not in seen_hrefs):
            items.append({
                "province": province_name,
                "branch": branch_name,
                "title": title,
                "url": href,
                "date": date_text
            })
            seen_hrefs.add(href)
            
    # 2. Lists (ul > li)
    # 即使 table 解析了，也尝试解析 ul (互补)
    uls = container.find_all("ul")
    if not uls:
        # 有些页面 ul 直接在 td 下
        uls = container.select("ul")
        
    for ul in uls:
        for li in ul.find_all("li"):
            a = li.find("a", href=True)
            if not a: continue
            
            title = (a.get("title") or a.get_text(strip=True)).strip()
            href = normalize_href(page_url, a.get("href"))
            
            if not href or href in seen_hrefs:
                continue
                
            ds = li.find("span", class_="date")
            date_text = (ds.get_text(strip=True) if ds else "") if ds else ""
            
            if not date_text:
                # 尝试从文本匹配日期
                m = re.search(r"\d{4}-\d{2}-\d{2}", li.get_text(" ", strip=True))
                if m:
                    date_text = m.group(0)
            
            items.append({
                "province": province_name,
                "branch": branch_name,
                "title": title,
                "url": href,
                "date": date_text
            })
            seen_hrefs.add(href)

    # 3. Fallback to TRs if no items found yet and not Shanghai (standard Beijing style)
    if not items and province_name != "上海市":
        trs = container.find_all("tr")
        for tr in trs:
            a = tr.find("a", href=True)
            if not a: continue
            title = (a.get("title") or a.get_text(strip=True)).strip()
            href = normalize_href(page_url, a.get("href"))
            if not href or href in seen_hrefs: continue
            
            text = tr.get_text(strip=True)
            date_match = re.search(r'\d{4}-\d{2}-\d{2}', text)
            date_text = date_match.group(0) if date_match else ""
            
            items.append({
                "province": province_name,
                "branch": branch_name,
                "title": title,
                "url": href,
                "date": date_text
            })
            seen_hrefs.add(href)
        
    return items

def parse_page_items(soup, page_url, province_name):
    if province_name in SPECIAL_PROVINCES:
        return parse_special_branch_page(soup, page_url, province_name)
    return parse_standard_branch_page(soup, page_url, province_name)

def save_to_db(items):
    """
    将爬取的数据保存到数据库
    判断“省份”+“分行”+“行政处罚文件”+“发布日期”+“下载链接”的内容是否与原表中任何一行有重复，
    如果重复则仅更新“数据更新时间”，否则插入新数据。
    """
    if not items:
        return

    try:
        conn = db.get_connection('fic')
        with conn.cursor() as cursor:
            # 检查重复的 SQL
            check_sql = """
            SELECT id FROM `pboc_penalty` 
            WHERE `省份`=%s AND `分行`=%s AND `行政处罚文件`=%s 
            AND `发布日期`<=>%s AND `下载链接`=%s
            """
            
            # 插入新数据的 SQL
            insert_sql = """
            INSERT INTO `pboc_penalty` 
            (`省份`, `分行`, `行政处罚文件`, `发布日期`, `下载链接`, `数据更新时间`, `数据类型`) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            # 更新数据的 SQL
            update_sql = """
            UPDATE `pboc_penalty` 
            SET `数据更新时间`=%s 
            WHERE `id`=%s
            """
            
            now = datetime.datetime.now()
            inserted_count = 0
            updated_count = 0
            
            for item in items:
                # 处理日期格式
                pub_date = None
                if item['date']:
                    try:
                        pub_date = datetime.datetime.strptime(item['date'], "%Y-%m-%d").date()
                    except ValueError:
                        pass # 日期格式不对则为 None
                
                # 检查是否存在
                cursor.execute(check_sql, (
                    item['province'],
                    item['branch'],
                    item['title'],
                    pub_date,
                    item['url']
                ))
                result = cursor.fetchone()
                
                if result:
                    # 存在则更新
                    existing_id = result['id']
                    cursor.execute(update_sql, (now, existing_id))
                    updated_count += 1
                else:
                    # 不存在则插入
                    cursor.execute(insert_sql, (
                        item['province'],
                        item['branch'],
                        item['title'],
                        pub_date,
                        item['url'],
                        now,
                        "行政处罚"
                    ))
                    inserted_count += 1

        conn.commit()
        print(f"数据库操作完成: 新增 {inserted_count} 条, 更新 {updated_count} 条。")
        conn.close()
    except Exception as e:
        print(f"保存数据库失败: {e}")

def process_single_page(page_url, prov):
    """
    处理单个页面：下载并解析
    """
    print(f"  正在处理页面: {page_url}")
    html = fetch(page_url)
    if not html:
        return []
    
    soup = BeautifulSoup(html, "lxml")
    return parse_page_items(soup, page_url, prov)

def run_spider(target_provinces=None, max_pages=5):
    """
    执行爬虫任务
    :param target_provinces: list, 指定要爬取的省份列表，如 ["北京市", "河北省"]。如果为 None，则爬取所有省份。
    :param max_pages: int, 每个省份最大爬取页数，默认为 5。
    """
    all_items = []
    seen_urls = set()

    for site in PROVINCE_SITES:
        prov = site['province']
        # 如果指定了 target_provinces 且当前省份不在列表中，则跳过
        if target_provinces is not None and prov not in target_provinces:
            continue
            
        base_url = site['base_url']
        print(f"开始爬取: {prov} - {base_url}")
        
        pages = list_pages(base_url, max_pages)
        print(f"找到 {len(pages)} 个页面")
        
        # 使用线程池并发抓取页面
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_url = {executor.submit(process_single_page, url, prov): url for url in pages}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    items = future.result()
                    count = 0
                    for item in items:
                        if item['url'] not in seen_urls:
                            seen_urls.add(item['url'])
                            all_items.append(item)
                            count += 1
                    print(f"    {url} 提取到 {count} 条新记录")
                except Exception as exc:
                    print(f"    {url} generated an exception: {exc}")
        
    print(f"爬取完成，共获取 {len(all_items)} 条记录，正在写入数据库...")
    save_to_db(all_items)

if __name__ == "__main__":
    run_spider()
