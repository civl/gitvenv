import datetime
import re
from urllib.parse import urljoin, urlparse
from flask import Flask, request, render_template_string
import requests
from bs4 import BeautifulSoup
import concurrent.futures

app = Flask(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}
KEY_WORDS = ["支付宝", "财付通", "拉卡拉", "快钱", "新生", "钱宝"]

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
CACHE = {"prov": None, "city": None, "records": None}
PROGRESS = {"status": "idle", "current": 0, "total": 0, "message": ""}

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
            return None
        r.encoding = r.apparent_encoding or "utf-8"
        return r.text
    except Exception:
        return None

def list_pages(base_url, max_pages=10):
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

def normalize_href(base, href):
    if not href:
        return None
    href = href.strip()
    if href.startswith("javascript"):
        return None
    if href == "#" or href.startswith("#"):
        return None
    u = urljoin(base, href)
    return u

SPECIAL_PROVINCES = {
    "北京市", "天津市", "上海市", "宁波市",
    "深圳市", "大连市", "青岛市", "厦门市"
}

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

def deduplicate_records(items):
    seen = set()
    res = []
    for it in items:
        u = it.get("url")
        if u and u not in seen:
            seen.add(u)
            res.append(it)
    return res

def collect_attachments(detail_url):
    html = fetch(detail_url)
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    atts = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = normalize_href(detail_url, a.get("href"))
        if not href:
            continue
        if href.lower().endswith(FILE_EXTS):
            name = a.get_text(strip=True) or href.split("/")[-1]
            if href not in seen:
                seen.add(href)
                atts.append({"name": name, "url": href})
    return atts

def filter_today(items, today_str):
    res = []
    for it in items:
        if it.get("date") == today_str:
            res.append(it)
    return res

def parse_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def sort_records(items):
    def _key(x):
        d = parse_date(x.get("date") or "")
        return d if d else datetime.date.min
    return sorted(items, key=_key, reverse=True)

def filter_by_range(items, range_key):
    if range_key == "all":
        return items
    today = datetime.date.today()
    if range_key == "year":
        start = today - datetime.timedelta(days=365)
    else:
        start = today - datetime.timedelta(days=30)
    res = []
    for it in items:
        d = parse_date(it.get("date") or "")
        if d and d >= start:
            res.append(it)
    return res

def get_all_data(force=False):
    if force or CACHE["records"] is None:
        records = []
        for site in PROVINCE_SITES:
            province = site["province"]
            pages = list_pages(site["base_url"], max_pages=20)
            for page in pages:
                html = fetch(page)
                if not html:
                    continue
                soup = BeautifulSoup(html, "lxml")
                items = parse_page_items(soup, page, province)
                records.extend(items)
        
        records = deduplicate_records(records)
        for it in records:
            it["attachments"] = collect_attachments(it["url"])
        CACHE["records"] = sort_records(records)
    return CACHE["records"]
def process_single_page(page, prov):
    html = fetch(page)
    if html:
        soup = BeautifulSoup(html, "lxml")
        return parse_page_items(soup, page, prov)
    return []

def _async_fetch_all():
    try:
        PROGRESS["status"] = "running"
        pages_map = []
        total_pages = 0
        for site in PROVINCE_SITES:
            pages = list_pages(site["base_url"], max_pages=50)
            pages_map.append({"province": site["province"], "pages": pages})
            total_pages += len(pages)
        
        PROGRESS["total"] = total_pages
        PROGRESS["current"] = 0
        records = []
        
        # Use ThreadPoolExecutor for concurrent page fetching
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_info = {}
            for entry in pages_map:
                prov = entry["province"]
                for page in entry["pages"]:
                    future = executor.submit(process_single_page, page, prov)
                    future_to_info[future] = page
            
            for future in concurrent.futures.as_completed(future_to_info):
                try:
                    items = future.result()
                    records.extend(items)
                except Exception as e:
                    print(f"Error processing {future_to_info[future]}: {e}")
                finally:
                    PROGRESS["current"] += 1
        
        records = deduplicate_records(records)
        PROGRESS["message"] = "解析完成，开始获取附件"
        PROGRESS["total"] = PROGRESS["current"] + len(records)
        
        # We can also parallelize attachment collection if needed, but keeping it simple for now
        # or we can use the same pattern if attachment fetching is slow
        for it in records:
            it["attachments"] = collect_attachments(it["url"])
            PROGRESS["current"] += 1
            
        CACHE["records"] = sort_records(records)
        PROGRESS["status"] = "done"
        PROGRESS["message"] = "完成"
    except Exception as e:
        PROGRESS["status"] = "error"
        PROGRESS["message"] = str(e)
def _async_fetch_one(province):
    try:
        target = None
        for s in PROVINCE_SITES:
            if s["province"] == province:
                target = s
                break
        if not target:
            PROGRESS["status"] = "error"
            PROGRESS["message"] = "未知省份"
            return
        PROGRESS["status"] = "running"
        pages = list_pages(target["base_url"], max_pages=50)
        PROGRESS["total"] = len(pages)
        PROGRESS["current"] = 0
        new_records = []
        # Use ThreadPoolExecutor for concurrent page fetching
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_url = {executor.submit(process_single_page, page, province): page for page in pages}
            for future in concurrent.futures.as_completed(future_to_url):
                try:
                    items = future.result()
                    new_records.extend(items)
                except Exception as e:
                    print(f"Error processing {future_to_url[future]}: {e}")
                finally:
                    PROGRESS["current"] += 1
        
        new_records = deduplicate_records(new_records)
        PROGRESS["message"] = "解析完成，开始获取附件"
        PROGRESS["total"] = PROGRESS["current"] + len(new_records)
        for it in new_records:
            it["attachments"] = collect_attachments(it["url"])
            PROGRESS["current"] += 1
        old = CACHE.get("records") or []
        others = [x for x in old if x.get("province") != province]
        CACHE["records"] = sort_records(others + new_records)
        PROGRESS["status"] = "done"
        PROGRESS["message"] = "完成"
    except Exception as e:
        PROGRESS["status"] = "error"
        PROGRESS["message"] = str(e)
INDEX_TMPL = """
<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8">
    <title>行政处罚文件汇总</title>
    <style>
      body { font-family: system-ui, -apple-system, "Segoe UI", Arial, sans-serif; margin: 20px; }
      .top { display:flex; justify-content: flex-start; align-items:center; margin-bottom:12px; }
      .note { color:#666; font-size:12px; }
      .filters { margin: 8px 0 16px; }
      table { width: 100%; border-collapse: collapse; }
      th, td { border: 1px solid #ddd; padding: 8px; }
      th { background: #f6f6f6; text-align: left; }
      .empty { color:#999; }
      .downloads a { margin-right: 6px; }
      .toolbar { display:flex; gap:12px; align-items:center; margin-bottom:12px; }
      .progress { width:240px; height:10px; background:#eee; border-radius:5px; overflow:hidden; display:inline-block; vertical-align:middle; }
      .bar { height:100%; width:0%; background:#0a4792; transition: width .2s ease; }
      select { padding:4px; }
      .range-buttons { display:inline-flex; gap:0; }
      .range-buttons button { margin:0; padding:4px 10px; }
    </style>
  </head>
  <body>
    <div class="top">
      <div><strong>中国人民银行各省行政处罚</strong></div>
      <div style="margin-left:3ch;">
        <label>选择省份：</label>
        <select id="provFetchSel">
          <option value="">全部</option>
          {% for p in site_provinces %}
            <option value="{{ p }}">{{ p }}</option>
          {% endfor %}
        </select>
        <button id="btnFetchOne">获取数据</button>
        <div class="progress"><div class="bar" id="bar"></div></div>
        <span id="progText"></span>
      </div>
    </div>
    <div class="toolbar">
      <div class="range-buttons">
        <button id="btnRangeMonth" {% if range_key == 'month' %}disabled{% endif %}>近一月</button>
        <button id="btnRangeYear" {% if range_key == 'year' %}disabled{% endif %}>近一年</button>
        <button id="btnRangeAll" {% if range_key == 'all' %}disabled{% endif %}>全部</button>
      </div>
      <label style="margin-left:3ch;">筛选省份：</label>
      <select id="provSel">
        <option value="">全部</option>
        {% for p in provinces %}
          <option value="{{ p }}" {% if p == province_filter %}selected{% endif %}>{{ p }}</option>
        {% endfor %}
      </select>
      <label style="margin-left:8px;">关键字：</label>
      <select id="kwSel">
        <option value="">全部</option>
        {% for k in keywords %}
          <option value="{{ k }}" {% if k == keyword_filter %}selected{% endif %}>{{ k }}</option>
        {% endfor %}
      </select>
    </div>
    {% if rows %}
      <table>
        <thead>
          <tr>
            <th>序号</th>
            <th>省份</th>
            <th>分行名称</th>
            <th>处罚文件名称</th>
            <th>发布日期</th>
            <th>附件</th>
          </tr>
        </thead>
        <tbody>
          {% for r in rows %}
          <tr>
            <td>{{ loop.index }}</td>
            <td>{{ r.province }}</td>
            <td>{{ r.branch }}</td>
            <td><a href="{{ r.url }}" target="_blank" rel="noopener">{{ r.name }}</a></td>
            <td>{{ r.date }}</td>
            <td class="downloads">
              {% if r.downloads and r.downloads|length > 0 %}
                {% for d in r.downloads %}
                  <a href="{{ d }}" target="_blank" rel="noopener">下载</a>
                {% endfor %}
              {% else %}-{% endif %}
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    {% else %}
      <p class="empty">暂无数据</p>
    {% endif %}
    <script>
      const bar = document.getElementById('bar');
      const txt = document.getElementById('progText');
      const sel = document.getElementById('provSel');
      const kwSel = document.getElementById('kwSel');
      const selFetch = document.getElementById('provFetchSel');
      const btnOne = document.getElementById('btnFetchOne');
      const btnRangeMonth = document.getElementById('btnRangeMonth');
      const btnRangeYear = document.getElementById('btnRangeYear');
      const btnRangeAll = document.getElementById('btnRangeAll');
      sel.onchange = () => {
        const p = sel.value;
        const q = new URLSearchParams(window.location.search);
        if (p) q.set('province', p); else q.delete('province');
        window.location.search = q.toString();
      };
      kwSel.onchange = () => {
        const k = kwSel.value;
        const q = new URLSearchParams(window.location.search);
        if (k) q.set('keyword', k); else q.delete('keyword');
        window.location.search = q.toString();
      };
      btnOne.onclick = async () => {
        const p = selFetch.value;
        btnOne.disabled = true;
        txt.textContent = '';
        bar.style.width = '0%';
        try {
          if (!p || p === '' || p === '全部') {
            await fetch('/api/fetch_start', {method:'POST'});
          } else {
            await fetch('/api/fetch_start_one?province=' + encodeURIComponent(p), {method:'POST'});
          }
          const t = setInterval(async () => {
            const r = await fetch('/api/fetch_status');
            const j = await r.json();
            const total = j.total || 0;
            const cur = j.current || 0;
            const pct = total ? Math.floor(cur * 100 / total) : 0;
            bar.style.width = pct + '%';
            txt.textContent = (j.status || '') + ' ' + cur + '/' + total + ' ' + (j.message || '');
            if (j.status === 'done' || j.status === 'error') {
              clearInterval(t);
              btnOne.disabled = false;
              const q = new URLSearchParams(window.location.search);
              if (p && p !== '' && p !== '全部') {
                q.set('province', p);
              } else {
                q.delete('province');
              }
              window.location.search = q.toString();
            }
          }, 800);
        } catch(e) {
          btnOne.disabled = false;
        }
      };
      btnRangeMonth.onclick = () => {
        const q = new URLSearchParams(window.location.search);
        q.set('range', 'month');
        window.location.search = q.toString();
      };
      btnRangeYear.onclick = () => {
        const q = new URLSearchParams(window.location.search);
        q.set('range', 'year');
        window.location.search = q.toString();
      };
      btnRangeAll.onclick = () => {
        const q = new URLSearchParams(window.location.search);
        q.set('range', 'all');
        window.location.search = q.toString();
      };
    </script>
  </body>
</html>
"""

@app.route("/")
def index():
    range_key = request.args.get("range", "all")
    province_filter = request.args.get("province", "")
    keyword_filter = request.args.get("keyword", "")
    records = CACHE.get("records") or []
    filtered = filter_by_range(list(records), range_key)
    if province_filter:
        filtered = [x for x in filtered if x.get("province") == province_filter]
    if keyword_filter:
        filtered = [x for x in filtered if keyword_filter in (x.get("title") or "")]
    rows = []
    for it in filtered:
        dls = [att["url"] for att in (it.get("attachments") or [])]
        rows.append(
            type(
                "Row",
                (),
                {"province": it.get("province", ""), "branch": it.get("branch", ""), "name": it.get("title", ""), "url": it.get("url", ""), "date": it.get("date", ""), "downloads": dls},
            )
        )
    return render_template_string(
        INDEX_TMPL,
        rows=rows,
        range_key=range_key,
        provinces=sorted({x.get("province") for x in records}),
        site_provinces=[s["province"] for s in PROVINCE_SITES],
        keywords=sorted({k for k in KEY_WORDS if any(k in (x.get("title") or "") for x in records)}),
        keyword_filter=keyword_filter,
        province_filter=province_filter,
    )
@app.route("/api/fetch_start", methods=["POST"])
def fetch_start():
    import threading
    if PROGRESS["status"] == "running":
        return {"status": "running"}
    t = threading.Thread(target=_async_fetch_all, daemon=True)
    t.start()
    return {"status": "started"}
@app.route("/api/fetch_status")
def fetch_status():
    return {
        "status": PROGRESS.get("status"),
        "current": PROGRESS.get("current"),
        "total": PROGRESS.get("total"),
        "message": PROGRESS.get("message"),
    }
@app.route("/api/fetch_start_one", methods=["POST"])
def fetch_start_one():
    import threading
    if PROGRESS["status"] == "running":
        return {"status": "running"}
    prov = request.args.get("province") or (request.json or {}).get("province") or request.form.get("province")
    if not prov:
        return {"status": "error", "message": "缺少省份"}
    t = threading.Thread(target=_async_fetch_one, args=(prov,), daemon=True)
    t.start()
    return {"status": "started"}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
