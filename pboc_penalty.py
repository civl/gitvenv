import datetime
import re
from urllib.parse import urljoin, urlparse
from flask import Flask, request, render_template_string
import requests
from bs4 import BeautifulSoup

app = Flask(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}
KEY_WORDS = ["支付宝", "财付通", "拉卡拉", "快钱", "新生", "钱宝"]

PROVINCE_SITES = [
    {"province": "海南省", "base_url": "https://haikou.pbc.gov.cn/haikou/132982/133000/133007/index.html"},
    {"province": "山东省", "base_url": "https://jinan.pbc.gov.cn/jinan/120967/120985/120994/index.html"},
]

FILE_EXTS = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".et", ".zip", ".rar")
CACHE = {"prov": None, "city": None, "records": None}
PROGRESS = {"status": "idle", "current": 0, "total": 0, "message": ""}

def fetch(url):
    r = requests.get(url, headers=HEADERS, timeout=15)
    if r.status_code != 200:
        return None
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

def list_pages(base_url, max_pages=10):
    pages = [base_url]
    first_html = fetch(base_url)
    if not first_html:
        return pages
    soup = BeautifulSoup(first_html, "lxml")
    inp = soup.find("input", attrs={"name": "article_paging_list_hidden"})
    total = 1
    if inp and inp.has_attr("totalpage"):
        try:
            total = int(inp["totalpage"])
        except Exception:
            total = 1
    candidates = soup.select("div.list_page a.pagingNormal[href]")
    pattern = None
    if candidates:
        last = candidates[-1]
        href = normalize_href(base_url, last.get("href"))
        if href:
            m = re.search(r"(.+?)-(\d+)\.html$", href)
            if m:
                pattern = re.sub(r"-(\d+)\.html$", "-%d.html", href)
    if pattern and total > 1:
        limit = min(total, max_pages)
        for i in range(2, limit + 1):
            pages.append(pattern % i)
    return pages

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

def collect_detail_links(base_url):
    items = []
    seen = set()
    parent_dir = base_url.rsplit("/", 1)[0]
    for page in list_pages(base_url, max_pages=10):
        html = fetch(page)
        if not html:
            continue
        soup = BeautifulSoup(html, "lxml")
        ban_titles = {"法律声明", "联系我们", "设为首页", "加入收藏"}
        container_lists = soup.select('div.txtbox_2.portlet[opentype="page"] ul.txtlist')
        if not container_lists:
            container_lists = soup.find_all("ul", class_="txtlist")
        for ul in container_lists:
            for li in ul.find_all("li"):
                a = li.find("a", href=True)
                if not a:
                    continue
                title = a.get("title") or a.get_text(strip=True)
                if title in ban_titles:
                    continue
                href = normalize_href(page, a.get("href"))
                if not href:
                    continue
                p = urlparse(href).path
                if "/haikou/132982/133000/133007/" not in p or not p.endswith("/index.html"):
                    continue
                ds = li.find("span", class_="date")
                date_text = (ds.get_text(strip=True) if ds else "") if ds else ""
                if href not in seen:
                    seen.add(href)
                    items.append({"title": title, "url": href, "date": date_text})
    return items

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
def infer_branch(title, source):
    m = re.search(r"(?:中国人民银行)?(.+?分行)", title or "")
    if m:
        return m.group(1)
    if source == "prov":
        return "海南省分行"
    return "辖内市分行"
def _title_to_branch(portlet_title):
    s = (portlet_title or "").strip()
    if "省分行" in s:
        return "省分行"
    if "市分行" in s or "辖内市分行" in s:
        return "辖内市分行"
    return "海南省分行"
def collect_from_base(base_url):
    html = fetch(base_url)
    if not html:
        return [], [], "海南省分行", "辖内市分行"
    soup = BeautifulSoup(html, "lxml")
    prov_items, city_items = [], []
    prov_branch, city_branch = "海南省分行", "辖内市分行"
    containers = soup.select('div.txtbox_2.portlet[opentype="page"]')
    for cont in containers:
        title_span = cont.find("span", class_="portlettitle2")
        title_text = title_span.get_text(strip=True) if title_span else ""
        branch = _title_to_branch(title_text)
        more = None
        for a in cont.find_all("a", href=True):
            if a.get_text(strip=True) == "更多" or re.search(r"更多", a.get_text(strip=True)):
                more = normalize_href(base_url, a.get("href"))
                break
        if more:
            items = collect_detail_links(more)
        else:
            items = []
            uls = cont.select("ul.txtlist")
            for ul in uls:
                for li in ul.find_all("li"):
                    a = li.find("a", href=True)
                    if not a:
                        continue
                    href = normalize_href(base_url, a.get("href"))
                    title = a.get("title") or a.get_text(strip=True)
                    ds = li.find("span", class_="date")
                    date_text = (ds.get_text(strip=True) if ds else "") if ds else ""
                    if href and title:
                        items.append({"title": title, "url": href, "date": date_text})
        if branch == "省分行":
            prov_branch = branch
            prov_items.extend(items)
        else:
            city_branch = branch
            city_items.extend(items)
    return prov_items, city_items, prov_branch, city_branch
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
                containers = soup.select('div.txtbox_2.portlet[opentype="page"]')
                for cont in containers:
                    title_span = cont.find("span", class_="portlettitle2")
                    title_text = title_span.get_text(strip=True) if title_span else ""
                    branch = "省分行" if ("省分行" in title_text) else ("辖内市分行" if ("市分行" in title_text or "辖内市分行" in title_text) else "省分行")
                    uls = cont.select("ul.txtlist")
                    for ul in uls:
                        for li in ul.find_all("li"):
                            a = li.find("a", href=True)
                            if not a:
                                continue
                            name = a.get("title") or a.get_text(strip=True)
                            href = normalize_href(page, a.get("href"))
                            ds = li.find("span", class_="date")
                            date_text = (ds.get_text(strip=True) if ds else "") if ds else ""
                            item = {"province": province, "branch": branch, "title": name, "url": href, "date": date_text}
                            records.append(item)
        for it in records:
            it["attachments"] = collect_attachments(it["url"])
        CACHE["records"] = records
    return CACHE["records"]
def _async_fetch_all():
    try:
        PROGRESS["status"] = "running"
        pages_map = []
        total = 0
        for site in PROVINCE_SITES:
            pages = list_pages(site["base_url"], max_pages=50)
            pages_map.append({"province": site["province"], "pages": pages})
            total += len(pages)
        PROGRESS["total"] = total
        PROGRESS["current"] = 0
        records = []
        for entry in pages_map:
            prov = entry["province"]
            for page in entry["pages"]:
                html = fetch(page)
                if html:
                    soup = BeautifulSoup(html, "lxml")
                    containers = soup.select('div.txtbox_2.portlet[opentype="page"]')
                    for cont in containers:
                        title_span = cont.find("span", class_="portlettitle2")
                        title_text = title_span.get_text(strip=True) if title_span else ""
                        branch = "省分行" if ("省分行" in title_text) else ("辖内市分行" if ("市分行" in title_text or "辖内市分行" in title_text) else "省分行")
                        uls = cont.select("ul.txtlist")
                        for ul in uls:
                            for li in ul.find_all("li"):
                                a = li.find("a", href=True)
                                if not a:
                                    continue
                                name = a.get("title") or a.get_text(strip=True)
                                href = normalize_href(page, a.get("href"))
                                ds = li.find("span", class_="date")
                                date_text = (ds.get_text(strip=True) if ds else "") if ds else ""
                                records.append({"province": prov, "branch": branch, "title": name, "url": href, "date": date_text})
                PROGRESS["current"] += 1
        PROGRESS["message"] = "解析完成，开始获取附件"
        PROGRESS["total"] = PROGRESS["current"] + len(records)
        for it in records:
            it["attachments"] = collect_attachments(it["url"])
            PROGRESS["current"] += 1
        CACHE["records"] = records
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
      .top { display:flex; justify-content: space-between; align-items:center; margin-bottom:12px; }
      .note { color:#666; font-size:12px; }
      .filters { margin: 8px 0 16px; }
      .filters a { margin-right: 8px; text-decoration:none; }
      .filters .active { font-weight: 600; }
      table { width: 100%; border-collapse: collapse; }
      th, td { border: 1px solid #ddd; padding: 8px; }
      th { background: #f6f6f6; text-align: left; }
      .empty { color:#999; }
      .downloads a { margin-right: 6px; }
      .toolbar { display:flex; gap:12px; align-items:center; margin-bottom:12px; }
      .progress { width:240px; height:10px; background:#eee; border-radius:5px; overflow:hidden; display:inline-block; vertical-align:middle; }
      .bar { height:100%; width:0%; background:#0a4792; transition: width .2s ease; }
      select { padding:4px; }
    </style>
  </head>
  <body>
    <div class="top">
      <div><strong>中国人民银行各省行政处罚</strong></div>
    </div>
    <div class="toolbar">
      <button id="btnFetch">手工拉取数据</button>
      <div class="progress"><div class="bar" id="bar"></div></div>
      <span id="progText"></span>
      <label>筛选省份：</label>
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
    <div class="filters">
      <a href="?range=month" class="{{ 'active' if range_key == 'month' else '' }}">近一月</a>
      <a href="?range=year" class="{{ 'active' if range_key == 'year' else '' }}">近一年</a>
      <a href="?range=all" class="{{ 'active' if range_key == 'all' else '' }}">全部</a>
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
            <th>下载</th>
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
      const btn = document.getElementById('btnFetch');
      const bar = document.getElementById('bar');
      const txt = document.getElementById('progText');
      const sel = document.getElementById('provSel');
      const kwSel = document.getElementById('kwSel');
      btn.onclick = async () => {
        btn.disabled = true;
        txt.textContent = '';
        bar.style.width = '0%';
        try {
          await fetch('/api/fetch_start', {method:'POST'});
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
              btn.disabled = false;
              location.href = location.pathname + '?range={{ range_key }}' + '{{ "&province=" + province_filter if province_filter else "" }}' + '{{ "&keyword=" + keyword_filter if keyword_filter else "" }}';
            }
          }, 800);
        } catch(e) {
          btn.disabled = false;
        }
      };
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
