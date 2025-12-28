# coding: utf-8
import os
import time
import re
import pymysql
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# 1. 环境变量与数据库配置
# ==========================================
# 加载 .env 文件中的环境变量，包括数据库连接信息
# env_path = r'D:/code/pyvenv3140/.env'
env_path = r'/Users/zhouwei/Code/venv3.14/.env'
is_load: bool = load_dotenv(dotenv_path=env_path)
if is_load:
    print("加载环境变量成功")
else:
    print("加载环境变量失败")

# 从环境变量获取数据库连接参数
# db_host: str | None = os.getenv('DB_HOST')
# db_port: str | None = os.getenv('DB_PORT')
# db_user: str | None = os.getenv('DB_USER')
# db_password: str | None = os.getenv('DB_PASSWORD')
db_host = os.getenv('url_aliyun')
db_port = os.getenv('port_aliyun')
db_user = os.getenv('user_aliyun')
db_password = os.getenv('password_aliyun')

# 指定数据库 schema和charset
db_schema: str = 'fic'
db_charset: str = 'utf8mb4'

def log_time_taken(start_time, step_description):
    """
    辅助函数：记录并打印某个步骤的耗时
    :param start_time: 步骤开始的时间戳
    :param step_description: 步骤描述
    """
    end_time = time.time()
    print(f"{step_description} 用时 {end_time - start_time:.2f} 秒")

# ==========================================
# 2. 数据处理工具函数
# ==========================================
def convert_date_format(date_string):
    """
    将中文日期格式转换为标准数据库日期格式 (yyyy-mm-dd)
    输入示例: '2025年7月2日'
    输出示例: '2025-07-02'
    """
    if not date_string:
        return ''
    # 匹配中文日期格式 yyyy年mm月dd日
    match = re.match(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_string)
    if match:
        year, month, day = match.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    return date_string

# ==========================================
# 3. 爬虫核心逻辑
# ==========================================
def get_total_pages(url):
    """
    获取列表页的总页数
    通过解析页面底部的分页控件（通常是倒数第二个加粗的数字）来获取
    """
    response = requests.get(url, timeout=15)
    response.encoding = response.apparent_encoding or 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    span = soup.find('span', style="padding:0 15px;")
    if span:
        b_tags = span.find_all('b')
        if len(b_tags) >= 3:
            return int(b_tags[2].text)
    return 1

def get_additional_info(url):
    """
    抓取详情页面的详细信息
    :param url: 详情页链接
    :return: 包含详情字段的字典
    """
    response = requests.get(url, timeout=15)
    response.encoding = response.apparent_encoding or 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    tbody = soup.find('tbody')
    info: dict[str, str] = {}
    if tbody:
        rows = tbody.find_all('tr')
        # 定义需要提取的字段及其正则匹配模式
        patterns = [
            (r'索引号|许可证编号|许可证号', '许可证号'),
            (r'公开信息名称|公司名称', '公司名称'),
            (r'法定代表人（负责人）', '法定代表人（负责人）'),            
            (r'住所（营业场所）', '住所（营业场所）'),
            (r'业务类型', '业务类型'),
            (r'业务覆盖范围', '业务覆盖范围'),            
            (r'换证日期', '换证日期'),
            (r'首次许可日期', '首次许可日期'),
            (r'有效期至|有效期截止', '有效期至'),
            (r'备注', '备注')
        ]
        for row in rows:
            tds = row.find_all('td')
            if len(tds) >= 2:
                key = tds[0].get_text(strip=True)
                value = tds[1].get_text(strip=True)
                # 遍历模式匹配字段
                for pat, target_key in patterns:
                    if re.search(pat, key):
                        # 对日期字段进行格式转换
                        if target_key in {'首次发证日期', '发证日期', '有效期至', '换证日期', '首次许可日期'}:
                            value = convert_date_format(value)
                        if target_key not in info:
                            info[target_key] = value
                        break
    return info

def scrape_page(url):
    """
    抓取单个列表页的数据，并自动进入详情页抓取补充信息
    :param url: 列表页 URL
    :return: 包含该页所有记录的列表，每条记录是一个字典
    """
    response = requests.get(url, timeout=15)
    response.encoding = response.apparent_encoding or 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    ul_element = soup.find('ul', class_='txtlist')
    data = []
    if ul_element:
        # 跳过表头，获取所有列表项
        list_items = ul_element.find_all('li')[1:]
        for item in list_items:
            # 提取列表页基本信息
            xkzh = item.find('span', class_='xkzh').text.strip() if item.find('span', 'xkzh') else ''
            jgmc_span = item.find('span', class_='jgmc')
            date = item.find('span', class_='date').text.strip() if item.find('span', 'date') else ''
            
            # 如果有详情页链接，进入详情页抓取更多字段
            if jgmc_span:
                link = jgmc_span.find('a')
                title = link.get('title', '').strip() if link else ''
                href = link.get('href') if link else ''
                if href:
                    full_url = urljoin(url, href)
                    additional_info = get_additional_info(full_url)
                    # 每次抓取详情页后短暂延时，避免多线程并发对服务器造成过大压力
                    time.sleep(0.2)
                else:
                    additional_info = {}
            else:
                title = ''
                additional_info = {}
            
            # 整合列表页和详情页的数据
            row_dict = {
                '许可证号': additional_info.get('许可证号', xkzh), # 优先使用详情页信息
                '公司名称': additional_info.get('公司名称', title),
                '生成日期': convert_date_format(date), # 列表页的发布日期
                '法定代表人（负责人）': additional_info.get('法定代表人（负责人）', ''),
                '住所（营业场所）': additional_info.get('住所（营业场所）', ''),
                '业务类型': additional_info.get('业务类型', ''),
                '业务覆盖范围': additional_info.get('业务覆盖范围', ''),
                '换证日期': additional_info.get('换证日期', ''),
                '首次许可日期': additional_info.get('首次许可日期', ''),
                '发证日期': additional_info.get('发证日期', ''), # 仅注销机构有此字段
                '有效期至': additional_info.get('有效期至', ''),
                '备注': additional_info.get('备注', '')
            }
            data.append(row_dict)
            
    return data

def scrape_and_save(base_url):
    """
    主抓取逻辑：遍历所有分页，抓取并汇总数据
    :param base_url: 包含分页占位符 {} 的基础 URL
    :return: 所有抓取到的数据列表
    """
    all_data = []
    total_pages = get_total_pages(base_url.format(1))
    print(f"开始抓取，总页数: {total_pages}，使用3个线程并行抓取")
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_page = {executor.submit(scrape_page, base_url.format(i)): i for i in range(1, total_pages + 1)}
        
        completed_count = 0
        for future in as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                page_data = future.result()
                all_data.extend(page_data)
                completed_count += 1
                # 实时打印进度日志
                print(f"进度: 已完成 {completed_count}/{total_pages} 页 | 本页抓取: {len(page_data)} 条 | 累计抓取: {len(all_data)} 条")
            except Exception as e:
                print(f"第 {page_num} 页抓取失败: {e}")
                
    return all_data

def find_target_url(base_url, keyword):
    """
    根据关键字在目录页中查找目标链接
    :param base_url: 目录页 URL
    :param keyword: 链接文本或标题中包含的关键字
    :return: 目标页面的完整 URL，如果未找到则返回 None
    """
    print(f"正在搜索'{keyword}'...")
    try:
        response = requests.get(base_url, timeout=15)
        response.encoding = response.apparent_encoding or 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 遍历所有链接，查找文本或 title 属性中包含关键字的链接
        for a_tag in soup.find_all('a'):
            text = a_tag.get_text(strip=True)
            title = a_tag.get('title', '').strip()
            
            if keyword in text or keyword in title:
                href = a_tag.get('href')
                if href:
                    full_url = urljoin(base_url, href)
                    print(f"找到链接: {full_url}")
                    return full_url
        
        print(f"未找到包含关键字 '{keyword}' 的链接。")
        return None
    except Exception as e:
        print(f"查找目标链接时出错: {e}")
        return None

def scrape_important_news(directory_url, keyword):
    """
    抓取重大事项变更许可信息公示
    :param directory_url: 目录页 URL
    :param keyword: 搜索关键字
    :return: 解析后的表格数据列表
    """
    # 步骤 1: 动态定位目标页面
    target_url = find_target_url(directory_url, keyword)
    if not target_url:
        print("警告: 未能找到动态链接。将尝试使用目录页作为回退（可能会失败）。")
        target_url = directory_url

    # 步骤 2: 抓取目标页面内容
    try:
        response = requests.get(target_url, timeout=15)
        response.encoding = response.apparent_encoding or 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        table = soup.find('table')
        data = []
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cols = [ele.get_text(strip=True) for ele in row.find_all(['td', 'th'])]
                if any(cols):
                    data.append(cols)
        
        print(f"提取到 {len(data) - 1} 行重大事项变更数据 (不含表头)")
        return data
    except Exception as e:
        print(f"抓取重大事项变更数据失败: {e}")
        return []

def insert_important_news_to_mysql(connection, data):
    """
    将重大事项变更数据写入数据库
    """
    if not data or len(data) <= 1:
        return

    with connection.cursor() as cursor:
        inserted_rows = 0
        # 跳过表头
        for row in data[1:]:
            # 数据清洗：确保至少7列，不足补None
            row = (row + [None] * 7)[:7]
            try:
                cursor.execute("""
                    REPLACE INTO pbc_important_news 
                    (`序号`, `被许可人名称（姓名）`, `许可文件编号`, `许可文件名称`, `有效期限`, `许可内容`, `许可机关`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, row)
                inserted_rows += 1
            except Exception as e:
                print(f"插入重大事项失败: {row} - {e}")
    
    connection.commit()
    print(f"已写入 {inserted_rows} 行重大事项变更数据到 pbc_important_news")

# ==========================================
# 4. 数据库操作
# ==========================================
def insert_data_to_mysql(connection, table_name, data, columns):
    """
    将抓取的数据批量写入 MySQL 数据库
    """
    if not data:
        return

    # 预构建SQL语句
    escaped_columns = [f"`{col}`" for col in columns]
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f"REPLACE INTO {table_name} ({', '.join(escaped_columns)}) VALUES ({placeholders})"

    with connection.cursor() as cursor:
        inserted_rows = 0
        for row_dict in data:
            row_values = [row_dict.get(col, '') for col in columns]
            try:
                cursor.execute(sql, row_values)
                inserted_rows += 1
            except Exception as e:
                print(f"插入行失败: {row_values} - {e}")

    connection.commit()
    print(f"已写入 {inserted_rows} 行到 {table_name}")

# ==========================================
# 5. 主程序入口
# ==========================================
if __name__ == "__main__":
    try:
        # 建立数据库连接
        connection = pymysql.connect(
            host=db_host,
            port=int(db_port),
            user=db_user,
            password=db_password,
            database=db_schema,
            charset=db_charset,
            cursorclass=pymysql.cursors.DictCursor
        )

        # 定义数据表对应的字段映射
        
        # 1. 已获许可机构（pbc_inst_registered）字段定义
        cols_registered = [
            "许可证号", "公司名称", "生成日期", "法定代表人（负责人）", "住所（营业场所）", 
            "业务类型", "业务覆盖范围", "换证日期", "首次许可日期", "有效期至", "备注"
        ]
        
        # 2. 已注销许可机构（pbc_inst_unregistered）字段定义
        cols_unregistered = [
            "许可证号", "公司名称", "生成日期", "法定代表人（负责人）", "住所（营业场所）", 
            "业务类型", "业务覆盖范围", "换证日期", "首次许可日期", "发证日期", "有效期至", "备注"
        ]

        # ---------------------------------------------------------
        # 任务一：抓取“已获许可机构”数据
        # ---------------------------------------------------------
        start_time = time.time()
        base_url1 = "https://www.pbc.gov.cn/zhengwugongkai/4081330/4081344/4081407/4081702/4081749/4081783/9398ddc0-{}.html"
        registered_data = scrape_and_save(base_url1)
        log_time_taken(start_time, "抓取“已获许可机构（支付机构）”数据")
        
        start_time = time.time()
        insert_data_to_mysql(connection, "pbc_inst_registered", registered_data, cols_registered)
        log_time_taken(start_time, "写入”已获许可机构（支付机构）“数据到数据库")

        # ---------------------------------------------------------
        # 任务二：抓取“已注销许可机构”数据
        # ---------------------------------------------------------
        start_time = time.time()
        base_url2 = "https://www.pbc.gov.cn/zhengwugongkai/4081330/4081344/4081407/4081702/4081749/4081786/63ead9a6-{}.html"
        unregistered_data = scrape_and_save(base_url2)
        log_time_taken(start_time, "抓取“已注销许可机构”数据")
        
        start_time = time.time()
        insert_data_to_mysql(connection, "pbc_inst_unregistered", unregistered_data, cols_unregistered)
        log_time_taken(start_time, "写入“已注销许可机构”数据到数据库")
        
        # ---------------------------------------------------------
        # 任务三：抓取“非银行支付机构重大事项变更许可信息公示”数据
        # ---------------------------------------------------------
        start_time = time.time()
        directory_url = "https://www.pbc.gov.cn/zhengwugongkai/4081330/4081344/4081407/4081702/4081749/4693227/index.html"
        important_news_data = scrape_important_news(directory_url, "非银行支付机构重大事项变更许可信息公示")
        log_time_taken(start_time, "抓取“重大事项变更”数据")

        start_time = time.time()
        insert_important_news_to_mysql(connection, important_news_data)
        log_time_taken(start_time, "写入“重大事项变更”数据到数据库")

    except Exception as e:
        print(f"程序运行出错: {e}")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()
