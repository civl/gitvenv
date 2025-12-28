# coding=utf-8
import os
import requests
from bs4 import BeautifulSoup
import mysql.connector
import time
import re
from dotenv import load_dotenv
from datetime import datetime

# 定义爬取的起始页、爬取页面增量，如（1，4），代表从第1页到第4页
start_page = 1
page_offset = 3
# env_path = r'D:/code/pyvenv3140/.env'
env_path = r'/Users/zhouwei/Code/venv3.14/.env'
# 加载环境变量
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

def scrape_mpaypass(start_page, num_pages):
    start_time = time.time()
    base_url = "https://www.mpaypass.com.cn/news/?id=2&page={}"
    
    # 设置请求头，模拟浏览器
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # 连接到MySQL数据库
    conn = mysql.connector.connect(
        host=db_host,
        port=int(db_port) if db_port else 3306,
        user=db_user,
        password=db_password,
        database=db_schema,
        charset=db_charset
        # cursorclass=mysql.connector.cursor.DictCursor # 插入操作不需要DictCursor，默认即可
    )
    cursor = conn.cursor()

    # 检查表是否存在，如果不存在则创建
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS news_mpaypass (
        website VARCHAR(50),
        subject VARCHAR(255),
        keyword VARCHAR(255),
        release_time DATETIME,
        hyperlink VARCHAR(255),
        UNIQUE KEY unique_news (website, subject, release_time)
    )
    """)

    total_inserted_rows = 0  # 统计插入的行数

    try:
        for page in range(start_page, start_page + num_pages):
            page_start_time = time.time()
            url = base_url.format(page)
            print(f"正在爬取第 {page} 页: {url}")
            
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.encoding = 'utf-8' # 显式设置编码，防止乱码
                
                if response.status_code != 200:
                    print(f"请求失败，状态码: {response.status_code}")
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                news_items = soup.find_all(class_="newslist")
                
                if not news_items:
                    print("未找到新闻列表，可能页面结构已变更或加载失败")
                
                for item in news_items:
                    website = '移动支付网'
                    subject = ''
                    keyword = ''
                    release_time = ''
                    href = ''
                    
                    try:
                        # 在BeautifulSoup中查找元素
                        # 注意：ID在HTML标准中应唯一，但如果页面有重复ID，find/find_all仍可工作
                        # 这里原代码使用了ID查找，我们沿用find(id="...")
                        
                        listbody = item.find(id="listbody")
                        if not listbody:
                            continue

                        title_elem = listbody.find(id="title")
                        keyword_elem = listbody.find(id="keywords")
                        time_elem = listbody.find(id="time")
                        
                        if title_elem:
                            a_tag = title_elem.find("a")
                            if a_tag:
                                href = a_tag.get('href')
                                subject = a_tag.get_text().strip()
                        
                        if keyword_elem:
                            keyword = keyword_elem.get_text().strip().replace("标签：", "")
                        
                        if time_elem:
                            release_time = time_elem.get_text().strip()
                        
                        # 数据清洗
                        if subject and release_time:
                            # 清理release_time中的非法字符和多余空格
                            release_time = re.sub(r'[^\d/: ]', '', release_time).strip()
                            
                            # 清理release_time，去掉多余的/，转换为MySQL数据的格式
                            release_time = re.sub(r'(\d{4}/\d{1,2}/\d{1,2})/?\s*(\d{1,2}:\d{1,2})', r'\1 \2', release_time)
                            release_time = datetime.strptime(release_time, '%Y/%m/%d %H:%M').strftime('%Y-%m-%d %H:%M:%S')
                            
                            # 检查数据库中是否存在相同的subject
                            check_sql = "SELECT 1 FROM news_mpaypass WHERE subject = %s LIMIT 1"
                            cursor.execute(check_sql, (subject,))
                            if cursor.fetchone():
                                print(f"数据已存在，跳过: {subject}")
                                continue
                            
                            # 使用INSERT INTO插入数据
                            sql = "INSERT INTO news_mpaypass (website, subject, keyword, release_time, hyperlink) VALUES (%s, %s, %s, %s, %s)"
                            val = (website, subject, keyword, release_time, href)
                            cursor.execute(sql, val)
                            total_inserted_rows += cursor.rowcount  # 累加插入行数
                            conn.commit()
                        else:
                            print(f"跳过不完整数据: subject={subject}, release_time={release_time}")

                    except Exception as e:
                        print(f"处理单条数据时发生错误：{str(e)}")
                        print(f"错误数据：website={website}, subject={subject}, keyword={keyword}, release_time={release_time}, href={href}")
            
            except requests.RequestException as e:
                print(f"请求页面时发生网络错误: {e}")
            
            page_end_time = time.time()
            print(f"第 {page} 页爬取完成，耗时 {page_end_time - page_start_time:.2f} 秒")
            time.sleep(2) # 礼貌性延时
        
        print(f"本次插入了 {total_inserted_rows} 条数据")
    
    except Exception as e:
        print(f"爬取过程中发生错误：{str(e)}")
    
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
    
    end_time = time.time()
    print(f"总耗时：{end_time - start_time:.2f} 秒")

if __name__ == "__main__":
    scrape_mpaypass(start_page, page_offset)
