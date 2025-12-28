# coding=utf-8
import os
import requests
import pandas as pd
import time
import re
import pymysql
import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# 定义爬取的起始页、爬取页面增量，如（1，5），代表从第1页到第4页
start_page = 1
page_offset = 5

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

def scrape_sina_finance(start_page, num_pages):
    # Target URL for "新浪财经-金融一线"
    base_url = "https://finance.sina.com.cn/roll/c/249630.shtml"
    
    # Connect to MySQL database
    connection = pymysql.connect(
        host=db_host,
        port=int(db_port),
        user=db_user,
        password=db_password,
        database=db_schema,
        charset=db_charset,
        cursorclass=pymysql.cursors.DictCursor
    )

    inserted_count = 0
    start_time = time.time()

    try:
        with connection.cursor() as cursor:
            # Create table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS news_sina (
                website VARCHAR(50),
                subject VARCHAR(255),
                keyword VARCHAR(255),
                release_time DATETIME,
                hyperlink VARCHAR(255),
                UNIQUE KEY unique_news (website, subject, release_time)
            )
            """
            cursor.execute(create_table_query)

            # Fetch the main page
            print(f"正在抓取页面: {base_url}")
            try:
                response = requests.get(base_url, timeout=10)
                # Handle encoding
                if 'charset=gb2312' in response.text.lower() or 'charset=gbk' in response.text.lower():
                    response.encoding = 'gbk'
                else:
                    response.encoding = 'utf-8'
                
                soup = BeautifulSoup(response.text, 'html.parser')
                list_content = soup.find('ul', id='listcontent')
                
                if list_content:
                    items = list_content.find_all('li')
                    print(f"找到 {len(items)} 条新闻")
                    
                    # Optional: Slice items based on page arguments if strict pagination is needed.
                    # But typically scraping all available is better. 
                    # If we really want to simulate pagination:
                    # items_per_page = 40
                    # start_idx = (start_page - 1) * items_per_page
                    # end_idx = start_idx + (num_pages * items_per_page)
                    # items = items[start_idx:end_idx]
                    
                    for item in items:
                        try:
                            a_tag = item.find('a')
                            span_tag = item.find('span')
                            
                            if not a_tag:
                                continue
                                
                            subject = a_tag.get_text().strip()
                            hyperlink = a_tag['href'].strip()
                            
                            release_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                            if span_tag:
                                # Format is usually (MM月DD日 HH:mm)
                                date_text = span_tag.get_text().strip().strip('()')
                                try:
                                    # Parse date. Assuming current year first.
                                    # Example: 12月28日 17:23
                                    match = re.search(r'(\d+)月(\d+)日\s*(\d+):(\d+)', date_text)
                                    if match:
                                        month, day, hour, minute = map(int, match.groups())
                                        now = datetime.datetime.now()
                                        year = now.year
                                        
                                        # Construct date
                                        release_time = datetime.datetime(year, month, day, hour, minute)
                                        
                                        # If date is in future (e.g. scraped 'Dec 31' on 'Jan 1'), subtract year
                                        if release_time > now + datetime.timedelta(days=1):
                                            release_time = release_time.replace(year=year - 1)
                                            
                                        release_time_str = release_time.strftime("%Y-%m-%d %H:%M")
                                except Exception as e:
                                    print(f"时间解析错误: {date_text}, error: {e}")
                            
                            website = '新浪财经-金融一线'

                            # Check if subject exists in database
                            check_query = "SELECT 1 FROM news_sina WHERE subject = %s LIMIT 1"
                            cursor.execute(check_query, (subject,))
                            if cursor.fetchone():
                                # print(f"Subject already exists, skipping: {subject}")
                                continue

                            # Insert data
                            insert_query = """
                            INSERT INTO news_sina (website, subject, release_time, hyperlink)
                            VALUES (%s, %s, %s, %s)
                            """
                            cursor.execute(insert_query, (website, subject, release_time_str, hyperlink))
                            connection.commit()
                            inserted_count += 1
                            
                        except Exception as e:
                            print(f"处理单条数据时发生错误: {str(e)}")
                            
                else:
                    print("未找到新闻列表内容 (#listcontent)")
                    
            except Exception as e:
                print(f"请求页面失败: {e}")

    except Exception as e:
        print(f"抓取过程中发生错误: {str(e)}")
    
    finally:
        connection.close()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"本次共抓取 {inserted_count} 条新闻数据")
    print(f"总耗时: {elapsed_time:.2f} 秒")


if __name__ == "__main__":
    scrape_sina_finance(start_page, page_offset)
