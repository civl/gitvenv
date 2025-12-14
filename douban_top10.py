import requests
from bs4 import BeautifulSoup
import time

def get_top_movies():
    # 豆瓣电影 Top 250 的URL
    url = "https://movie.douban.com/top250"
    
    # 设置请求头，模拟浏览器访问
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        # 发送请求获取页面内容
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # 检查请求是否成功
        
        # 使用BeautifulSoup解析页面
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 找到所有电影条目
        movies = soup.find_all('div', class_='item')[:20]
        
        # 提取每部电影的名称
        for i, movie in enumerate(movies, 1):
            title = movie.find('span', class_='title').text
            print(f"{i}. {title}")
            
    except Exception as e:
        print(f"发生错误：{e}")
        
    time.sleep(1)  # 添加延时，避免频繁请求

if __name__ == "__main__":
    get_top_movies()

