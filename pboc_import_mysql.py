import sys
import os

# 确保能导入同目录下的模块
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from pboc_penalty_data import run_spider

def main():
    # 这里定义需要抓取的省份列表
    # 可以根据实际需求修改这个列表
    target_provinces = ["重庆市"]
    
    print(f"准备抓取以下省份的数据: {', '.join(target_provinces)}")
    
    # 调用抓取函数
    # 尝试最大爬取5页
    run_spider(target_provinces, max_pages=5)

if __name__ == "__main__":
    main()
