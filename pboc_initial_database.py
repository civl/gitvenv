import os
import pymysql
from dotenv import load_dotenv

# 模块加载时自动加载环境变量
env_path = r'D:\code\.venv\.env'
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    print(f"警告: 未找到配置文件 {env_path}")

def get_connection(schema_name: str) -> pymysql.connections.Connection:
    """
    获取指定 Schema (数据库) 的数据库连接。
    
    Args:
        schema_name (str): 目标数据库名称 (Schema Name)。用户需提前知晓该名称。
        
    Returns:
        pymysql.connections.Connection: 数据库连接对象。
        
    Raises:
        ValueError: 如果环境变量配置不完整。
        pymysql.Error: 如果连接数据库失败。
    """
    
    # 获取通用数据库连接配置
    host = os.getenv('url_aliyun')
    port = os.getenv('port_aliyun')
    user = os.getenv('user_aliyun')
    password = os.getenv('password_aliyun')
    
    # 简单的配置检查
    if not all([host, port, user, password]):
        raise ValueError("数据库配置缺失，请检查 .env 文件中的 url_aliyun, port_aliyun, user_aliyun, password_aliyun 配置。")

    print(f"正在尝试连接数据库: {schema_name} @ {host}...")

    try:
        connection = pymysql.connect(
            host=host,
            port=int(port) if port else 3306,
            user=user,
            password=password,
            database=schema_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        print(f"成功连接到数据库: {schema_name}")
        return connection

    except pymysql.Error as e:
        print(f"连接数据库 {schema_name} 失败: {e}")
        raise e

# 仅在直接运行此脚本时执行测试
if __name__ == "__main__":
    # 从环境变量获取默认 schema 用于测试，模拟用户已知 schema 名称的情况
    default_schema = os.getenv('schema_1')
    
    if default_schema:
        print(f"正在进行连接测试，使用默认 Schema: {default_schema}")
        try:
            conn = get_connection(default_schema)
            
            with conn.cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                result = cursor.fetchone()
                print(f"测试查询成功 - MySQL 版本: {result['VERSION()']}")
            
            conn.close()
            print("测试连接已关闭")
        except Exception as e:
            print(f"测试过程中发生错误: {e}")
    else:
        print("未在环境变量中找到 schema_1，无法进行默认测试。请被调用时指定 schema_name。")
