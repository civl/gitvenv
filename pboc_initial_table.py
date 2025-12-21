import pboc_initial_database as db

def ensure_table_exists():
    schema_name = 'fic'
    table_name = 'pboc_penalty'
    
    # 获取数据库连接
    try:
        conn = db.get_connection(schema_name)
    except Exception as e:
        print(f"无法连接到数据库 {schema_name}: {e}")
        return

    try:
        with conn.cursor() as cursor:
            # 检查表是否存在
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            result = cursor.fetchone()
            
            if result:
                print(f"表 `{table_name}` 在数据库 `{schema_name}` 中已存在。")
            else:
                print(f"表 `{table_name}` 不存在，正在创建...")
                
                # 用户提供的建表 SQL
                create_sql = """
                CREATE TABLE `pboc_penalty` ( 
                   `id` int NOT NULL AUTO_INCREMENT, 
                   `省份` varchar(30) DEFAULT NULL, 
                   `分行` varchar(50) DEFAULT NULL, 
                   `行政处罚文件` varchar(200) DEFAULT NULL, 
                   `发布日期` date DEFAULT NULL, 
                   `下载链接` varchar(300) DEFAULT NULL, 
                   `数据更新时间` datetime DEFAULT NULL, 
                   `数据类型` varchar(30) DEFAULT NULL, 
                   PRIMARY KEY (`id`) 
                 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
                
                cursor.execute(create_sql)
                conn.commit()
                print(f"表 `{table_name}` 创建成功！")

    except Exception as e:
        print(f"操作失败: {e}")
    finally:
        conn.close()
        print("数据库连接已关闭")

if __name__ == "__main__":
    ensure_table_exists()
