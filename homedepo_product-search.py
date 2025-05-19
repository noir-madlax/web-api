import os
import csv
import json
from serpapi import GoogleSearch
import dotenv
import time

# 加载环境变量
dotenv.load_dotenv()

# 从.env文件获取API密钥
api_key = os.environ.get("homedepot.apikey")
if not api_key:
    # 如果不能通过os.environ.get获取，直接读取.env文件
    with open(".env", "r") as env_file:
        for line in env_file:
            if line.startswith("homedepot.apikey="):
                api_key = line.strip().split("=", 1)[1]
                break

# 从关键词文件读取搜索词
def read_keywords(file_path):
    with open(file_path, 'r') as file:
        keywords = [line.strip() for line in file if line.strip()]
    return keywords

# 使用SerpAPI搜索Home Depot
def search_home_depot(keyword, api_key, page=1):
    # Home Depot API使用nao参数进行分页，每页显示24个结果
    # 第一页为0，第二页为24，第三页为48，以此类推
    nao = 0 if page == 1 else (page-1) * 24
    
    params = {
        "engine": "home_depot",
        "q": keyword,
        "api_key": api_key,
        "nao": nao,
        "page_size": 24  # 每页显示24个结果
    }
    
    search = GoogleSearch(params)
    results = search.get_dict()
    return results

# 将结果保存为CSV
def save_to_csv(results, keyword, output_file, append=True):
    # 检查是否有产品数据
    if "products" not in results or not results["products"]:
        print(f"未找到关于'{keyword}'的产品数据")
        return 0
    
    products = results["products"]
    
    # 定义CSV文件的字段名
    fieldnames = [
        "keyword", "title", "link", "price", "unit", "rating", "reviews", 
        "model_number", "brand", "delivery_free", "store_name", "in_stock_quantity"
    ]
    
    # 检查文件是否存在，决定是否写入标题行
    file_exists = os.path.isfile(output_file)
    
    mode = 'a' if append else 'w'
    with open(output_file, mode=mode, newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        # 如果文件不存在或不是追加模式，写入标题行
        if not file_exists or not append:
            writer.writeheader()
        
        # 写入产品数据
        for product in products:
            # 处理delivery和pickup字段
            delivery_free = False
            if isinstance(product.get("delivery"), dict):
                delivery_free = product["delivery"].get("free", False)
            
            store_name = ""
            in_stock_quantity = 0
            if isinstance(product.get("pickup"), dict):
                store_name = product["pickup"].get("store_name", "")
                in_stock_quantity = product["pickup"].get("quantity", 0)
            
            # 创建一个格式化后的产品数据字典
            product_data = {
                "keyword": keyword,
                "title": product.get("title", ""),
                "link": product.get("link", ""),
                "price": product.get("price", ""),
                "unit": product.get("unit", ""),
                "rating": product.get("rating", ""),
                "reviews": product.get("reviews", ""),
                "model_number": product.get("model_number", ""),
                "brand": product.get("brand", ""),
                "delivery_free": delivery_free,
                "store_name": store_name,
                "in_stock_quantity": in_stock_quantity
            }
            
            writer.writerow(product_data)
    
    return len(products)

def main():
    # 定义文件路径
    keywords_file = "homedepo_search_keywords.txt"
    output_file = "homedepo_search_results.csv"
    
    # 读取关键词
    keywords = read_keywords(keywords_file)
    
    # 设置不追加模式，以覆盖旧文件
    first_write = True
    
    # 对每个关键词进行搜索并保存结果
    for keyword in keywords:
        print(f"正在搜索: {keyword}")
        
        total_products = 0
        max_pages = 3  # 最多获取3页数据
        
        for page in range(1, max_pages + 1):
            print(f"  获取第{page}页...")
            results = search_home_depot(keyword, api_key, page)
            
            # 保存结果，第一次写入时覆盖文件，之后追加
            products_count = save_to_csv(results, keyword, output_file, not first_write)
            first_write = False
            
            total_products += products_count
            
            # 检查是否有更多页面
            if "serpapi_pagination" not in results or "next" not in results["serpapi_pagination"] or products_count == 0:
                break
                
            # 为了避免API请求过快，添加短暂延迟
            time.sleep(1)
        
        print(f"已将{total_products}个'{keyword}'的搜索结果保存到{output_file}")

if __name__ == "__main__":
    main()
