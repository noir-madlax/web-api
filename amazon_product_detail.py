#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Amazon产品数据抓取工具

读取ASIN列表，通过Unwrangle API获取Amazon产品详情，并输出为CSV格式
"""

import os
import time
import csv
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# 配置参数
LIMIT = 3  # 测试模式处理ASIN数量，设置为0表示处理全部
# 生成带时间戳的输出文件名
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_FILE = f'amazon_product_details_{timestamp}.csv'  # 输出CSV文件名
API_URL = 'https://data.unwrangle.com/api/getter/'
INPUT_FILE = 'cable_asin_list.txt'  # 输入ASIN列表文件
MAX_RETRIES = 1  # API请求最大重试次数
REQUEST_DELAY = 1  # 请求间隔(秒)

# 定义CSV输出字段
CSV_FIELDS = [
    'asin', 'name', 'brand', 'url', 'price', 'price_reduced', 'rating', 
    'review_count', 'availability', 'category', 'bullet_points', 'description',
    'product_dimensions', 'product_specifications', 'product_weight', 
    'main_image_url', 'whats_in_box', 'variant_data'
]

def main():
    """主函数"""
    # 加载环境变量中的API密钥
    load_dotenv()
    api_key = os.getenv('unwrangle.apikey')
    
    if not api_key:
        print("错误: 未找到API密钥，请在.env文件中设置 unwrangle.apikey")
        return
    
    # 读取ASIN列表
    try:
        with open(INPUT_FILE, 'r') as f:
            asin_list = f.read().strip().split()
            # 去除空白项
            asin_list = [asin.strip() for asin in asin_list if asin.strip()]
            print(f"读取到{len(asin_list)}个ASIN")
    except Exception as e:
        print(f"错误: 无法读取ASIN列表文件 {INPUT_FILE}: {str(e)}")
        return
    
    # 限制处理数量（测试模式）
    if LIMIT > 0:
        asin_list = asin_list[:LIMIT]
        print(f"测试模式: 仅处理前{LIMIT}个ASIN")
    
    # 创建CSV文件
    try:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDS)
            writer.writeheader()
            
            # 处理每个ASIN
            success_count = 0
            failed_asins = []
            remaining_credits = None
            
            for i, asin in enumerate(asin_list):
                print(f"处理 {i+1}/{len(asin_list)}: ASIN {asin}")
                
                product_data = fetch_product_data(asin, api_key)
                
                if product_data:
                    # 记录剩余配额
                    if 'remaining_credits' in product_data:
                        remaining_credits = product_data['remaining_credits']
                        del product_data['remaining_credits']
                    
                    # 写入CSV
                    writer.writerow(product_data)
                    success_count += 1
                    print(f"✓ 成功获取 ASIN {asin} 的数据")
                else:
                    failed_asins.append(asin)
                    print(f"✗ 无法获取 ASIN {asin} 的数据")
                
                # 定期刷新文件
                if (i + 1) % 10 == 0:
                    csvfile.flush()
                    print(f"已完成 {i+1}/{len(asin_list)} 个ASIN的处理")
                
                # 延迟，避免API限流
                time.sleep(REQUEST_DELAY)
            
            # 输出统计信息
            print("\n处理完成!")
            print(f"输出文件: {OUTPUT_FILE}")
            print(f"总计处理: {len(asin_list)} 个ASIN")
            print(f"成功: {success_count}")
            print(f"失败: {len(failed_asins)}")
            
            if remaining_credits is not None:
                print(f"剩余API配额: {remaining_credits}")
            
            if failed_asins:
                print("\n处理失败的ASIN:")
                for asin in failed_asins:
                    print(asin)
    
    except Exception as e:
        print(f"处理过程中出现错误: {str(e)}")

def fetch_product_data(asin, api_key):
    """获取产品数据并处理为CSV需要的格式"""
    # 重试机制
    for attempt in range(MAX_RETRIES):
        try:
            # 构建API请求URL，参照test.py的成功案例
            amazon_url = f"https://www.amazon.com/dp/{asin}/"
            full_url = f"{API_URL}?platform=amazon_detail&url={amazon_url}&api_key={api_key}"
            
            # 发送API请求
            response = requests.get(full_url)
            data = response.json()
            
            # 打印API响应，用于调试
            print(f"API响应: {data.keys()}")
            
            # 检查API响应
            if 'detail' in data:
                product = data['detail']
                
                # 准备CSV数据行
                csv_row = {
                    'asin': asin,
                    'name': product.get('name', ''),
                    'brand': product.get('brand', ''),
                    'url': product.get('url', ''),
                    'price': product.get('price', ''),
                    'price_reduced': product.get('price_reduced', ''),
                    'rating': product.get('rating', ''),
                    'review_count': product.get('total_ratings', ''),
                    'availability': product.get('in_stock', True),
                    'category': '; '.join([cat.get('name', '') for cat in product.get('categories', [])]),
                    'bullet_points': '; '.join(product.get('features', [])),
                    'description': truncate_text(product.get('description', ''), 1000),
                    'product_dimensions': json.dumps(next((item.get('value', '') for item in product.get('details_table', []) if item.get('name') == 'Product Dimensions'), '')),
                    'product_specifications': json.dumps(product.get('details_table', {})),
                    'product_weight': next((item.get('value', '') for item in product.get('details_table', []) if item.get('name') == 'Item Weight'), ''),
                    'main_image_url': product.get('main_image', ''),
                    'whats_in_box': '; '.join(product.get('whats_in_box', [])),
                    'variant_data': json.dumps(product.get('variants', {}))
                }
                
                # 保存剩余配额信息
                if 'remaining_credits' in data:
                    csv_row['remaining_credits'] = data['remaining_credits']
                
                return csv_row
            else:
                print(f"尝试 {attempt+1}/{MAX_RETRIES}: API返回错误: {data.get('message', '未知错误')}")
        
        except Exception as e:
            print(f"尝试 {attempt+1}/{MAX_RETRIES}: 请求异常: {str(e)}")
        
        # 如果不是最后一次尝试，则等待后重试
        if attempt < MAX_RETRIES - 1:
            time.sleep(REQUEST_DELAY * (attempt + 1))  # 递增等待时间
    
    # 所有尝试都失败
    return None

def truncate_text(text, max_length):
    """截断文本，避免过长"""
    if not text:
        return ""
    
    if len(text) <= max_length:
        return text
    
    return text[:max_length] + "..."

if __name__ == "__main__":
    main()
