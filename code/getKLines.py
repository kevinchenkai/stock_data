# 实时K线：https://openapi.futunn.com/futu-api-doc/quote/get-kl.html
# 历史K线：https://openapi.futunn.com/futu-api-doc/quote/request-history-kline.html

from futu import *
import os
from datetime import datetime
import time

STOCK_CODES = [
    'HK.00700',  # 腾讯控股
    'HK.01024',  # 快手-W
    'HK.03690',  # 美团-W
    'HK.09988',  # 阿里巴巴-W
    'HK.01810'   # 小米集团-W
]

def get_stock_kline(stock_code, days=10, kl_type=KLType.K_DAY, au_type=AuType.QFQ):
    """
    获取单只股票的K线数据
    
    Args:
        stock_code (str): 股票代码
        days (int): 获取天数，默认10天
        kl_type: K线类型，默认日K
        au_type: 复权类型，默认前复权
    
    Returns:
        tuple: (成功标志, 数据或错误信息)
    """
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    
    try:
        # 订阅K线数据
        ret_sub, err_message = quote_ctx.subscribe([stock_code], [SubType.K_DAY], 
                                                  subscribe_push=False, session=Session.ALL)
        
        if ret_sub == RET_OK:
            # 获取K线数据
            ret, data = quote_ctx.get_cur_kline(stock_code, days, kl_type, au_type)
            if ret == RET_OK:
                return True, data
            else:
                return False, f"获取K线数据失败: {data}"
        else:
            return False, f"订阅失败: {err_message}"
            
    except Exception as e:
        return False, f"请求异常: {str(e)}"
    finally:
        quote_ctx.close()

def save_kline_data(stock_code, data, data_dir='./data'):
    """
    保存K线数据到文件
    
    Args:
        stock_code (str): 股票代码
        data: K线数据DataFrame
        data_dir (str): 数据保存目录
    """
    # 确保数据目录存在
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    # 生成文件名：股票代码_kline_日期.jsonl
    today = datetime.now().strftime('%Y%m%d')
    stock_symbol = stock_code.replace('.', '_').lower()
    filename = f"{stock_symbol}_kline_{today}.jsonl"
    filepath = os.path.join(data_dir, filename)
    
    # 保存数据
    data.to_json(filepath, orient='records', lines=True, force_ascii=False)
    print(f"✓ {stock_code} K线数据已保存到: {filepath}")

def get_all_stocks_kline(stock_codes, days=10, delay=1):
    """
    获取所有股票的K线数据
    
    Args:
        stock_codes (list): 股票代码列表
        days (int): 获取天数
        delay (int): 请求间隔时间（秒），避免频繁请求
    """
    print(f"开始获取 {len(stock_codes)} 只股票的K线数据...")
    print("=" * 50)
    
    success_count = 0
    failed_stocks = []
    
    for i, stock_code in enumerate(stock_codes, 1):
        print(f"[{i}/{len(stock_codes)}] 正在处理: {stock_code}")
        
        # 获取K线数据
        success, result = get_stock_kline(stock_code, days)
        
        if success:
            # 保存数据
            save_kline_data(stock_code, result)
            success_count += 1
        else:
            print(f"✗ {stock_code} 获取失败: {result}")
            failed_stocks.append((stock_code, result))
        
        # 添加延迟，避免请求过于频繁
        if i < len(stock_codes):
            time.sleep(delay)
    
    # 输出统计结果
    print("=" * 50)
    print(f"处理完成！成功: {success_count}/{len(stock_codes)}")
    
    if failed_stocks:
        print(f"失败的股票 ({len(failed_stocks)}):")
        for stock_code, error in failed_stocks:
            print(f"  - {stock_code}: {error}")

if __name__ == "__main__":
    # 获取所有股票的K线数据
    get_all_stocks_kline(STOCK_CODES, days=64, delay=1)
