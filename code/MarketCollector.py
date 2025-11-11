import logging
import os
import sys
from typing import List, Optional, Dict, Any
from contextlib import contextmanager
from datetime import datetime
import pandas as pd
from futu import OpenQuoteContext, RET_OK, SubType

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./logs/market_snapshot.log'),
        logging.StreamHandler()
    ]
)

class MarketSnapshotCollector:
    """市场快照数据收集器"""
    
    DEFAULT_STOCK_CODES = [
        'HK.00700',  # 腾讯控股
        'HK.01024',  # 快手-W
        'HK.03690',  # 美团-W
        'HK.09988',  # 阿里巴巴-W
        'HK.01810'   # 小米集团-W
    ]
    
    def __init__(self, host: str = '127.0.0.1', port: int = 11111, 
                 output_dir: str = '.', enable_logging: bool = True):
        """
        初始化市场快照收集器
        
        Args:
            host: FuTu API主机地址
            port: FuTu API端口
            output_dir: 输出文件目录
            enable_logging: 是否启用日志记录
        """
        self.host = host
        self.port = port
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__) if enable_logging else None
        self._ensure_output_dir()
    
    def _ensure_output_dir(self) -> None:
        """确保输出目录存在"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            if self.logger:
                self.logger.info(f"创建输出目录: {self.output_dir}")
    
    @contextmanager
    def _get_quote_context(self):
        """上下文管理器，确保连接正确关闭"""
        quote_ctx = None
        try:
            quote_ctx = OpenQuoteContext(host=self.host, port=self.port)
            if self.logger:
                self.logger.info(f"成功连接到 FuTu OpenD: {self.host}:{self.port}")
            yield quote_ctx
        except Exception as e:
            if self.logger:
                self.logger.error(f"连接FuTu OpenD失败: {e}")
            raise
        finally:
            if quote_ctx:
                quote_ctx.close()
                if self.logger:
                    self.logger.info("已关闭FuTu连接")
    
    def _generate_filename(self, base_name: str, date_str: Optional[str] = None) -> str:
        """生成文件名"""
        if date_str is None:
            date_str = datetime.now().strftime("%Y%m%d%H")
        filename = f"{base_name}_{date_str}.jsonl"
        return os.path.join(self.output_dir, filename)
    
    def _save_data_to_jsonl(self, data: pd.DataFrame, filename: str) -> bool:
        """
        保存数据到JSONL文件
        
        Args:
            data: 要保存的DataFrame数据
            filename: 文件名
            
        Returns:
            bool: 保存是否成功
        """
        try:
            data.to_json(filename, orient='records', lines=True, 
                        force_ascii=False, date_format='iso')
            if self.logger:
                self.logger.info(f"数据已保存到: {filename} (共{len(data)}条记录)")
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"保存文件失败 {filename}: {e}")
            return False
    
    def get_market_snapshot(self, stock_codes: Optional[List[str]] = None, 
                          custom_filename: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        获取市场快照数据
        
        Args:
            stock_codes: 股票代码列表，默认使用预设股票
            custom_filename: 自定义文件名（不含扩展名）
            
        Returns:
            pd.DataFrame: 快照数据，失败时返回None
        """
        if stock_codes is None:
            stock_codes = self.DEFAULT_STOCK_CODES
        
        if self.logger:
            self.logger.info(f"开始获取市场快照数据，股票数量: {len(stock_codes)}")
        
        try:
            with self._get_quote_context() as quote_ctx:
                ret, data = quote_ctx.get_market_snapshot(stock_codes)
                
                if ret != RET_OK:
                    if self.logger:
                        self.logger.error(f"获取市场快照失败: {data}")
                    return None
                
                if self.logger:
                    self.logger.info(f"成功获取{len(data)}只股票的快照数据")
                
                # 保存数据
                filename = self._generate_filename(
                    custom_filename or 'market_snapshot'
                )
                
                if self._save_data_to_jsonl(data, filename):
                    return data
                else:
                    return None
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取市场快照时发生错误: {e}")
            return None
    
    def get_stock_quotes(self, stock_codes: Optional[List[str]] = None,
                        custom_filename: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        获取股票实时报价数据
        
        Args:
            stock_codes: 股票代码列表
            custom_filename: 自定义文件名
            
        Returns:
            pd.DataFrame: 报价数据，失败时返回None
        """
        if stock_codes is None:
            stock_codes = self.DEFAULT_STOCK_CODES
            
        if self.logger:
            self.logger.info(f"开始获取股票报价数据，股票数量: {len(stock_codes)}")
        
        try:
            with self._get_quote_context() as quote_ctx:
                # 订阅股票
                ret_sub, err_message = quote_ctx.subscribe(
                    stock_codes, [SubType.QUOTE], subscribe_push=False
                )
                
                if ret_sub != RET_OK:
                    if self.logger:
                        self.logger.error(f"订阅失败: {err_message}")
                    return None
                
                # 获取报价数据
                ret, data = quote_ctx.get_stock_quote(stock_codes)
                
                if ret != RET_OK:
                    if self.logger:
                        self.logger.error(f"获取股票报价失败: {data}")
                    return None
                
                if self.logger:
                    self.logger.info(f"成功获取{len(data)}只股票的报价数据")
                
                # 保存数据
                filename = self._generate_filename(
                    custom_filename or 'futu_stock_quote'
                )
                
                if self._save_data_to_jsonl(data, filename):
                    return data
                else:
                    return None
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取股票报价时发生错误: {e}")
            return None


def main():
    """主函数"""
    try:
        # 创建收集器实例
        collector = MarketSnapshotCollector(
            host='127.0.0.1',
            port=11111,
            output_dir='./data/'
        )
        
        # 获取市场快照
        snapshot_data = collector.get_market_snapshot()
        if snapshot_data is not None:
            print(f"市场快照数据获取成功，共{len(snapshot_data)}条记录")
            # 显示部分关键数据
            if not snapshot_data.empty:
                key_columns = ['code', 'name', 'last_price', 'volume', 'turnover']
                available_columns = [col for col in key_columns if col in snapshot_data.columns]
                print("\n关键数据预览:")
                print(snapshot_data[available_columns].to_string(index=False))
        else:
            print("获取市场快照数据失败")
            return 1
        
        # 可选：获取实时报价数据
        # quote_data = collector.get_stock_quotes()
        # if quote_data is not None:
        #     print(f"实时报价数据获取成功，共{len(quote_data)}条记录")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n用户中断操作")
        return 1
    except Exception as e:
        logging.error(f"程序运行时发生错误: {e}")
        return 1


if __name__ == "__main__":
    main()
