import akshare as ak
import json
import os
from datetime import datetime
import pandas as pd
import shutil
import logging
import time

class StockAnalyzer:
    def __init__(self):
        self.current_dir = os.getcwd()
        self.data_dir = os.path.join(self.current_dir, 'data')
        self.log_dir = os.path.join(self.current_dir, 'log')
        self.care_file = os.path.join(self.current_dir, 'care.json')
        self.result_file = os.path.join(self.current_dir, 'result.md')
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 确保data和log目录存在
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        
        # 配置日志系统
        self.setup_logging()
    
    def setup_logging(self):
        """配置日志系统"""
        log_file = os.path.join(self.log_dir, f'log_{self.today}.txt')
        
        # 配置日志格式
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()  # 同时输出到控制台
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"股票分析程序启动 - {self.current_time}")
    
    def fetch_data_by_ak(self, method_name, max_retries=3, retry_delay=25, **kwargs):
        """统一的akshare数据获取方法，支持重试机制"""
        for attempt in range(max_retries):
            try:
                match method_name:
                    case "trade_calendar":
                        self.logger.info(f"开始获取交易日历数据（第{attempt + 1}次尝试）")
                        data = ak.tool_trade_date_hist_sina()
                        self.logger.info(f"成功获取交易日历数据，共{len(data)}条记录")
                        return data
                    case "stock_spot":
                        self.logger.info(f"开始获取A股现货数据（第{attempt + 1}次尝试）")
                        data = ak.stock_zh_a_spot_em()
                        self.logger.info(f"成功获取A股现货数据，共{len(data)}只股票")
                        return data
                    case "main_fund_flow":
                        symbol = kwargs.get('symbol', '全部股票')
                        self.logger.info(f"开始获取主力资金流向数据（第{attempt + 1}次尝试）")
                        data = ak.stock_main_fund_flow(symbol=symbol)
                        self.logger.info(f"获取到主力资金流向数据，共{len(data)}只股票")
                        return data
                    case _:
                        self.logger.error(f"不支持的akshare方法: {method_name}")
                        return None
            except Exception as e:
                self.logger.error(f"获取{method_name}数据失败（第{attempt + 1}次尝试）: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"等待{retry_delay}秒后重试...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error(f"获取{method_name}数据最终失败，已重试{max_retries}次")
        
        return None
    
    def get_trade_calendar(self):
        """获取交易日历"""
        return self.fetch_data_by_ak("trade_calendar")
    
    def is_trading_day(self):
        """检查今天是否是交易日"""
        trade_calendar = self.get_trade_calendar()
        if trade_calendar is None:
            self.logger.error("无法获取交易日历，无法判断是否为交易日")
            return False
        
        today_str = self.today
        trading_days = trade_calendar['trade_date'].astype(str).tolist()
        is_trading = today_str in trading_days
        
        if is_trading:
            self.logger.info(f"今天({today_str})是交易日")
        else:
            self.logger.info(f"今天({today_str})不是交易日")
        
        return is_trading
    
    def load_care_stocks(self):
        """从care.json中加载关注的股票"""
        if os.path.exists(self.care_file):
            try:
                with open(self.care_file, 'r', encoding='utf-8') as f:
                    care_stocks = json.load(f)
                self.logger.info(f"成功加载关注股票列表，共{len(care_stocks)}只股票")
                return care_stocks
            except Exception as e:
                self.logger.error(f"读取care.json失败: {e}")
                # # print(f"读取care.json失败: {e}")
                return []
        else:
            self.logger.info("care.json文件不存在，返回空列表")
            return []
    
    def load_previous_settlements(self):
        """从result.json中读取上一周期的结算结果"""
        result_json_file = os.path.join(self.current_dir, 'result.json')
        if os.path.exists(result_json_file):
            try:
                with open(result_json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    previous_settlements = data.get('settled_stocks', [])
                self.logger.info(f"成功加载历史结算记录，共{len(previous_settlements)}条记录")
                return previous_settlements
            except Exception as e:
                self.logger.error(f"读取result.json失败: {e}")
                return []
        else:
            self.logger.info("result.json文件不存在，返回空列表")
            return []
    
    def get_stock_data(self):
        """获取股票数据"""
        return self.fetch_data_by_ak("stock_spot")
    
    def get_main_fund_flow(self):
        """获取主力净流入排名"""
        stock_main_fund_flow_df = self.fetch_data_by_ak("main_fund_flow", symbol="全部股票")
        if stock_main_fund_flow_df is None:
            return None, None
            
        df = stock_main_fund_flow_df
        
        filtered_df = df[
            (df["最新价"].notna()) &
            (df["今日排行榜-主力净占比"] > 0) &
            (~df["名称"].str.contains("ST", na=False)) &
            (df["今日排行榜-今日涨跌"] < 0)
        ]
        
        self.logger.info(f"筛选后符合条件的股票数量: {len(filtered_df)}")
        
        # 按照"今日排行榜-主力净占比"倒序排列
        result_df = filtered_df.sort_values(by="今日排行榜-主力净占比", ascending=False)
        return result_df, stock_main_fund_flow_df
    
    def analyze_stocks(self):
        """分析股票"""
        # 检查是否是交易日
        if not self.is_trading_day():
            self.logger.info(f"今天({self.today})不是交易日，程序结束")
            # print(f"今天({self.today})不是交易日，程序结束")
            return
        
        self.logger.info(f"今天({self.today})是交易日，开始股票分析")
        # print(f"今天({self.today})是交易日，开始分析...")
        
        # 加载关注的股票
        care_stocks = self.load_care_stocks()
        
        # 获取股票数据
        stock_data = self.get_stock_data()
        if stock_data is None:
            self.logger.error("无法获取股票数据，程序结束")
            # print("无法获取股票数据，程序结束")
            return
        
        # 获取主力净流入排名
        result_df, main_fund_flow_df = self.get_main_fund_flow()
        if result_df is None or main_fund_flow_df is None:
            self.logger.error("无法获取主力净流入排名，程序结束")
            # print("无法获取主力净流入排名，程序结束")
            return
        
        # 取前五名股票代码
        top_5_stocks = result_df.head(5)['代码'].tolist() if len(result_df) >= 5 else []
        self.logger.info(f"筛选出前5名股票: {top_5_stocks}")
        
        # 分析现有关注股票
        settled_stocks = []  # 已结算股票
        updated_care_stocks = []  # 更新后的关注股票
        
        for stock in care_stocks:
            stock_code = stock['code']
            # 在主力净流入数据中查找该股票
            stock_flow_data = main_fund_flow_df[main_fund_flow_df['代码'] == stock_code]
            stock_all_data = stock_data[stock_data['代码'] == stock_code]
            if not stock_all_data.empty:
                highest_price = stock_all_data.iloc[0]['最高']
            else:
                highest_price = None

            if not stock_flow_data.empty:
                main_net_ratio = stock_flow_data.iloc[0]['今日排行榜-主力净占比']
                current_price = stock_flow_data.iloc[0]['最新价']
                today_change = stock_flow_data.iloc[0]['今日排行榜-今日涨跌']
                stock_name = stock_flow_data.iloc[0]['名称']
                
                # 计算今日最高价与模拟买入价格的差值
                price_difference = current_price - stock['buy_price']

                if main_net_ratio < -5:
                    # 结算股票
                    profit_loss = current_price - stock['buy_price']
                    if highest_price:
                        if highest_price > stock['buy_price']:
                            profit_loss = 0 # 假设最高价大于买入价格的时候，在当前价格与买入价格相等时候卖出
                    self.logger.info(f"股票{stock_code}({stock_name})触发卖出条件，主力净占比{main_net_ratio}%，盈亏{profit_loss:.2f}")
                    settled_stock = {
                        'code': stock_code,
                        'name': stock_name,
                        'buy_price': stock['buy_price'],
                        'start_time': stock['start_time'],
                        'current_price': current_price,
                        'main_net_ratio': main_net_ratio,
                        'today_change': today_change,
                        'profit_loss': profit_loss,
                        'remark': f"主力净占比{main_net_ratio}%小于-5%，触发卖出条件"
                    }
                    settled_stocks.append(settled_stock)
                elif today_change > 10 :
                    # 结算股票
                    profit_loss = current_price - stock['buy_price']
                    self.logger.info(f"股票{stock_code}({stock_name})触发卖出条件，主力净占比{main_net_ratio}%，盈亏{profit_loss:.2f}")
                    settled_stock = {
                        'code': stock_code,
                        'name': stock_name,
                        'buy_price': stock['buy_price'],
                        'start_time': stock['start_time'],
                        'current_price': current_price,
                        'main_net_ratio': main_net_ratio,
                        'today_change': today_change,
                        'profit_loss': profit_loss,
                        'remark': f"今日涨幅{main_net_ratio}%大于10%，触发卖出条件"
                    }
                    settled_stocks.append(settled_stock)
                elif today_change > 0 and main_net_ratio < 0:
                    # 结算股票
                    profit_loss = current_price - stock['buy_price']
                    if highest_price:
                        if highest_price > stock['buy_price']:
                            profit_loss = 0 # 假设最高价大于买入价格的时候，在当前价格与买入价格相等时候卖出
                    self.logger.info(f"股票{stock_code}({stock_name})触发卖出条件，主力净占比{main_net_ratio}%，盈亏{profit_loss:.2f}")
                    settled_stock = {
                        'code': stock_code,
                        'name': stock_name,
                        'buy_price': stock['buy_price'],
                        'start_time': stock['start_time'],
                        'current_price': current_price,
                        'main_net_ratio': main_net_ratio,
                        'today_change': today_change,
                        'profit_loss': profit_loss,
                        'remark': f"今日涨幅{main_net_ratio}%大于0%，且主力净占比{main_net_ratio}%小于0%，触发卖出条件"
                    }
                    settled_stocks.append(settled_stock)
                else:
                    # 继续关注
                    stock['current_price'] = current_price
                    stock['main_net_ratio'] = main_net_ratio
                    stock['today_change'] = today_change
                    stock['name'] = stock_name
                    updated_care_stocks.append(stock)
            else:
                # 如果在主力净流入数据中找不到，继续关注
                updated_care_stocks.append(stock)
        
        # 分析前五名股票，添加到关注列表
        for stock_code in top_5_stocks:
            # 检查是否已经在关注列表中
            if not any(stock['code'] == stock_code for stock in updated_care_stocks):
                # 在股票数据中查找当前价格
                stock_info = stock_data[stock_data['代码'] == stock_code]
                if not stock_info.empty:
                    current_price = stock_info.iloc[0]['最新价']
                    stock_name = stock_info.iloc[0]['名称']
                    
                    main_net_ratio = result_df[result_df['代码'] == stock_code].iloc[0]['今日排行榜-主力净占比']
                    today_change = result_df[result_df['代码'] == stock_code].iloc[0]['今日排行榜-今日涨跌']
                    
                    self.logger.info(f"新增关注股票{stock_code}({stock_name})，买入价格{current_price:.2f}")
                    
                    new_stock = {
                        'code': stock_code,
                        'name': stock_name,
                        'buy_price': current_price,
                        'start_time': self.current_time,
                        'current_price': current_price,
                        'main_net_ratio': main_net_ratio,
                        'today_change': today_change
                    }
                    updated_care_stocks.append(new_stock)
        
        # 加载历史结算记录
        previous_settlements = self.load_previous_settlements()
        
        # 合并当前结算和历史结算
        all_settled_stocks = previous_settlements + settled_stocks
        
        # 保存结果
        self.save_care_stocks(updated_care_stocks)
        self.save_settlements_to_json(all_settled_stocks)
        self.save_result_report(updated_care_stocks, all_settled_stocks)
        
        self.logger.info(f"股票分析完成，当前关注{len(updated_care_stocks)}只股票，本次结算{len(settled_stocks)}只股票，历史结算{len(previous_settlements)}只股票")
        # print(f"分析完成，当前关注{len(updated_care_stocks)}只股票，结算{len(settled_stocks)}只股票")
    
    def save_care_stocks(self, care_stocks):
        """保存关注股票到care.json"""
        try:
            # 保存到当前目录
            with open(self.care_file, 'w', encoding='utf-8') as f:
                json.dump(care_stocks, f, ensure_ascii=False, indent=4)
            
            # 备份到data目录
            backup_file = os.path.join(self.data_dir, f'care_{self.today}.json')
            shutil.copy2(self.care_file, backup_file)
            
            self.logger.info(f"care.json已保存，备份文件: {backup_file}")
            # print(f"care.json已保存，备份文件: {backup_file}")
        except Exception as e:
            self.logger.error(f"保存care.json失败: {e}")
            # print(f"保存care.json失败: {e}")
    
    def save_settlements_to_json(self, settled_stocks):
        """保存结算结果到result.json"""
        try:
            result_json_file = os.path.join(self.current_dir, 'result.json')
            result_data = {
                'last_update': self.current_time,
                'settled_stocks': settled_stocks
            }
            
            with open(result_json_file, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=4)
            
            self.logger.info(f"result.json已保存，共{len(settled_stocks)}条结算记录")
        except Exception as e:
            self.logger.error(f"保存result.json失败: {e}")
    
    def save_result_report(self, care_stocks, settled_stocks):
        """保存分析结果到result.md"""
        try:
            content = f"# 股票分析结果（{self.current_time}）\n\n"
            
            # 当前关注的股票
            content += "## 当前关注的股票\n\n"
            content += "股票代码 | 股票名称 | 模拟买入价格 | 关注的起始时间 | 当前价格 | 今日排行榜-主力净占比 | 今日排行榜-今日涨跌 | 备注\n"
            content += "--- | --- | --- | --- | --- | --- | --- | ---\n"
            
            if care_stocks:
                for stock in care_stocks:
                    remark = stock.get('remark', '正常关注中')
                    content += f"{stock['code']} | {stock.get('name', 'N/A')} | {stock['buy_price']:.2f} | {stock['start_time']} | {stock.get('current_price', 'N/A')} | {stock.get('main_net_ratio', 'N/A')}% | {stock.get('today_change', 'N/A')}% | {remark}\n"
            else:
                content += "暂无关注股票\n"
            
            content += "\n"
            
            # 已经结算的股票
            content += "## 已经结算的股票\n\n"
            content += "股票代码 | 股票名称 | 模拟买入价格 | 关注的起始时间 | 当前价格 | 今日排行榜-主力净占比 | 今日排行榜-今日涨跌 | 盈亏 | 备注\n"
            content += "--- | --- | --- | --- | --- | --- | --- | --- | ---\n"
            
            if settled_stocks:
                for stock in settled_stocks:
                    profit_loss_str = f"{stock['profit_loss']:+.2f}"
                    content += f"{stock['code']} | {stock['name']} | {stock['buy_price']:.2f} | {stock['start_time']} | {stock['current_price']:.2f} | {stock['main_net_ratio']}% | {stock['today_change']}% | {profit_loss_str} | {stock['remark']}\n"
            else:
                content += "暂无结算股票\n"
            
            with open(self.result_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            self.logger.info(f"result.md已保存")
            # print(f"result.md已保存")
        except Exception as e:
            self.logger.error(f"保存result.md失败: {e}")
            # print(f"保存result.md失败: {e}")

if __name__ == "__main__":
    analyzer = StockAnalyzer()
    analyzer.analyze_stocks()