import json
import requests
from datetime import datetime, timedelta
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StockValuationBot:
    def __init__(self, config_file='config.json'):
        """初始化配置"""
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # 使用股票配置
        self.config = config['stock_config']
        self.lixinger_config = self.config['lixinger']
        self.dingtalk_config = self.config['dingtalk']
        self.stock_codes = self.config['stock_codes']
        self.stock_names = self.config.get('stock_names', {})
    
    def get_stock_valuation(self, date=None):
        """获取股票估值数据"""
        if date is None:
            # 使用7天前的日期（避免使用未来日期）
            seven_days_ago = datetime.now() - timedelta(days=7)
            date = seven_days_ago.strftime('%Y-%m-%d')
        
        # 构建请求参数
        payload = {
            "token": self.lixinger_config['token'],
            "date": date,
            "stockCodes": self.stock_codes,
            "metricsList": [
                "pe_ttm",
                "pe_ttm.y3.cvpos",
                "pe_ttm.y5.cvpos", 
                "pe_ttm.y10.cvpos"
            ]
        }
        
        # 添加详细日志输出
        logger.info(f"请求参数: {json.dumps(payload, ensure_ascii=False, indent=2)}")
        
        try:
            logger.info(f"正在获取 {date} 的股票估值数据...")
            response = requests.post(
                self.lixinger_config['api_url'],
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            logger.info(f"API响应状态码: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info("成功获取理杏仁API数据")
                # 详细打印API返回数据
                logger.info(f"API返回数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
                
                # 检查数据结构
                if 'data' in data and data['data']:
                    logger.info(f"获取到 {len(data['data'])} 条股票数据")
                    for i, item in enumerate(data['data']):
                        stock_code = item.get('stockCode', 'Unknown')
                        logger.info(f"股票 {i+1}: {stock_code} - {json.dumps(item, ensure_ascii=False)}")
                else:
                    logger.warning("API返回数据中没有 'data' 字段或数据为空")
                
                return data
            else:
                logger.error(f"API请求失败，状态码: {response.status_code}")
                logger.error(f"响应内容: {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"请求异常: {e}")
            return None
    
    def format_message(self, valuation_data, date):
        """格式化钉钉消息"""
        logger.info(f"开始格式化消息，数据: {json.dumps(valuation_data, ensure_ascii=False) if valuation_data else 'None'}")
        
        if not valuation_data or 'data' not in valuation_data:
            logger.warning("估值数据为空或缺少data字段")
            return "📊 股票估值数据获取失败"
        
        message_lines = [
            "📊 **股票估值播报**",
            f"📅 **日期**: {date}",
            ""
        ]
        
        logger.info(f"可用的股票名称映射: {json.dumps(self.stock_names, ensure_ascii=False)}")
        
        try:
            processed_count = 0
            for item in valuation_data['data']:
                stock_code = item.get('stockCode', '')
                logger.info(f"处理股票: {stock_code}")
                
                # 使用配置文件中的股票名称映射
                stock_name = self.stock_names.get(stock_code, f"股票{stock_code}")
                logger.info(f"股票名称: {stock_name}")
                
                # 获取估值数据
                pe_ttm = item.get('pe_ttm')
                pe_3y_pos = item.get('pe_ttm.y3.cvpos')
                pe_5y_pos = item.get('pe_ttm.y5.cvpos')
                pe_10y_pos = item.get('pe_ttm.y10.cvpos')
                
                logger.info(f"原始数据: PE_TTM={pe_ttm}, 3年百分位={pe_3y_pos}, 5年百分位={pe_5y_pos}, 10年百分位={pe_10y_pos}")
                
                if pe_ttm is not None:
                    # 构建消息行
                    line_parts = [f"📈 **{stock_name}({stock_code})**"]
                    line_parts.append(f"PE: **{pe_ttm:.2f}**")
                    
                    # 添加百分位信息
                    percentile_info = []
                    if pe_3y_pos is not None:
                        percentile_info.append(f"3年: {pe_3y_pos*100:.1f}%")
                    if pe_5y_pos is not None:
                        percentile_info.append(f"5年: {pe_5y_pos*100:.1f}%")
                    if pe_10y_pos is not None:
                        percentile_info.append(f"10年: {pe_10y_pos*100:.1f}%")
                    
                    if percentile_info:
                        line_parts.append(f"百分位: {' | '.join(percentile_info)}")
                    
                    # 根据10年百分位给出评级（优先使用10年，其次5年，最后3年）
                    main_percentile = pe_10y_pos or pe_5y_pos or pe_3y_pos
                    if main_percentile is not None:
                        percentile_percent = main_percentile * 100
                        if percentile_percent <= 20:
                            level = "🟢 低估"
                        elif percentile_percent <= 40:
                            level = "🟡 偏低"
                        elif percentile_percent <= 60:
                            level = "🟠 适中"
                        elif percentile_percent <= 80:
                            level = "🔴 偏高"
                        else:
                            level = "🔴 高估"
                        line_parts.append(level)
                    
                    line = " | ".join(line_parts) + "  "
                    message_lines.append(line)
                    logger.info(f"添加消息行: {line}")
                    processed_count += 1
                else:
                    line = f"📈 **{stock_name}({stock_code})** | 状态: ❌ 数据获取失败  "
                    message_lines.append(line)
                    logger.warning(f"股票 {stock_code} 未能获取到PE数据")
            
            logger.info(f"成功处理 {processed_count} 个股票的数据")
            
            message_lines.extend([
                "",
                "---",
                "",
                "💡 **估值说明**",
                "",
                "百分位越低表示估值越便宜：",
                "",
                "🟢 **0-20%**: 低估区域  ",
                "🟡 **20-40%**: 偏低区域  ", 
                "🟠 **40-60%**: 适中区域  ",
                "🔴 **60-80%**: 偏高区域  ",
                "🔴 **80-100%**: 高估区域  "
            ])
            
        except Exception as e:
            logger.error(f"数据解析错误: {e}", exc_info=True)
            message_lines.append("❌ 数据解析失败")
        
        final_message = "\n".join(message_lines)
        logger.info(f"最终消息内容: {final_message}")
        return final_message
    
    def send_to_dingtalk(self, message):
        """发送消息到钉钉机器人"""
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": "股票估值播报",
                "text": message
            }
        }
        
        try:
            logger.info("正在发送消息到钉钉...")
            response = requests.post(
                self.dingtalk_config['webhook_url'],
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('errcode') == 0:
                    logger.info("消息发送成功")
                    return True
                else:
                    logger.error(f"钉钉API返回错误: {result}")
                    return False
            else:
                logger.error(f"钉钉请求失败，状态码: {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"钉钉请求异常: {e}")
            return False
    
    def run(self, date=None):
        """运行股票估值播报任务"""
        success = False
        try:
            # 如果没有指定日期，使用7天前的日期（避免使用未来日期）
            if date is None:
                seven_days_ago = datetime.now() - timedelta(days=7)
                date = seven_days_ago.strftime('%Y-%m-%d')
            
            logger.info(f"开始执行股票估值播报任务，日期: {date}")
            
            # 获取估值数据
            valuation_data = self.get_stock_valuation(date)
            
            if valuation_data:
                # 格式化消息
                message = self.format_message(valuation_data, date)
                
                # 发送到钉钉
                if self.send_to_dingtalk(message):
                    logger.info("任务执行成功")
                    success = True
                else:
                    logger.error("钉钉消息发送失败")
            else:
                logger.error("获取估值数据失败")
                
        except Exception as e:
            logger.error(f"任务执行失败: {e}", exc_info=True)
        
        return success

def main():
    """主函数"""
    try:
        bot = StockValuationBot()
        
        # 可以指定日期，不指定则使用7天前的日期
        # bot.run('2025-09-09')
        bot.run()
        
    except FileNotFoundError:
        logger.error("配置文件 config.json 不存在")
    except json.JSONDecodeError:
        logger.error("配置文件格式错误")
    except Exception as e:
        logger.error(f"程序执行出错: {e}")

if __name__ == "__main__":
    main()