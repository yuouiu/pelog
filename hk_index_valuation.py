import json
import requests
from datetime import datetime, timedelta
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HKIndexValuationBot:
    def __init__(self, config_file='config.json'):
        """初始化配置"""
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # 使用港股配置
        self.config = config['hk_config']
        self.lixinger_config = self.config['lixinger']
        self.dingtalk_config = self.config['dingtalk']
        self.stock_codes = self.config['stock_codes']
        self.index_names = self.config.get('index_names', {})
    
    def get_index_valuation(self, date=None):
        """获取港股指数估值数据"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        # 构建请求参数
        payload = {
            "token": self.lixinger_config['token'],
            "date": date,
            "stockCodes": self.stock_codes,
            "metricsList": [
                "pe_ttm.y10.mcw.cvpos"  # 市盈率TTM 10年历史百分位
            ]
        }
        
        # 添加详细日志输出
        logger.info(f"请求参数: {json.dumps(payload, ensure_ascii=False, indent=2)}")
        
        try:
            logger.info(f"正在获取 {date} 的港股指数估值数据...")
            response = requests.post(
                self.lixinger_config['api_url'],
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            logger.info(f"API响应状态码: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info("成功获取理杏仁港股API数据")
                # 详细打印API返回数据
                logger.info(f"API返回数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
                
                # 检查数据结构
                if 'data' in data and data['data']:
                    logger.info(f"获取到 {len(data['data'])} 条港股指数数据")
                    for i, item in enumerate(data['data']):
                        stock_code = item.get('stockCode', 'Unknown')
                        logger.info(f"指数 {i+1}: {stock_code} - {json.dumps(item, ensure_ascii=False)}")
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
            return "📊 港股指数估值数据获取失败"
        
        message_lines = [
            "🇭🇰 **港股指数估值播报**",
            f"📅 **日期**: {date}",
            ""
        ]
        
        logger.info(f"可用的指数名称映射: {json.dumps(self.index_names, ensure_ascii=False)}")
        
        try:
            processed_count = 0
            for item in valuation_data['data']:
                stock_code = item.get('stockCode', '')
                logger.info(f"处理指数: {stock_code}")
                
                # 使用配置文件中的指数名称映射
                index_name = self.index_names.get(stock_code, f"指数{stock_code}")
                logger.info(f"指数名称: {index_name}")
                
                # 获取估值百分位
                pe_percentile = None
                logger.info(f"原始数据结构: {json.dumps(item, ensure_ascii=False)}")
                
                # 直接从扁平化的键名获取百分位数据
                pe_percentile = item.get('pe_ttm.y10.mcw.cvpos')
                
                if pe_percentile is not None:
                    logger.info(f"提取到百分位: {pe_percentile}")
                    # 转换为百分比
                    pe_percentile_percent = pe_percentile * 100
                    # 根据百分位给出评级
                    if pe_percentile_percent <= 20:
                        level = "🟢 低估"
                    elif pe_percentile_percent <= 40:
                        level = "🟡 偏低"
                    elif pe_percentile_percent <= 60:
                        level = "🟠 适中"
                    elif pe_percentile_percent <= 80:
                        level = "🔴 偏高"
                    else:
                        level = "🔴 高估"
                    
                    line = f"📈 **{index_name}** | 估值: **{pe_percentile_percent:.1f}%** | {level}"
                    message_lines.append(line)
                    logger.info(f"添加消息行: {line}")
                    processed_count += 1
                else:
                    line = f"📈 **{index_name}** | 状态: ❌ 数据获取失败"
                    message_lines.append(line)
                    logger.warning(f"指数 {stock_code} 未能获取到百分位数据")
            
            logger.info(f"成功处理 {processed_count} 个指数的数据")
            
            message_lines.extend([
                "",
                "---",
                "",
                "💡 **估值说明**",
                "",
                "百分位越低表示估值越便宜：",
                "",
                "🟢 **0-20%**: 低估区域",
                "🟡 **20-40%**: 偏低区域", 
                "🟠 **40-60%**: 适中区域",
                "🔴 **60-80%**: 偏高区域",
                "🔴 **80-100%**: 高估区域"
            ])
            
        except Exception as e:
            logger.error(f"数据解析错误: {e}", exc_info=True)
            message_lines.append("❌ 数据解析失败")
        
        final_message = "\n\n".join(message_lines)
        logger.info(f"最终消息内容: {final_message}")
        return final_message
    
    def send_to_dingtalk(self, message):
        """发送消息到钉钉机器人"""
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": "港股指数估值播报",
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
        """运行港股估值播报任务"""
        success = False
        try:
            # 如果没有指定日期，使用7天前的日期（避免使用未来日期）
            if date is None:
                seven_days_ago = datetime.now() - timedelta(days=7)
                date = seven_days_ago.strftime('%Y-%m-%d')
            
            logger.info(f"开始执行港股指数估值播报任务，日期: {date}")
            
            # 获取估值数据
            valuation_data = self.get_index_valuation(date)
            
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
        bot = HKIndexValuationBot()
        
        # 可以指定日期，不指定则使用7天前
        # bot.run('2024-12-20')
        bot.run()
        
    except FileNotFoundError:
        logger.error("配置文件 config.json 不存在")
    except json.JSONDecodeError:
        logger.error("配置文件格式错误")
    except Exception as e:
        logger.error(f"程序执行出错: {e}")

if __name__ == "__main__":
    main()