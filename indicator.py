import akshare as ak
import pandas as pd
from datetime import datetime
import json
import requests
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IndicatorBot:
    def __init__(self, config_file='config.json'):
        """初始化配置"""
        with open(config_file, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        self.dingtalk_config = self.config['cn_config']['dingtalk']
    
    def get_stock_indicators(self):
        """获取股票相关指标数据"""
        logger.info(f"开始获取股票指标数据 - {datetime.now()}")
        
        indicators_data = {}
        
        # 1. 股债利差 - 只取最新日期的数据
        logger.info("正在获取股债利差数据...")
        try:
            stock_ebs_lg_df = ak.stock_ebs_lg()
            if not stock_ebs_lg_df.empty:
                # 取最新日期的数据
                latest_ebs = stock_ebs_lg_df.iloc[-1]
                indicators_data['股债利差'] = latest_ebs.to_dict()
                logger.info("股债利差数据获取成功")
            else:
                logger.warning("股债利差数据为空")
                indicators_data['股债利差'] = "数据为空"
        except Exception as e:
            logger.error(f"获取股债利差数据失败: {e}")
            indicators_data['股债利差'] = f"获取失败: {e}"
        
        # 2. 巴菲特指标 - 只取最新日期的数据
        logger.info("正在获取巴菲特指标数据...")
        try:
            stock_buffett_index_lg_df = ak.stock_buffett_index_lg()
            if not stock_buffett_index_lg_df.empty:
                # 取最新日期的数据
                latest_buffett = stock_buffett_index_lg_df.iloc[-1]
                indicators_data['巴菲特指标'] = latest_buffett.to_dict()
                logger.info("巴菲特指标数据获取成功")
            else:
                logger.warning("巴菲特指标数据为空")
                indicators_data['巴菲特指标'] = "数据为空"
        except Exception as e:
            logger.error(f"获取巴菲特指标数据失败: {e}")
            indicators_data['巴菲特指标'] = f"获取失败: {e}"
        
        # 3. A股等权重与中位数市盈率 - 新增字段并重命名
        logger.info("正在获取A股等权重与中位数市盈率数据...")
        try:
            stock_a_ttm_lyr_df = ak.stock_a_ttm_lyr()
            if not stock_a_ttm_lyr_df.empty:
                # 定义字段映射关系
                field_mapping = {
                    'date': '日期',
                    'middlePETTM': '全A股滚动市盈率(TTM)中位数',
                    'averagePETTM': '全A股滚动市盈率(TTM)等权平均',
                    'quantileInRecent10YearsMiddlePeTtm': '当前"TTM(滚动市盈率)中位数"在最近10年数据上的分位数',
                    'quantileInRecent10YearsAveragePeTtm': '当前"TTM(滚动市盈率)等权平均"在在最近10年数据上的分位数'
                }
                
                # 检查哪些字段存在
                available_columns = [col for col in field_mapping.keys() if col in stock_a_ttm_lyr_df.columns]
                
                if available_columns:
                    latest_pe_data = stock_a_ttm_lyr_df[available_columns].iloc[-1]
                    # 重命名字段
                    renamed_data = {}
                    for col in available_columns:
                        renamed_data[field_mapping[col]] = latest_pe_data[col]
                    indicators_data['A股市盈率指标'] = renamed_data
                    logger.info("A股市盈率指标数据获取成功")
                else:
                    logger.warning("未找到指定的字段")
                    indicators_data['A股市盈率指标'] = "指定字段不存在"
            else:
                logger.warning("A股等权重与中位数市盈率数据为空")
                indicators_data['A股市盈率指标'] = "数据为空"
        except Exception as e:
            logger.error(f"获取A股等权重与中位数市盈率数据失败: {e}")
            indicators_data['A股市盈率指标'] = f"获取失败: {e}"
        
        logger.info(f"数据获取完成 - {datetime.now()}")
        return indicators_data
    
    def format_message(self, indicators_data):
        """格式化钉钉消息"""
        logger.info("开始格式化消息")
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        message_lines = [
            "📊 **股票指标数据播报**",
            f"📅 **日期**: {current_date}",
            ""
        ]
        
        try:
            # 股债利差
            if '股债利差' in indicators_data:
                message_lines.append("📈 **股债利差**")
                ebs_data = indicators_data['股债利差']
                if isinstance(ebs_data, dict):
                    for key, value in ebs_data.items():
                        if key == 'date':
                            message_lines.append(f"📅 日期: {value}")
                        else:
                            message_lines.append(f"📊 {key}: {value}")
                else:
                    message_lines.append(f"❌ {ebs_data}")
                message_lines.append("")
            
            # 巴菲特指标
            if '巴菲特指标' in indicators_data:
                message_lines.append("💰 **巴菲特指标**")
                buffett_data = indicators_data['巴菲特指标']
                if isinstance(buffett_data, dict):
                    for key, value in buffett_data.items():
                        if key == 'date':
                            message_lines.append(f"📅 日期: {value}")
                        else:
                            message_lines.append(f"📊 {key}: {value}")
                else:
                    message_lines.append(f"❌ {buffett_data}")
                message_lines.append("")
            
            # A股市盈率指标
            if 'A股市盈率指标' in indicators_data:
                message_lines.append("📈 **A股市盈率指标**")
                pe_data = indicators_data['A股市盈率指标']
                if isinstance(pe_data, dict):
                    for key, value in pe_data.items():
                        if '日期' in key:
                            message_lines.append(f"📅 {key}: {value}")
                        elif '分位数' in key:
                            # 分位数转换为百分比显示
                            if isinstance(value, (int, float)):
                                percentage = value * 100
                                message_lines.append(f"📊 {key}: {percentage:.1f}%")
                            else:
                                message_lines.append(f"📊 {key}: {value}")
                        else:
                            message_lines.append(f"📊 {key}: {value}")
                else:
                    message_lines.append(f"❌ {pe_data}")
                message_lines.append("")
            
            message_lines.extend([
                "---",
                "",
                "💡 **数据说明**",
                "",
                "📊 股债利差：股票收益率与债券收益率的差值",
                "💰 巴菲特指标：股市总市值与GDP的比值",
                "📈 市盈率分位数：当前估值在历史数据中的相对位置"
            ])
            
        except Exception as e:
            logger.error(f"消息格式化错误: {e}", exc_info=True)
            message_lines.append("❌ 数据格式化失败")
        
        # 钉钉需要使用双换行来确保分行
        final_message = "\n\n".join(message_lines)
        logger.info("消息格式化完成")
        return final_message
    
    def send_to_dingtalk(self, message):
        """发送消息到钉钉机器人"""
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": "股票指标数据播报",
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
    
    def run(self):
        """运行指标播报任务"""
        success = False
        try:
            logger.info("开始执行股票指标播报任务")
            
            # 获取指标数据
            indicators_data = self.get_stock_indicators()
            
            if indicators_data:
                # 格式化消息
                message = self.format_message(indicators_data)
                
                # 发送到钉钉
                if self.send_to_dingtalk(message):
                    logger.info("任务执行成功")
                    success = True
                else:
                    logger.error("钉钉消息发送失败")
            else:
                logger.error("获取指标数据失败")
                
        except Exception as e:
            logger.error(f"任务执行失败: {e}", exc_info=True)
        
        return success

# 保留原有的独立函数，用于向后兼容
def get_stock_indicators():
    """获取股票相关指标数据（原有函数，保持兼容性）"""
    print(f"开始获取股票指标数据 - {datetime.now()}")
    
    # 1. 股债利差 - 只取最新日期的数据
    print("正在获取股债利差数据...")
    try:
        stock_ebs_lg_df = ak.stock_ebs_lg()
        if not stock_ebs_lg_df.empty:
            # 取最新日期的数据
            latest_ebs = stock_ebs_lg_df.iloc[-1]
            print("股债利差最新数据:")
            print(latest_ebs)
            print("-" * 50)
        else:
            print("股债利差数据为空")
    except Exception as e:
        print(f"获取股债利差数据失败: {e}")
    
    # 2. 巴菲特指标 - 只取最新日期的数据
    print("正在获取巴菲特指标数据...")
    try:
        stock_buffett_index_lg_df = ak.stock_buffett_index_lg()
        if not stock_buffett_index_lg_df.empty:
            # 取最新日期的数据
            latest_buffett = stock_buffett_index_lg_df.iloc[-1]
            print("巴菲特指标最新数据:")
            print(latest_buffett)
            print("-" * 50)
        else:
            print("巴菲特指标数据为空")
    except Exception as e:
        print(f"获取巴菲特指标数据失败: {e}")
    
    # 3. A股等权重与中位数市盈率 - 新增字段并重命名
    print("正在获取A股等权重与中位数市盈率数据...")
    try:
        stock_a_ttm_lyr_df = ak.stock_a_ttm_lyr()
        if not stock_a_ttm_lyr_df.empty:
            # 定义字段映射关系
            field_mapping = {
                'date': '日期',
                'middlePETTM': '全A股滚动市盈率(TTM)中位数',
                'averagePETTM': '全A股滚动市盈率(TTM)等权平均',
                'quantileInRecent10YearsMiddlePeTtm': '当前"TTM(滚动市盈率)中位数"在最近10年数据上的分位数',
                'quantileInRecent10YearsAveragePeTtm': '当前"TTM(滚动市盈率)等权平均"在在最近10年数据上的分位数'
            }
            
            # 检查哪些字段存在
            available_columns = [col for col in field_mapping.keys() if col in stock_a_ttm_lyr_df.columns]
            
            if available_columns:
                selected_data = stock_a_ttm_lyr_df[available_columns].copy()
                
                # 重命名字段
                rename_dict = {col: field_mapping[col] for col in available_columns}
                selected_data = selected_data.rename(columns=rename_dict)
                
                print("A股等权重与中位数市盈率指定字段数据:")
                print(selected_data)
                print(f"\n数据形状: {selected_data.shape}")
                print(f"可用字段: {list(selected_data.columns)}")
                
                # 显示最新数据
                if len(selected_data) > 0:
                    print("\n最新数据:")
                    print(selected_data.iloc[-1])
            else:
                print("未找到指定的字段")
                print(f"可用字段: {list(stock_a_ttm_lyr_df.columns)}")
        else:
            print("A股等权重与中位数市盈率数据为空")
    except Exception as e:
        print(f"获取A股等权重与中位数市盈率数据失败: {e}")
    
    print(f"数据获取完成 - {datetime.now()}")

def get_latest_indicators_summary():
    """获取所有指标的最新数据摘要（原有函数，保持兼容性）"""
    summary = {}
    
    try:
        # 股债利差最新数据
        ebs_df = ak.stock_ebs_lg()
        if not ebs_df.empty:
            summary['股债利差'] = ebs_df.iloc[-1].to_dict()
    except Exception as e:
        summary['股债利差'] = f"获取失败: {e}"
    
    try:
        # 巴菲特指标最新数据
        buffett_df = ak.stock_buffett_index_lg()
        if not buffett_df.empty:
            summary['巴菲特指标'] = buffett_df.iloc[-1].to_dict()
    except Exception as e:
        summary['巴菲特指标'] = f"获取失败: {e}"
    
    try:
        # A股市盈率指定字段最新数据
        pe_df = ak.stock_a_ttm_lyr()
        if not pe_df.empty:
            # 定义字段映射关系
            field_mapping = {
                'date': '日期',
                'middlePETTM': '全A股滚动市盈率(TTM)中位数',
                'averagePETTM': '全A股滚动市盈率(TTM)等权平均',
                'quantileInRecent10YearsMiddlePeTtm': '当前"TTM(滚动市盈率)中位数"在最近10年数据上的分位数',
                'quantileInRecent10YearsAveragePeTtm': '当前"TTM(滚动市盈率)等权平均"在在最近10年数据上的分位数'
            }
            
            available_columns = [col for col in field_mapping.keys() if col in pe_df.columns]
            if available_columns:
                latest_pe_data = pe_df[available_columns].iloc[-1]
                # 重命名字段
                renamed_data = {}
                for col in available_columns:
                    renamed_data[field_mapping[col]] = latest_pe_data[col]
                summary['A股市盈率指标'] = renamed_data
            else:
                summary['A股市盈率指标'] = "指定字段不存在"
    except Exception as e:
        summary['A股市盈率指标'] = f"获取失败: {e}"
    
    return summary

def main():
    """主函数"""
    try:
        bot = IndicatorBot()
        
        # 运行指标播报任务
        bot.run()
        
    except FileNotFoundError:
        logger.error("配置文件 config.json 不存在")
    except json.JSONDecodeError:
        logger.error("配置文件格式错误")
    except Exception as e:
        logger.error(f"程序执行出错: {e}")

if __name__ == "__main__":
    # 可以选择运行方式：
    # 1. 运行钉钉播报任务
    main()
    
    # 2. 或者运行原有的控制台输出（注释掉上面的main()调用）
    # get_stock_indicators()
    # 
    # print("\n" + "=" * 60)
    # print("数据摘要")
    # print("=" * 60)
    # 
    # summary = get_latest_indicators_summary()
    # for key, value in summary.items():
    #     print(f"\n{key}:")
    #     if isinstance(value, dict):
    #         for k, v in value.items():
    #             print(f"  {k}: {v}")
    #     else:
    #         print(f"  {value}")