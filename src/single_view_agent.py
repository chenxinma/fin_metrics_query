import os

from dotenv import load_dotenv
from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIModel, OpenAIModelSettings
from pydantic_ai.providers.openai import OpenAIProvider
import pandas as pd

load_dotenv()
_model = OpenAIModel('qwen3-1.7b', provider=OpenAIProvider(
                  base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
                  api_key=os.environ.get("BAILIAN_API_KEY")
              ))

_settings = OpenAIModelSettings(
    temperature=0.0,
    extra_body={
        "enable_thinking": False,
    }
)

def show_df_info(df: pd.DataFrame):
    info = []
    for c, dtype in zip(df.columns, df.dtypes):
        info.append(f"\"{c}\": {dtype}")
    return "\n".join(info)

def make_agent(df: pd.DataFrame):
    agent = Agent(
        _model,
        deps_type=pd.DataFrame,
        model_settings=_settings,
    )

    def get_graph_schema(ctx: RunContext[pd.DataFrame]) -> str:
        return f"""
        你是一个数据分析师，能够熟练的使用sql完成分析，你需要根据提供的表格数据来回答问题。
            这对 ** df ** 表生成SQL
            ## 约束：
            - 你需要根据问题和df的字段定义生成SQL查询语句。
            - 严格按照df的字段定义生成SQL。
            - 不要生成任何其他信息。
            - 取数类型 默认使用 '1'本期发生数
            - 取数类型 '2'本年累计数 不可以做多月累加
            - 字段名称加半角双引号
            
        df的字段定义如下：
            {show_df_info(ctx.deps)}
        
        + 财务期间格式为YYYYMM的字符串
        + 地区：区域、海外、并购、上海地区
        + 所属大区：北方中心、中西部中心、南方中心、长三角大区
        """

    agent.system_prompt(get_graph_schema)

    return agent