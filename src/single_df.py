import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
import duckdb
import pandas as pd
from pydantic_ai.models.openai import OpenAIModel, OpenAIModelSettings
from pydantic_ai.providers.openai import OpenAIProvider
from rich.console import Console
from rich.live import Live
from rich.markdown import Markdown
from rich.table import Table

from single_view_agent import make_agent
from util import (
    df_companies,
    df_dm_incm_cost_dtl_rpt,
    do_query,
    prettier_code_blocks,
    wrap_sql,
)

SRC_DIR = Path(__file__).parent.parent

load_dotenv()
_model = OpenAIModel('qwen-max', provider=OpenAIProvider(
                  base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
                  api_key=os.environ.get("BAILIAN_API_KEY")
              ))
_settings = OpenAIModelSettings(
    temperature=0.0
)

async def main():
    with open(SRC_DIR / "reference/income_cost.sql", "r", encoding="utf-8") as f:
        base_sql = f.read()
    df = duckdb.query(base_sql).df()
    agent = make_agent(df)

    prettier_code_blocks()
    console = Console()
    console.log(f'问题示例: 2025年3月营业收入最高的外服机构是哪家？', style='cyan')
    
    while True:
        prompt = input("请输入问题（输入 '\\q' 退出）: ")
        if prompt == '\\q':
            break
        # console.log(f'问题: {prompt}...', style='cyan')
        result = await agent.run(prompt, deps=df)
        sql = result.output
        console.log(Markdown(sql))
        data = do_query(wrap_sql(sql), df)
        if isinstance(data, pd.DataFrame):
            data = data.round(2)            
            with Live('', console=console, vertical_overflow='visible') as live:
                table = Table(
                    title='Result',
                    width=120,
                )
                for c, dtype in zip(data.columns, data.dtypes):
                    _justify = "left"
                    if dtype == 'float64' or dtype == 'int64':
                        _justify = "right"
                    table.add_column(c, justify=_justify)
                for _, row in data.iterrows():
                    table.add_row(*[str(v) for v in row.values])
                live.update(table)
            # console.log(result.usage())
        else:
            console.log(data, style='green')

if __name__ == "__main__":
    asyncio.run(main())
