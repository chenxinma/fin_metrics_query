import asyncio

import pandas as pd
from rich.console import Console
from rich.live import Live
from rich.markdown import Markdown
from rich.table import Table

from graph.kuzu_graph import KuzuGraph
from kag_agent import SupportDependencies, make_agent
from util import do_query, prettier_code_blocks, wrap_sql

async def main():
    graph = KuzuGraph("./kuzudb")
    agent = make_agent()
    # prettier_code_blocks()
    console = Console()
    async with agent.run_mcp_servers():
        console.log("Agent ready.")
        console.log(f'问题示例: 2025年3月营业收入最高的外服机构是哪家？', style='cyan')
        while True:
            prompt = input("请输入问题（输入 '\\q' 退出）: ")
            if prompt == '\\q':
                break
            result = await agent.run(prompt, deps=SupportDependencies(graph=graph))
            sql = result.output
            console.log(Markdown(sql))
            data = do_query(wrap_sql(sql))
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
