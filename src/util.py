from pathlib import Path

import duckdb
from rich.console import Console, ConsoleOptions, RenderResult
from rich.markdown import CodeBlock, Markdown
from rich.syntax import Syntax
from rich.text import Text
import pandas as pd

REF = Path(__file__).parent.parent / 'reference'
ref_abs = REF.absolute()

df_companies = \
    duckdb.query(f"SELECT * FROM read_parquet('{ref_abs}/companies.parquet')").df()
df_dm_finance_mon_balance_sheet_manual_slice = \
    duckdb.query(f"SELECT * FROM read_parquet('{ref_abs}/dm_finance_mon_balance_sheet_manual_slice.parquet')").df()
df_dm_incm_cost_dtl_rpt = \
    duckdb.query(f"SELECT * FROM read_parquet('{ref_abs}/dm_incm_cost_dtl_rpt.parquet')").df()

def wrap_sql(sql: str):
    return sql.replace('```sql', '').replace('```', '')

def do_query(sql: str, df: pd.DataFrame | None = None):
    return duckdb.query(sql).df()

def prettier_code_blocks():
    """Make rich code blocks prettier and easier to copy.

    From https://github.com/samuelcolvin/aicli/blob/v0.8.0/samuelcolvin_aicli.py#L22
    """

    class SimpleCodeBlock(CodeBlock):
        def __rich_console__(
            self, console: Console, options: ConsoleOptions
        ) -> RenderResult:
            code = str(self.text).rstrip()
            yield Text(self.lexer_name, style='dim')
            yield Syntax(
                code,
                self.lexer_name,
                theme=self.theme,
                background_color='default',
                word_wrap=True,
            )
            yield Text(f'/{self.lexer_name}', style='dim')

    Markdown.elements['fence'] = SimpleCodeBlock
