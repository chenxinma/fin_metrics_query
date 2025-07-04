from pathlib import Path

from dotenv import load_dotenv

from graph.kuzu_graph import KuzuGraph
from kag_agent import MetricTool


SCRIPT_DIR = Path(__file__).parent

load_dotenv()
graph = KuzuGraph("./kuzudb")

def test_query01():
    tool = MetricTool(graph)
    tool.query(["营业收入"], ["时间", "地区-所属大区-外服机构", '是否关联方'])
    print(dict(m=tool.Metrics, 
               d=tool.Dimensions, 
               ds=tool.DataSources))
