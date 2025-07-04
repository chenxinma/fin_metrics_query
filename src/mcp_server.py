"""MCP Server"""
import os
import sys
from pathlib import Path

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict

from mcp import types
from mcp.server.lowlevel import Server

from mcp.server.sse import SseServerTransport 
from starlette.applications import Starlette 
from starlette.routing import Mount, Route
from graph.kuzu_graph import KuzuGraph
from util import do_query

MCP_DIR = Path(__file__).parent.parent
sys.path.append(str(MCP_DIR))

class MCPRetry(Exception):
    """Retry exception"""

@asynccontextmanager
async def app_lifespan(_server: Server) -> AsyncIterator[Dict[str, Any]]:
    """Manage application lifecycle with type-safe context"""
    # 资源初始化
    print("kuzu:", str((MCP_DIR / "./kuzudb").absolute()))
    graph = KuzuGraph(str((MCP_DIR / "./kuzudb").absolute()))

   
    yield {
        'graph': graph
    }

# Pass lifespan to server
sse = SseServerTransport("/messages/")
server = Server("data_governance", lifespan=app_lifespan)

def _wrap_cypher(cypher: str) -> str:
    c = cypher.replace("\\n", "\n")
    if c.endswith(";"):
        c = c[:-1]
    return c

@server.list_tools()
async def list_tools() -> list[types.Tool]:
    """list tools"""
    _graph = server.request_context.lifespan_context["graph"]
    return [
        types.Tool(
            name="metric_metadata_query",
            description=
f""""### 指标统计所需信息元数据
你可以根据下面给定的图数据架构利用cypher_query工具查询所需的指标(Metric),
以获得指标和指标相关的其他信息:
    [ 
        {{ 
            'm' : {{ '_label': Metric }},
            'd' : {{ '_label': Dimension }},
            'ds' : {{ '_label': DataSource }}
        }}, 
        ... 
    ]
** 注意：** 
- 获取所有相关的 Dimension 和 DataSource，不要做过滤。
- 如果Dimension的required标识为true那么条件必须在输出的SQL中体现。

{_graph.schema}
""",
            inputSchema={
                "type": "object",
                "properties": {
                    "cypher": {"type": "string", "description": "Cypher query"},
                },
                "required": ["cypher"]
            }
        ),
        types.Tool(
            name="sql_query",
            description="""
            ### 执行SQL查询
执行此前根据 metric_metadata_query 获得的指标、维度、维度关联的数据源、数据源信息生成SQL查询语句。
""",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query"},
                },
                "required": ["sql"]
            }
        )
    ]

@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent | types.EmbeddedResource]:
    """Call tool"""
    if name == "metric_metadata_query":
        resp = metric_metadata_query(arguments["cypher"])
        return [types.TextContent(type="text", text=str(resp))]
    elif name == "sql_query":
        resp = sql_query(arguments["sql"])
        return [types.TextContent(type="text", text=str(resp))]

    raise MCPRetry(f"Unknown tool name: {name}")



def metric_metadata_query(cypher: str):
    """Do cypher query       
    Args:
        query: cypher from agent to execute
    """
    _graph = server.request_context.lifespan_context["graph"]
    # vaildate cypher
    if not cypher.upper().startswith('MATCH'):
        raise MCPRetry('请编写一个MATCH的查询。')

    try:
        _wraped_cypher = _wrap_cypher(cypher)
        return _graph.query(_wraped_cypher)
    except Exception as e:
        raise e

def sql_query(sql: str):
    """Do sql query
    Args:
        query: sql from agent to execute
    """

    # vaildate sql
    if not sql.upper().startswith('SELECT'):
        raise MCPRetry('请编写一个SELECT的查询。')

    try:
        df = do_query(sql)
        return df.to_dict(orient='records')
    except Exception as e:
        raise e


async def handle_sse(request):
    # 定义异步函数handle_sse，处理SSE请求
    # 参数: request - HTTP请求对象
    
    async with sse.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        # 建立SSE连接，获取输入输出流
        await server.run(
            streams[0], streams[1], server.create_initialization_options()
        )  # 运行MCP应用，处理SSE连接

starlette_app = Starlette(
    debug=True,  # 启用调试模式
    routes=[
        Route("/sse", endpoint=handle_sse),  # 设置/sse路由，处理函数为handle_sse
        Mount("/messages/", app=sse.handle_post_message),  # 挂载/messages/路径，处理POST消息
    ],
)  # 创建Starlette应用实例，配置路由

if __name__ == "__main__":
    import uvicorn  # 导入uvicorn ASGI服务器
    mcp_host = os.environ.get("MCP_HOST", "127.0.0.1")
    mcp_port = int(os.environ.get("MCP_PORT", "8001"))
    uvicorn.run(starlette_app, host=mcp_host, port=mcp_port)  # 运行Starlette应用，监听默认127.0.0.1和指定端口8001
