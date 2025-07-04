# 财务指标查询

## 项目概述
本项目是一个指标分析LLM生成工具，提供命令行问答界面和MCP服务接口。

## 项目结构
```plaintext
fin_metrics_query/
├── src/                  # 源代码目录
│   ├── main.py           # 主程序入口
│   ├── mcp_server.py     # MCP服务入口
│   ├── graph/            # 新增图处理相关文件
│   │   └── kuzu_graph.py
│   └── make_graph/       # 指标模型构建
│       ├── __init__.py
│       └── metric_model.py
├── lib/                  # 库文件
├── reference/            # 参考资料（组织机构树、问题数据集、测试数据parquet等）
├── .gitignore            # Git忽略文件
├── .python-version       # Python版本文件
├── pyproject.toml        # Python项目配置
├── uv.lock               # 锁文件
└── README.md             # 项目说明文档
```

## 程序入口说明
### `src/main.py`
项目的主要入口，提供一个面向命令行的指标问答界面。

### `src/mcp_server.py`
MCP 服务器的入口，提供SSE的MCP服务。

### `src/make_graph/metric_model.py`
指标定义模型的构建，用于初始化kuzudb的数据。

## 安装与运行
1. 安装依赖：
```bash
uv sync
```
2. 运行命令行问答界面：
```bash
uv run src/main.py
```
3. 运行MCP服务：
```bash
uv run src/mcp_server.py
```

## 环境变量配置
在项目根目录创建 `.env` 文件，配置以下环境变量：
```ini
BAILIAN_API_KEY=sk-...
```
