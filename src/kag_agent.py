from dataclasses import dataclass
import os

from dotenv import load_dotenv
from pydantic_ai import Agent, ModelRetry, RunContext
from pydantic_ai.mcp import MCPServerStreamableHTTP
from pydantic_ai.models.openai import OpenAIModel, OpenAIModelSettings
from pydantic_ai.providers.openai import OpenAIProvider

from graph.kuzu_graph import KuzuGraph

bert_server = MCPServerStreamableHTTP(url='http://localhost:8000/mcp')

load_dotenv()
_model = OpenAIModel('qwen-max', provider=OpenAIProvider(
                  base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
                  api_key=os.environ.get("BAILIAN_API_KEY")
              ))

_settings = OpenAIModelSettings(
    temperature=0.0
)

@dataclass
class SupportDependencies:
    """指标图数据库"""
    graph: KuzuGraph

class MetricTool:
    """
    指标查询工具，用于查询指标信息
    """

    def __init__(self, graph: KuzuGraph):
        self.graph = graph
        self.metrics = []
        self.dimensions = {}
        self.datasources = {}
    
    @property
    def Metrics(self):
        return self.metrics
    
    @property
    def Dimensions(self):
        return list(self.dimensions.values())

    @property
    def DataSources(self):
        return list(self.datasources.values())
    
    def fetch_metric(self, metric_id: str):
        """
        直接通过指标id查询指标信息，包括指标、维度、数据源信息
        Args:
            metric_id: 指标id
        Returns:
            指标信息
        """
        metric = None
        dimensions = []
        datasource = None
        # print(f"metrics: {metric_ids}")
        cypher = f"""
        MATCH (m:Metric)-[:USES_DIMENSION]->(d:Dimension) 
        WHERE m.id = '{metric_id}'
        RETURN m,  collect(d) as dimensions
        """
        result = self.graph.query(cypher)
        if result:
            metric = result[0]['m']
            dimensions = result[0]['dimensions']
        
        cypher = f"""
        MATCH (m:Metric)-[:FROM_TABLE]->(ds:DataSource)
        WHERE m.id = '{metric_id}'
        RETURN ds
        """
        result_ds = self.graph.query(cypher)
        if result_ds:
            datasource = result_ds[0]['ds']

        return (metric, dimensions, datasource)
    
    def fetch_all_metrics(self, metric_ids: list[str]):
        for metric_id in metric_ids:
            metric, dimensions, datasource = self.fetch_metric(metric_id)
            if metric:
                if metric['dependent_metrics']:
                    self.fetch_all_metrics(metric['dependent_metrics'])
                self.metrics.append(metric)
            if dimensions:
                for d in dimensions:
                    self.dimensions[d['id']] = d

            if datasource:
                self.datasources[datasource['table_name']] = datasource

    def query(self, metric_names: list[str], dimension_names: list[str]):
        metrics = ", ".join([f"'{name}'" for name in metric_names])
        dimensions = ", ".join([f"'{name}'" for name in dimension_names])
        
        # 获得能够支持所有维度的指标列表
        cypher = f"""
        MATCH (m:Metric)
        WHERE m.alias IN [{metrics}] 
        AND COUNT {{ 
            MATCH (m)-[:USES_DIMENSION]->(d:Dimension:MetricDimension) 
            WHERE d.name IN [{dimensions}]
        }} = {len(dimension_names)}  
        RETURN m.id, COUNT {{ MATCH (m)-[:USES_DIMENSION]->(d:Dimension:MetricDimension) }} as dimension_count
        """
        try:
            metric_ids = self.graph.query(cypher)
        except Exception as e:
            raise ModelRetry('未找到相关结果，请重新查询。')
        # 筛选出 metric_ids 中 dimension_count 最小的指标
        if metric_ids:
            min_dimension_count = min(metric_ids, key=lambda x: x['dimension_count'])['dimension_count']
            min_available_metric_ids = \
                [metric_id['m.id'] for metric_id in metric_ids if metric_id['dimension_count'] == min_dimension_count]

            self.fetch_all_metrics(min_available_metric_ids)


def make_agent():
    agent = Agent(
        _model,
        deps_type=SupportDependencies,
        model_settings=_settings,
        mcp_servers=[bert_server]
    )

    def get_graph_schema(ctx: RunContext[SupportDependencies]) -> str:
        return f"""企业经营指标分析师。
    ## 1.用detect_dimensions识别出需要的维度列表
    ## 2.查询指标统计所需信息元数据
    根据下面给定的图数据架构利用metric_query工具查询提及的指标(Metric),
        [ 
            {{ 
                'm' : {{ '_label': Metric ... }},
                'd' : {{ '_label': Dimension ... }},
                'ds' : {{ '_label': DataSource ... }}
            }}, 
            ... 
        ]

    ## 3.生成SQL查询语句
    根据获得的指标、维度、维度关联的数据源、数据源信息生成SQL查询语句。
    ** 注意** :
    - 请严格按照获得的信息生成SQL查询语句。
    - 需要替换其中的 {{Metric.DataSource}} 为数据源的名称。
    - 如果Dimension的required标识为true那么条件必须在输出的SQL中体现。
    - "本年累计数"不可以做多月累加。
    ## 输出格式:
    ```sql
    SELECT * FROM table_name WHERE condition;
    ```
    仅输出SQL查询语句,不要包含任何其他信息。
    """

    def metric_query(ctx: RunContext[SupportDependencies], metric_names: list[str], dimension_names: list[str]):
        """
        指标查询
        Args:
            metric_names: 指标名称列表
            dimension_names: 维度名称列表
        Returns:
            指标、维度、数据源信息列表
        """
        print(f"metric_names: {metric_names}")
        print(f"dimensions: {dimension_names}")
        tool = MetricTool(ctx.deps.graph)
        tool.query(metric_names, dimension_names)

        return dict(m=tool.Metrics, 
                    d=tool.Dimensions, 
                    ds=tool.DataSources)


    agent.system_prompt(get_graph_schema)
    agent.tool(metric_query)
    
    return agent