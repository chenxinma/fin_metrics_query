import json
import asyncio
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd
from pydantic_ai.usage import UsageLimits

from graph.kuzu_graph import KuzuGraph
from kag_agent import SupportDependencies, make_agent
from tqdm import tqdm

SCRIPT_DIR = Path(__file__).parent

load_dotenv()
graph = KuzuGraph("./kuzudb")
agent = make_agent()
doc = []

agent_limits = UsageLimits(request_limit=3)

async def run(questions: list[dict[str, str]]):
    for question in questions:
        prompt = question['question']
        print("[Question]", prompt)
        try:
            result = await agent.run(prompt, 
                                     deps=SupportDependencies(graph=graph), 
                                     usage_limits=agent_limits)
            doc.append({
                'question': prompt,
                'sql': result.output,
                'error': ''
            })
            print("[SQL]", result.output)
        except Exception as e:
            doc.append({
                'question': prompt,
                'sql': 'error',
                'error': str(e)
            })
    pd.DataFrame(doc).to_json(SCRIPT_DIR / '../reference' / 'question_with_sql.json', \
        orient='records', force_ascii=False)

def test_make_sql():
    with open('./reference/question.json', 'r', encoding='utf-8') as f:
        questions = json.load(f)

    asyncio.run(run(questions))
