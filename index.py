from typing import Literal
import os
import oracledb
from pydantic import BaseModel, Field
from oracle import OracleSaver
from langchain_core.prompts.prompt import PromptTemplate
from langgraph.graph import StateGraph,MessagesState
from langgraph.constants import START, END
from langchain_core.messages import AIMessage, HumanMessage



class WeatherAgent(BaseModel):
    city: Literal["sf", "nyc"] = Field(..., description="City to get weather for")
    response: str = Field(..., description="Weather response")


class State(MessagesState):
    city: any
    response: any

def get_weather(state: State) -> State:
    """Use this to get weather information."""
    if state["city"] == "nyc":
        return {"messages": [AIMessage("It's always sunny in nyc")]}
    elif state["city"] == "sf":
        return {"messages": [AIMessage("It's always sunny in sf")]}
    else:
        raise AssertionError("Unknown city")


def find_city(state: State) -> State:
    state["city"] = 'sf'
    return state


workflow = StateGraph(State)
workflow.add_node(get_weather)
workflow.add_node(find_city)

workflow.add_edge(START, "find_city")
workflow.add_edge("find_city", "get_weather")
workflow.add_edge("get_weather", END)

oracle_config = {
    "host": "localhost",
    "port": 1521,
    "service_name": "FREE",
    "user": "system",
    "password": "root",
}
with oracledb.connect(
    user=oracle_config["user"],
    password=oracle_config["password"],
    dsn=f"{oracle_config['host']}:{oracle_config['port']}/{oracle_config['service_name']}",
) as conn:
    conn.autocommit = True
    checkpointer = OracleSaver(conn)
    checkpointer.setup()
    
    graph = workflow.compile(checkpointer=checkpointer)
    while True:
        config = {
        "configurable": {"thread_id": "100"},
        "recursion_limit": 1000,
    }
        q = "Weather in SF"#input()
        res = graph.invoke({"messages": [HumanMessage(q)]}, config)
        print(res["messages"][-1].content)
        r = list(checkpointer.list(config))
        print(r)
