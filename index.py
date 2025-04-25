from typing import Literal
import os
import oracledb
from pydantic import BaseModel, Field
from oracle import OracleSaver
from langchain_core.prompts.prompt import PromptTemplate
from langgraph.graph import StateGraph,MessagesState
from langgraph.constants import START, END

model=None
class WeatherAgent(BaseModel):
    city: Literal["sf", "nyc"] = Field(..., description="City to get weather for")
    response: str = Field(..., description="Weather response")


class State(MessagesState):
    city: any
    response: any

def get_weather(state: State) -> State:
    """Use this to get weather information."""
    if state["city"] == "nyc":
        state["messages"]= ["It might be cloudy in nyc"]
    elif state["city"] == "sf":
        state["messages"]= ["It's always sunny in sf"]
    else:
        raise AssertionError("Unknown city")
    return state


def find_city(state: State) -> State:
    p = PromptTemplate(
        template="Find the city in the message.\n{question}", input_variables=["question"]
    )
    c = p | model.with_structured_output(WeatherAgent)
    result = c.invoke({"question": state["messages"][-1].content})
    state["city"] = result.city
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
        "configurable": {"thread_id": "2", "checkpoint_ns": "oracle"},
        "recursion_limit": 1000,
    }
        q = input()
        res = graph.stream({"messages": [("human", q)]}, config,stream_mode=['updates'])
        for r in res:
            print(r)

