#!/usr/bin/env python3
"""
Alpha Agent: LangGraph ReAct Orchestrator over Model Context Protocol (MCP).
Connects directly to the Spring Boot Alpha Feature Engine via SSE and utilizes Google Gemini
to analyze real-time quantitative market data, evaluate trading signals, and autonomously execute
orders via the MCP submitOrder tool.
"""

import asyncio
import functools
import json
import os
import sys
from typing import Annotated, TypedDict

print = functools.partial(print, flush=True)

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from mcp import ClientSession
from mcp.client.sse import sse_client


# ---------------------------------------------------------------------------
# System Prompt: Autonomous Quantitative Reasoning & Execution Rules
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are an autonomous quantitative trading agent operating on real-time market data.
You have access to tools for fetching historical quantitative features and executing live trade orders:
1. `get_historical_vwap`: Retrieves historical VWAP and OHLCV time bars downsampled from the market feed.
2. `submit_order`: Submits a live trade order to the execution engine.

Operational Rules & Protocol:
1. First, retrieve the requested market data using `get_historical_vwap`.
2. Analyze the price action relative to VWAP, calculate price momentum, and assess the market signal:
   - BULLISH: Price is trading consistently above VWAP or demonstrating an upward breakout with volume support.
   - BEARISH: Price is trading consistently below VWAP or demonstrating downward breakdown momentum.
   - NEUTRAL: Price oscillates tightly around VWAP with no clear directional trend or low conviction.
3. Order Execution Decision Rules:
   - If BULLISH: You MUST actively execute a trade by calling the `submit_order` tool with side="BUY", quantity=100, and your calculated confidence score (0-100).
   - If BEARISH: You MUST actively execute a trade by calling the `submit_order` tool with side="SELL", quantity=100, and your calculated confidence score (0-100).
   - If NEUTRAL: Do NOT submit an order. Simply return a comprehensive summary of the market conditions and your reasoning.
4. After receiving the tool execution result (e.g. order confirmation with status 'FILLED'), provide a final summary detailing:
   - Symbol analyzed and time range
   - Market observation (Price vs VWAP, trend analysis)
   - Quantitative signal (BULLISH / BEARISH / NEUTRAL) with confidence score
   - Order execution outcome (order ID, side, quantity, status, timestamp) if executed.
"""


# ---------------------------------------------------------------------------
# LangGraph State Schema
# ---------------------------------------------------------------------------
class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]


# ---------------------------------------------------------------------------
# Main Orchestrator Execution
# ---------------------------------------------------------------------------
async def run_agent(
    query: str = (
        "Analyze the price action and VWAP for PLTR over the last 5 minutes at a 15-second resolution. "
        "If you detect a bullish or bearish signal, execute the trade."
    ),
    mcp_url: str = "http://localhost:8080/sse",
):
    print("=" * 70, flush=True)
    print(" 🚀 Alpha Agent: LangGraph ReAct + Spring Boot MCP Client", flush=True)
    print("=" * 70, flush=True)
    print(f"[*] Target MCP Server: {mcp_url}", flush=True)
    print(f"[*] Query: \"{query}\"\n", flush=True)

    # Step 1: Connect to the Spring Boot MCP Server over SSE
    print("[1/5] Connecting to Spring Boot MCP Server via SSE...", flush=True)
    async with sse_client(mcp_url) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            mcp_tools_list = await session.list_tools()
            available_tool_names = [t.name for t in mcp_tools_list.tools]
            print(f"  [+] Connected! Available MCP Tools: {available_tool_names}")

            # Helper to parse natural time or ISO strings to RFC-3339/ISO-8601 UTC
            def to_iso_utc(val: str, default_minutes_ago: int = 0) -> str:
                from datetime import datetime, timezone, timedelta
                v = (val or "").strip().lower()
                now = datetime.now(timezone.utc)
                if not v or v == "now":
                    return now.strftime("%Y-%m-%dT%H:%M:%SZ")
                if "now-" in v or v.startswith("-"):
                    cleaned = v.replace("now", "").replace("-", "").replace("m", "").strip()
                    try:
                        mins = int(cleaned)
                    except ValueError:
                        mins = default_minutes_ago
                    return (now - timedelta(minutes=mins)).strftime("%Y-%m-%dT%H:%M:%SZ")
                return val

            # Step 2: Python Tool Wrappers executing session.call_tool directly against Spring Boot MCP
            async def get_historical_vwap(
                symbol: str, startTime: str, endTime: str, resolutionSeconds: int = 60
            ) -> str:
                """Retrieves historical Volume-Weighted Average Price (VWAP) and OHLCV time bars downsampled from the market engine.

                Args:
                    symbol: Stock ticker symbol (e.g., 'PLTR')
                    startTime: Query start time in ISO-8601 format (e.g., '2026-09-05T23:26:00Z' or 'now-5m')
                    endTime: Query end time in ISO-8601 format (e.g., '2026-09-05T23:32:00Z' or 'now')
                    resolutionSeconds: Bar aggregation resolution in seconds (default 60)
                """
                start_iso = to_iso_utc(startTime, default_minutes_ago=5)
                end_iso = to_iso_utc(endTime, default_minutes_ago=0)

                print(
                    f"\n  [🔧 Tool Call] Executing getHistoricalVwap via MCP:\n"
                    f"     symbol={symbol}, range=[{start_iso} -> {end_iso}], res={resolutionSeconds}s"
                )
                try:
                    result = await session.call_tool(
                        "getHistoricalVwap",
                        arguments={
                            "symbol": symbol,
                            "startTime": start_iso,
                            "endTime": end_iso,
                            "resolutionSeconds": resolutionSeconds,
                        },
                    )
                    content = result.content[0].text if result.content else "{}"
                    return content
                except Exception as e:
                    print(f"     [-] MCP getHistoricalVwap error: {e}")
                    return json.dumps({"error": str(e), "bars": []})

            async def submit_order(
                symbol: str, side: str, quantity: int, confidenceScore: int
            ) -> str:
                """Submits a live trade order to the execution engine.

                Args:
                    symbol: The stock ticker symbol (e.g., 'PLTR')
                    side: Order side, either 'BUY' or 'SELL'
                    quantity: Number of shares to trade (e.g., 100)
                    confidenceScore: Quantitative model confidence score between 0 and 100
                """
                side_clean = side.upper().strip()
                print(
                    f"\n  [⚡ Tool Call] Executing submitOrder via MCP:\n"
                    f"     symbol={symbol}, side={side_clean}, quantity={quantity}, confidenceScore={confidenceScore}"
                )
                try:
                    result = await session.call_tool(
                        "submitOrder",
                        arguments={
                            "symbol": symbol,
                            "side": side_clean,
                            "quantity": int(quantity),
                            "confidenceScore": int(confidenceScore),
                        },
                    )
                    content = result.content[0].text if result.content else "{}"
                    return content
                except Exception as e:
                    print(f"     [-] MCP submitOrder error: {e}")
                    return json.dumps({"error": str(e), "status": "FAILED"})

            # Step 3: Initialize Google Gemini LLM and Bind Tools
            api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")

            if api_key:
                from langchain_google_genai import ChatGoogleGenerativeAI

                print("  [+] Initializing ChatGoogleGenerativeAI (model='gemini-2.5-flash', temp=0)...")
                llm = ChatGoogleGenerativeAI(
                    model="gemini-2.5-flash",
                    temperature=0,
                    api_key=api_key,
                    max_retries=3,
                )
                llm_with_tools = llm.bind_tools([get_historical_vwap, submit_order])
            else:
                print("  [!] Error: Neither GOOGLE_API_KEY nor GEMINI_API_KEY found in environment.")
                print("      Please set GOOGLE_API_KEY to query Google Gemini.")
                return

            # Step 4: Build the LangGraph StateGraph (ReAct Architecture)
            print("[2/5] Building LangGraph StateGraph (ReAct Architecture)...")

            async def reasoning_node(state: AgentState) -> dict:
                import re

                print("\n  [🧠 Reasoning Node] Invoking Gemini LLM...")
                messages = state["messages"]
                for attempt in range(5):
                    try:
                        ai_message = await llm_with_tools.ainvoke(messages)
                        return {"messages": [ai_message]}
                    except Exception as e:
                        err_str = str(e)
                        if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                            wait_sec = 15
                            match = re.search(r"retry in (\d+(?:\.\d+)?)s", err_str, re.IGNORECASE)
                            if match:
                                wait_sec = max(5, int(float(match.group(1))) + 2)
                            print(
                                f"  [⏳ Quota Throttled] Free-tier rate limit reached. "
                                f"Backing off for {wait_sec}s (attempt {attempt + 1}/5)..."
                            )
                            await asyncio.sleep(wait_sec)
                        else:
                            raise
                raise RuntimeError("Failed to invoke Gemini LLM after retries due to quota limits.")

            async def tool_execution_node(state: AgentState) -> dict:
                print("  [⚙️  Tool Execution Node] Processing tool calls requested by LLM...")
                last_message = state["messages"][-1]
                tool_messages = []

                if hasattr(last_message, "tool_calls") and last_message.tool_calls:
                    for call in last_message.tool_calls:
                        call_id = call["id"]
                        tool_name = call["name"]
                        args = call["args"]
                        print(f"     ↳ Routing execution for tool: '{tool_name}' with args: {args}")

                        if tool_name in ["get_historical_vwap", "getHistoricalVwap"]:
                            tool_result_str = await get_historical_vwap(**args)
                            tool_messages.append(
                                ToolMessage(content=tool_result_str, tool_call_id=call_id, name=tool_name)
                            )
                        elif tool_name in ["submit_order", "submitOrder"]:
                            # Handle parameter key variants if model returns snake_case
                            if "confidence_score" in args and "confidenceScore" not in args:
                                args["confidenceScore"] = args.pop("confidence_score")
                            tool_result_str = await submit_order(**args)
                            tool_messages.append(
                                ToolMessage(content=tool_result_str, tool_call_id=call_id, name=tool_name)
                            )
                        else:
                            print(f"     [-] Warning: Unknown tool '{tool_name}' requested")
                            tool_messages.append(
                                ToolMessage(
                                    content=f"Error: Unknown tool '{tool_name}'",
                                    tool_call_id=call_id,
                                    name=tool_name,
                                )
                            )
                return {"messages": tool_messages}

            def should_continue(state: AgentState) -> str:
                last_message = state["messages"][-1]
                if hasattr(last_message, "tool_calls") and last_message.tool_calls:
                    print("     ↳ Routing: LLM requested tool call -> routing to 'tool_execution'")
                    return "tool_execution"
                print("     ↳ Routing: LLM completed reasoning -> routing to END")
                return END

            workflow = StateGraph(AgentState)
            workflow.add_node("reasoning", reasoning_node)
            workflow.add_node("tool_execution", tool_execution_node)

            workflow.set_entry_point("reasoning")
            workflow.add_conditional_edges(
                "reasoning",
                should_continue,
                {"tool_execution": "tool_execution", END: END},
            )
            workflow.add_edge("tool_execution", "reasoning")

            app = workflow.compile()

            # Step 5: Execute Query
            print("[3/5] Starting LangGraph ReAct Agent Loop...")
            initial_state = {
                "messages": [
                    SystemMessage(content=SYSTEM_PROMPT),
                    HumanMessage(content=query),
                ]
            }

            async for event in app.astream(initial_state, stream_mode="updates"):
                for node_name, node_update in event.items():
                    msg = node_update["messages"][-1]
                    if isinstance(msg, AIMessage) and msg.tool_calls:
                        print(f"\n[AI Decision] LLM requested {len(msg.tool_calls)} tool call(s):")
                        for tc in msg.tool_calls:
                            print(f"   • Tool: {tc['name']} with args: {tc['args']}")
                    elif isinstance(msg, ToolMessage):
                        preview = msg.content[:250] + "..." if len(msg.content) > 250 else msg.content
                        print(f"\n[Tool Output] Received live response from MCP for '{msg.name}':\n   {preview}")
                    elif isinstance(msg, AIMessage):
                        print("\n" + "=" * 70)
                        print(" 🏁 FINAL AGENT RESPONSE")
                        print("=" * 70)
                        content_text = ""
                        if isinstance(msg.content, list):
                            for part in msg.content:
                                if isinstance(part, dict) and "text" in part:
                                    content_text += part["text"]
                                elif isinstance(part, str):
                                    content_text += part
                        else:
                            content_text = str(msg.content)
                        print(content_text.strip())
                        print("=" * 70 + "\n")


if __name__ == "__main__":
    custom_query = sys.argv[1] if len(sys.argv) > 1 else (
        "Analyze the price action and VWAP for PLTR over the last 5 minutes at a 15-second resolution. "
        "If you detect a bullish or bearish signal, execute the trade."
    )
    asyncio.run(run_agent(query=custom_query))
