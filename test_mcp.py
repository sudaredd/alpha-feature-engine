#!/usr/bin/env python3
"""
Interactive test script to query the live running Alpha Feature Engine MCP Server.
Connects to http://localhost:8080/sse and invokes getHistoricalVwap over Model Context Protocol.
"""
import urllib.request
import json
import threading
import time
from datetime import datetime, timezone, timedelta

def main():
    print("==================================================")
    print(" Connecting to Alpha Feature Engine MCP Server... ")
    print("==================================================")

    # 1. Establish SSE Stream Connection
    req = urllib.request.Request("http://localhost:8080/sse", headers={"Accept": "text/event-stream"})
    try:
        sse_resp = urllib.request.urlopen(req)
    except Exception as e:
        print(f"[-] Could not connect to http://localhost:8080/sse: {e}")
        print("    Ensure the application is running on port 8080.")
        return

    # 2. Receive the endpoint event containing the session message URL
    message_endpoint = None
    for _ in range(20):
        line = sse_resp.readline().decode("utf-8")
        if "data:" in line:
            message_endpoint = line.split("data:")[1].strip()
            break

    if not message_endpoint:
        print("[-] Failed to retrieve session endpoint from SSE.")
        return

    message_url = f"http://localhost:8080{message_endpoint}"
    print(f"[+] Connected! Session message endpoint:\n    {message_url}\n")

    # Background thread to receive and display SSE messages
    response_event = threading.Event()
    last_response = {}

    def listen_sse():
        while True:
            try:
                line = sse_resp.readline().decode("utf-8")
                if line.startswith("data:"):
                    payload = line[5:].strip()
                    if payload:
                        data = json.loads(payload)
                        if "result" in data or "error" in data:
                            last_response["data"] = data
                            response_event.set()
            except Exception:
                break

    listener = threading.Thread(target=listen_sse, daemon=True)
    listener.start()

    def send_rpc(payload):
        body = json.dumps(payload).encode("utf-8")
        post_req = urllib.request.Request(
            message_url,
            data=body,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(post_req) as r:
            pass

    # 3. Protocol Handshake: Initialize
    print("[*] Sending 'initialize' handshake...")
    response_event.clear()
    send_rpc({
        "jsonrpc": "2.0",
        "id": "1",
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "1.0.0"}
        }
    })
    response_event.wait(timeout=3)
    if "data" in last_response:
        server_info = last_response["data"].get("result", {}).get("serverInfo", {})
        print(f"[+] Server Info: {server_info.get('name', 'N/A')} (v{server_info.get('version', 'N/A')})")

    # 4. Notify initialized
    send_rpc({
        "jsonrpc": "2.0",
        "method": "notifications/initialized",
        "params": {}
    })
    time.sleep(0.2)

    # 5. List available tools
    print("\n[*] Sending 'tools/list'...")
    response_event.clear()
    send_rpc({
        "jsonrpc": "2.0",
        "id": "2",
        "method": "tools/list",
        "params": {}
    })
    response_event.wait(timeout=3)
    if "data" in last_response:
        tools = last_response["data"].get("result", {}).get("tools", [])
        for tool in tools:
            print(f"[+] Discovered Tool: '{tool.get('name')}' - {tool.get('description')[:70]}...")

    # 6. Call getHistoricalVwap tool
    now = datetime.now(timezone.utc)
    start_time = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = (now + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"\n[*] Invoking tool 'getHistoricalVwap' for PLTR (last 5 minutes, 5-second bars)...")
    response_event.clear()
    send_rpc({
        "jsonrpc": "2.0",
        "id": "3",
        "method": "tools/call",
        "params": {
            "name": "getHistoricalVwap",
            "arguments": {
                "symbol": "PLTR",
                "startTime": start_time,
                "endTime": end_time,
                "resolutionSeconds": 5
            }
        }
    })

    if response_event.wait(timeout=5):
        print("\n==================== MCP TOOL RESPONSE (PLTR) ====================")
        result = last_response["data"].get("result", {})
        print(json.dumps(result, indent=2))
        print("==================================================================")
    else:
        print("[-] Timed out waiting for tool response.")

    print(f"\n[*] Invoking tool 'getHistoricalVwap' for SOLUSDT (live Binance feed via Kafka)...")
    response_event.clear()
    send_rpc({
        "jsonrpc": "2.0",
        "id": "4",
        "method": "tools/call",
        "params": {
            "name": "getHistoricalVwap",
            "arguments": {
                "symbol": "SOLUSDT",
                "startTime": start_time,
                "endTime": end_time,
                "resolutionSeconds": 5
            }
        }
    })

    if response_event.wait(timeout=5):
        print("\n==================== MCP TOOL RESPONSE (SOLUSDT) ====================")
        result = last_response["data"].get("result", {})
        print(json.dumps(result, indent=2))
        print("=====================================================================")
    else:
        print("[-] Timed out waiting for tool response.")

if __name__ == "__main__":
    main()
