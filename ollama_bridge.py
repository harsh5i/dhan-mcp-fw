"""
Ollama ↔ MCP Bridge for dhan-nifty-mcp.

Connects any Ollama model to the dhan-nifty MCP server. Supports both:
  1. Native tool calling (models that support it: qwen3, llama3.1, mistral, etc.)
  2. Text-based tool calling (any model — parses TOOL_CALL from output)

Usage:
  python ollama_bridge.py --model gemma4:e2b
  python ollama_bridge.py --model qwen3:8b
  python ollama_bridge.py --model gemma4:e2b --no-native-tools
"""

import asyncio
import argparse
import json
import re
import sys
import os
from contextlib import AsyncExitStack

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import ollama


# ── MCP type → JSON Schema type mapping ─────────────────

TYPE_MAP = {
    "string": "string",
    "str": "string",
    "int": "integer",
    "integer": "integer",
    "float": "number",
    "number": "number",
    "bool": "boolean",
    "boolean": "boolean",
}


def mcp_tool_to_ollama(tool) -> dict:
    """Convert an MCP tool definition to Ollama's tool format."""
    properties = {}
    required = []

    if tool.inputSchema and tool.inputSchema.get("properties"):
        for name, prop in tool.inputSchema["properties"].items():
            prop_type = prop.get("type", "string")
            # Handle anyOf types (e.g. string | null)
            if prop_type == "object" or isinstance(prop_type, list):
                prop_type = "string"
            if "anyOf" in prop:
                for opt in prop["anyOf"]:
                    if opt.get("type") and opt["type"] != "null":
                        prop_type = opt["type"]
                        break
            properties[name] = {
                "type": TYPE_MAP.get(prop_type, "string"),
                "description": prop.get("description", prop.get("title", "")),
            }
            if prop.get("enum"):
                properties[name]["enum"] = prop["enum"]

        required = tool.inputSchema.get("required", [])

    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description or "",
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required,
            },
        },
    }


def build_tool_descriptions(mcp_tools) -> str:
    """Build a text description of tools for the system prompt."""
    lines = []
    for t in mcp_tools:
        desc = (t.description or "").strip().split("\n")[0]  # First line only
        params = []
        if t.inputSchema and t.inputSchema.get("properties"):
            required = set(t.inputSchema.get("required", []))
            for name, prop in t.inputSchema["properties"].items():
                req = " (required)" if name in required else ""
                params.append(f"{name}{req}")
        param_str = f"({', '.join(params)})" if params else "()"
        lines.append(f"  - {t.name}{param_str}: {desc}")
    return "\n".join(lines)


SYSTEM_PROMPT_TEMPLATE = """You are a Nifty/BankNifty options trading assistant connected to a live Dhan broker account.

You have access to {tool_count} tools. USE THEM. Do not say "I don't have access" — you DO have access.

## Key IDs
- NIFTY spot: security_id="13", exchange_segment="INDEX"
- BANKNIFTY spot: security_id="25", exchange_segment="INDEX"
- Lot sizes: NIFTY=75, BANKNIFTY=30

## How to call tools
To call a tool, write EXACTLY this format (no other text on those lines):

TOOL_CALL: tool_name
ARGS: {{"param1": "value1", "param2": "value2"}}

Example — get NIFTY spot price:
TOOL_CALL: get_ltp
ARGS: {{"security_id": "13", "exchange_segment": "INDEX"}}

Example — check server health:
TOOL_CALL: server_status
ARGS: {{}}

After you write a TOOL_CALL, I will execute it and show you the result. Then continue your response.

## Available tools
{tool_list}

## Onboarding
First message of session: "We have gained Dhan trade capability. What do you wanna do?"
When user says "DhanWin": present menu — 1.Learn 2.Reconcile 3.Monitor 4.New strategy 5.Backtest

## Rules
- ALWAYS use tools to get data. Never guess prices or positions.
- Call server_status first in a session to check token and mode.
- If in dry-run mode, say so clearly.
- Be explicit about risks.
"""


def parse_tool_calls(text: str) -> list:
    """Parse TOOL_CALL/ARGS patterns from model output."""
    calls = []
    # Match TOOL_CALL: name followed by ARGS: {...}
    pattern = r'TOOL_CALL:\s*(\w+)\s*\nARGS:\s*(\{[^}]*\})'
    matches = re.finditer(pattern, text, re.MULTILINE)
    for m in matches:
        tool_name = m.group(1).strip()
        try:
            args = json.loads(m.group(2))
        except json.JSONDecodeError:
            args = {}
        calls.append({"name": tool_name, "args": args, "match": m})
    return calls


async def run_bridge(model: str, use_native_tools: bool = True):
    """Main bridge loop."""
    server_path = os.path.join(os.path.dirname(__file__), "server.py")
    venv_python = os.path.join(os.path.dirname(__file__), ".venv", "bin", "python3")

    python_cmd = venv_python if os.path.exists(venv_python) else sys.executable

    server_params = StdioServerParameters(
        command=python_cmd,
        args=[server_path],
    )

    print(f"[bridge] Model: {model}")
    print(f"[bridge] Tool mode: {'native' if use_native_tools else 'text-based'}")
    print(f"[bridge] Connecting to dhan-nifty MCP server...")

    async with AsyncExitStack() as stack:
        read_stream, write_stream = await stack.enter_async_context(
            stdio_client(server_params)
        )
        session = await stack.enter_async_context(
            ClientSession(read_stream, write_stream)
        )
        await session.initialize()

        # Discover tools
        tools_result = await session.list_tools()
        mcp_tools = tools_result.tools
        tool_names = {t.name for t in mcp_tools}
        ollama_tools = [mcp_tool_to_ollama(t) for t in mcp_tools] if use_native_tools else None

        # Build system prompt with tool descriptions
        tool_list_text = build_tool_descriptions(mcp_tools)
        system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
            tool_count=len(mcp_tools),
            tool_list=tool_list_text,
        )

        print(f"[bridge] Connected. {len(mcp_tools)} tools available:")
        for t in mcp_tools:
            print(f"  - {t.name}")
        print()
        print("Type your message (or 'quit' to exit):")
        print("-" * 50)

        messages = [{"role": "system", "content": system_prompt}]

        while True:
            try:
                user_input = input("\nyou> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\n[bridge] Bye.")
                break

            if not user_input:
                continue
            if user_input.lower() in ("quit", "exit", "q"):
                print("[bridge] Bye.")
                break

            messages.append({"role": "user", "content": user_input})

            # Chat loop — keep going until no more tool calls
            max_rounds = 10  # Prevent infinite loops
            for _ in range(max_rounds):
                chat_kwargs = {"model": model, "messages": messages}
                if ollama_tools:
                    chat_kwargs["tools"] = ollama_tools

                response = ollama.chat(**chat_kwargs)
                msg = response.message
                content = msg.content or ""

                # Check for native tool calls first
                native_calls = msg.tool_calls if hasattr(msg, "tool_calls") and msg.tool_calls else []

                if native_calls:
                    messages.append(msg)
                    for tool_call in native_calls:
                        fn_name = tool_call.function.name
                        fn_args = tool_call.function.arguments or {}
                        await _execute_tool(session, fn_name, fn_args, messages, tool_names)
                    continue

                # Check for text-based tool calls
                text_calls = parse_tool_calls(content)

                if text_calls:
                    # Print the text before the first tool call
                    first_match = text_calls[0]["match"]
                    pre_text = content[:first_match.start()].strip()
                    if pre_text:
                        print(f"\nassistant> {pre_text}")

                    messages.append({"role": "assistant", "content": content})

                    for tc in text_calls:
                        result_text = await _execute_tool(
                            session, tc["name"], tc["args"], messages, tool_names
                        )

                    # Let the model continue with the tool results
                    continue

                # No tool calls — just print and break
                print(f"\nassistant> {content}")
                messages.append({"role": "assistant", "content": content})
                break


async def _execute_tool(session, fn_name: str, fn_args: dict, messages: list,
                        valid_tools: set) -> str:
    """Execute a tool call and append result to messages."""
    if fn_name not in valid_tools:
        error = f"Unknown tool: {fn_name}"
        print(f"\n  [tool] {fn_name} — ERROR: {error}")
        messages.append({"role": "user", "content": f"Tool error: {error}"})
        return error

    print(f"\n  [tool] {fn_name}({json.dumps(fn_args, default=str)})")

    try:
        result = await session.call_tool(fn_name, fn_args)
        tool_output = result.content[0].text if result.content else "{}"
    except Exception as e:
        tool_output = json.dumps({"error": str(e)})

    # Truncate display but pass full result to model
    display = tool_output[:300]
    if len(tool_output) > 300:
        display += "..."
    print(f"  [result] {display}")

    messages.append({
        "role": "user",
        "content": f"Tool result for {fn_name}:\n{tool_output}",
    })
    return tool_output


def main():
    parser = argparse.ArgumentParser(description="Ollama ↔ dhan-nifty MCP bridge")
    parser.add_argument("--model", default="qwen3:8b",
                        help="Ollama model name (default: qwen3:8b)")
    parser.add_argument("--no-native-tools", action="store_true",
                        help="Use text-based tool calling instead of native (for models that don't support it)")
    args = parser.parse_args()

    asyncio.run(run_bridge(args.model, use_native_tools=not args.no_native_tools))


if __name__ == "__main__":
    main()
