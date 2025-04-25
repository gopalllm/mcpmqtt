from mcp.server.fastmcp import FastMCP
from paho.mqtt import subscribe
import json
from datetime import datetime


mcp = FastMCP(name="MinimalServer", host="0.0.0.0", port=8099)


@mcp.tool()
def add(a: int, b: int) -> int:
    """Adds two numbers."""
    return a + b


@mcp.tool()
def process_mqtt_data()-> str:

    """Get meter tag data from the json data format for the specified sub or blk or cab or trk  from the main meter.

    Args:
        name: Name of the main meter
        trk: Name of the trk
        blk: Name of the block
        sub: Name of the substation
    """



    msg = subscribe.simple("mcpserver",
                           hostname="broker.mqtt.cool",
                           port=1883,
                           msg_count=1,
                           auth={"username": "gopalmqttuser", "password": "GkGk@cyber7880"},
                           qos=1)

    payload_str = msg.payload.decode('utf-8')
    data = json.loads(payload_str)

    def convert_ts(ts: int) -> str:
        return datetime.utcfromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S UTC')

    lines = []
    tree = {}

    for entry in data:
        parts = entry["t"].strip("/").split("/")
        node = tree
        for segment in parts[:-1]:
            node = node.setdefault(segment, {})
        node.setdefault(parts[-1], {})["_data"] = entry

    def print_node(node, depth=0):
        emoji = {0: "📂", 1: "🔧"}.get(depth, "🔹")
        for key, subtree in sorted(node.items()):
            indent = "  " * depth
            if "_data" in subtree:
                d = subtree["_data"]
                timestamp = convert_ts(d.get("ts")) if d.get("ts") else "Unknown Time"
                lines.append(f"{indent}{emoji} {key}: {d['v']} | Time: {timestamp}")
            else:
                lines.append(f"{indent}{emoji} {key}")
                print_node(subtree, depth + 1)

    print_node(tree)
    output = "\n".join(lines)
    return output  # Simply changed the print to return


if __name__ == "__main__":
    mcp.run(transport="sse")
