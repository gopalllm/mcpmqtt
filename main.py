import json
from datetime import datetime
import paho.mqtt.client as paho
from paho import mqtt


# setting callbacks for different events to see if it works, print the message etc.
def on_connect(client, userdata, flags, rc, properties=None):
    #print("CONNACK received with code %s." % rc)
    pass

# print which topic was subscribed to
def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    #print("Subscribed: " + str(mid) + " " + str(granted_qos))
    pass


# print message, useful for checking if it was successful
def on_message(client, userdata, msg):
    payload_str = msg.payload.decode('utf-8')
    data = json.loads(payload_str)
    # Helper to convert milliseconds since epoch to a readable UTC timestamp
    def convert_ts(ts: int) -> str:
        return datetime.utcfromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S UTC')
    # Prepare lines for output
    lines = []
    # Build a nested dictionary from each path
    tree = {}
    for entry in data:
        parts = entry["t"].strip("/").split("/")
        node = tree
        for segment in parts[:-1]:
            node = node.setdefault(segment, {})
        node.setdefault(parts[-1], {})["_data"] = entry

    # Recursive function to walk the tree and append lines
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

    # Populate lines by traversing the tree
    print_node(tree)
    # Combine lines into final output
    output = "\n".join(lines)
    print(output)  # Print before returning
    return output



client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect
client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
client.username_pw_set("gopalmqttuser", "GkGk@cyber7880")
client.connect("01eb605dc7ff4cf2ac14e103b3a746f2.s1.eu.hivemq.cloud", 8883)
client.on_subscribe = on_subscribe
client.on_message = on_message
client.subscribe("#", qos=1)
client.loop_forever()