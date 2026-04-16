#!/usr/bin/env python3
"""IPMI to MQTT bridge for Home Assistant Add-on."""

import json
import logging
import os
import re
import subprocess
import sys
import time
import urllib.request
import urllib.error

import paho.mqtt.client as mqtt

# Constants
OPTIONS_PATH = "/data/options.json"
SUPERVISOR_API = "http://supervisor/services/mqtt"

HA_BINARY_TOPIC = "homeassistant/binary_sensor"
HA_SENSOR_TOPIC = "homeassistant/sensor"
HA_SWITCH_TOPIC = "homeassistant/switch"

POWER_TOPIC = "server_power_state"
SWITCH_TOPIC = "server_switch"

# Global state
guid_dict = {}
complete_guid_dict = {}
topic_dict = {}
client = None
mqtt_host = ""


def load_options():
    """Load add-on options from /data/options.json."""
    try:
        with open(OPTIONS_PATH, "r") as f:
            options = json.load(f)
        logging.info("Add-on options loaded successfully.")
        return options
    except Exception as e:
        logging.critical("Failed to load add-on options: %s", e)
        sys.exit(1)


def get_mqtt_config(options):
    """Get MQTT connection config from add-on options or HA service."""
    # If manual MQTT host is configured, use it
    if options.get("mqtt_host"):
        logging.info("Using manually configured MQTT broker: %s", options["mqtt_host"])
        return {
            "host": options["mqtt_host"],
            "port": options.get("mqtt_port", 1883),
            "username": options.get("mqtt_user", ""),
            "password": options.get("mqtt_password", ""),
        }

    # Try HA MQTT service discovery via Supervisor API
    supervisor_token = os.environ.get("SUPERVISOR_TOKEN")
    if supervisor_token:
        try:
            req = urllib.request.Request(
                SUPERVISOR_API,
                headers={
                    "Authorization": f"Bearer {supervisor_token}",
                    "Content-Type": "application/json",
                },
            )
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode())["data"]
                logging.info(
                    "Using Home Assistant MQTT service: %s:%s",
                    data["host"],
                    data["port"],
                )
                return {
                    "host": data["host"],
                    "port": data["port"],
                    "username": data.get("username", ""),
                    "password": data.get("password", ""),
                }
        except urllib.error.URLError as e:
            logging.warning("Could not reach Supervisor MQTT service: %s", e)
        except Exception as e:
            logging.warning("Failed to get MQTT config from Supervisor: %s", e)

    logging.critical(
        "No MQTT configuration available. Set mqtt_host in add-on config "
        "or configure the MQTT integration in Home Assistant."
    )
    sys.exit(1)


def on_connect(client, userdata, flags, rc):
    """Handle MQTT connection."""
    if int(rc) == 0:
        logging.info("Connected to MQTT broker successfully.")
        client.connected_flag = True
    else:
        messages = {
            1: "incorrect protocol version",
            2: "invalid client identifier",
            3: "server unavailable",
            4: "bad username or password",
            5: "not authorized",
        }
        reason = messages.get(int(rc), f"unknown error (rc={rc})")
        logging.error("MQTT connection refused: %s", reason)


def on_message(client, userdata, msg):
    """Handle incoming MQTT messages (switch commands)."""
    if "$SYS/" in msg.topic:
        return

    payload = msg.payload.decode("utf-8")
    logging.info("Received message on %s: %s", msg.topic, payload)

    # Extract server GUID from topic: homeassistant/switch/{guid}_server_switch/set
    server_guid = msg.topic.replace(HA_SWITCH_TOPIC + "/", "")
    server_guid = server_guid.replace("_" + SWITCH_TOPIC + "/set", "")

    if server_guid not in complete_guid_dict:
        logging.warning("Received command for unknown server GUID: %s", server_guid)
        return

    server_info = complete_guid_dict[server_guid]
    server_ip = server_info["server_ip"]
    server_user = server_info["server_user"]
    server_pass = server_info["server_pass"]

    if payload == "on":
        action = "on"
    elif payload == "off":
        action = "off"
    else:
        logging.warning("Unknown power command: %s", payload)
        return

    ipmi_cmd = (
        f'ipmitool -I lanplus -L Administrator -H "{server_ip}" '
        f'-U "{server_user}" -P "{server_pass}" chassis power {action}'
    )
    logging.debug("Sending IPMI command: chassis power %s", action)
    subprocess.run(ipmi_cmd, shell=True, capture_output=True)

    # Clear the command topic and update power state
    client.publish(msg.topic, "", qos=2, retain=True)
    get_single_power_data(server_guid)


def on_publish(client, userdata, mid):
    """Handle MQTT publish confirmation."""
    logging.debug("Message published (mid=%s)", mid)


def on_subscribe(client, userdata, mid, granted_qos):
    """Handle MQTT subscription confirmation."""
    logging.info("Subscribed (mid=%s, qos=%s)", mid, granted_qos)


def mqtt_publish(topic, payload):
    """Publish a message to MQTT."""
    client.publish(str(topic), str(payload), qos=2, retain=True)
    logging.debug("Published to %s: %s", topic, payload)


def get_guid(servers):
    """Get IPMI GUID for each server."""
    g_dict = {}
    cg_dict = {}

    for server in servers:
        name = server["name"]
        ip = server["host"]
        user = server["username"]
        passwd = server["password"]

        cmd = (
            f'ipmitool -I lanplus -H "{ip}" -L User '
            f'-U "{user}" -P "{passwd}" mc guid | grep -i guid'
        )
        result = subprocess.run(cmd, shell=True, capture_output=True)
        output = result.stdout.decode("utf-8").strip()

        if not output:
            logging.error(
                "Server '%s' (%s) returned no GUID. Check IPMI connectivity.", name, ip
            )
            continue

        # Strip "System GUID  : " prefix
        guid = output[15:].strip() if len(output) > 15 else output.strip()

        if not guid:
            logging.error("Empty GUID for server '%s' (%s).", name, ip)
            continue

        g_dict[ip] = guid
        cg_dict[guid] = {
            "server_ip": ip,
            "server_user": user,
            "server_pass": passwd,
            "server_nodename": name,
            "brand": server.get("brand", "OTHER"),
        }
        logging.info("Server '%s' GUID: %s", name, guid)

    return g_dict, cg_dict


def power_sdr_initialization(servers):
    """Create MQTT discovery config for power binary sensors."""
    for server in servers:
        name = server["name"]
        ip = server["host"]

        if ip not in guid_dict:
            logging.warning("Skipping power init for '%s': no GUID.", name)
            continue

        identifier = guid_dict[ip]
        device_config = {
            "identifiers": identifier,
            "configuration_url": f"http://{ip}",
            "manufacturer": server.get("brand", "OTHER"),
            "name": name,
        }

        config_topic = f"{HA_BINARY_TOPIC}/{identifier}_{POWER_TOPIC}/config"
        state_topic = f"{HA_BINARY_TOPIC}/{identifier}_{POWER_TOPIC}/state"

        payload = json.dumps({
            "device": device_config,
            "device_class": "power",
            "name": POWER_TOPIC,
            "unique_id": f"{identifier}_power_",
            "force_update": True,
            "payload_on": "on",
            "payload_off": "off",
            "retain": True,
            "state_topic": state_topic,
        })

        mqtt_publish(config_topic, payload)
        logging.info("Power sensor initialized for '%s'.", name)


def switch_sdr_initialization(servers):
    """Create MQTT discovery config for power switches."""
    for server in servers:
        name = server["name"]
        ip = server["host"]

        if ip not in guid_dict:
            logging.warning("Skipping switch init for '%s': no GUID.", name)
            continue

        identifier = guid_dict[ip]
        device_config = {
            "identifiers": identifier,
            "configuration_url": f"http://{ip}",
            "manufacturer": server.get("brand", "OTHER"),
            "name": name,
        }

        config_topic = f"{HA_SWITCH_TOPIC}/{identifier}_{SWITCH_TOPIC}/config"
        state_topic = f"{HA_BINARY_TOPIC}/{identifier}_{POWER_TOPIC}/state"
        command_topic = f"{HA_SWITCH_TOPIC}/{identifier}_{SWITCH_TOPIC}/set"

        payload = json.dumps({
            "device": device_config,
            "device_class": "switch",
            "name": SWITCH_TOPIC,
            "unique_id": f"{identifier}_switch_",
            "force_update": True,
            "payload_on": "on",
            "payload_off": "off",
            "retain": True,
            "state_topic": state_topic,
            "command_topic": command_topic,
            "optimistic": True,
        })

        mqtt_publish(config_topic, payload)
        logging.info("Power switch initialized for '%s'.", name)


def sensor_sdr_initialization(servers, sdr_topic_types):
    """Create MQTT discovery config for SDR sensors."""
    for server in servers:
        name = server["name"]
        ip = server["host"]

        if ip not in guid_dict:
            logging.warning("Skipping SDR init for '%s': no GUID.", name)
            continue

        identifier = guid_dict[ip]
        device_config = {
            "identifiers": identifier,
            "configuration_url": f"http://{ip}",
            "manufacturer": server.get("brand", "OTHER"),
            "name": name,
        }

        for sdr in server.get("sdrs", []):
            sdr_name = sdr["name"]
            sdr_class = sdr["sdr_class"]

            config_topic = f"{HA_SENSOR_TOPIC}/{identifier}_{sdr_name}/config"
            state_topic = f"{HA_SENSOR_TOPIC}/{identifier}_{sdr_name}/state"

            unit_map = {
                "temperature": ("temperature", "\u00b0C"),
                "temperaturef": ("temperature", "\u00b0F"),
                "fan": ("frequency", "RPM"),
                "frequency": ("frequency", "Hz"),
                "voltage": ("voltage", "V"),
            }

            if sdr_class in unit_map:
                device_class, unit = unit_map[sdr_class]
                payload = {
                    "device": device_config,
                    "device_class": device_class,
                    "name": sdr_name,
                    "unique_id": f"{identifier}_sdr_{sdr_name}",
                    "unit_of_meas": unit,
                    "force_update": True,
                    "retain": True,
                    "state_topic": state_topic,
                }
            else:
                logging.warning("Unknown SDR class '%s' for sensor '%s'.", sdr_class, sdr_name)
                payload = {
                    "device": device_config,
                    "name": sdr_name,
                    "unique_id": f"{identifier}_sdr_{sdr_name}",
                    "force_update": True,
                    "retain": True,
                    "state_topic": state_topic,
                }

            mqtt_publish(config_topic, json.dumps(payload))
            logging.info("SDR sensor '%s' initialized for '%s'.", sdr_name, name)


def get_power_data(servers):
    """Collect and publish power state for all servers."""
    for server in servers:
        name = server["name"]
        ip = server["host"]

        if ip not in guid_dict:
            continue

        if not server.get("enable_power_sensor", True):
            continue

        identifier = guid_dict[ip]
        user = server["username"]
        passwd = server["password"]

        state_topic = f"{HA_BINARY_TOPIC}/{identifier}_{POWER_TOPIC}/state"
        cmd = (
            f'ipmitool -I lanplus -L User -H "{ip}" '
            f'-U "{user}" -P "{passwd}" chassis power status | cut -b 18-20'
        )

        result = subprocess.run(cmd, shell=True, capture_output=True)
        power_state = result.stdout.decode("utf-8").strip()

        if power_state:
            mqtt_publish(state_topic, power_state)
            logging.debug("Server '%s' power state: %s", name, power_state)
        else:
            logging.warning("No power state returned for '%s'.", name)


def get_single_power_data(server_guid):
    """Collect and publish power state for a single server by GUID."""
    if server_guid not in complete_guid_dict:
        return

    info = complete_guid_dict[server_guid]
    state_topic = f"{HA_BINARY_TOPIC}/{server_guid}_{POWER_TOPIC}/state"

    cmd = (
        f'ipmitool -I lanplus -L User -H "{info["server_ip"]}" '
        f'-U "{info["server_user"]}" -P "{info["server_pass"]}" '
        f"chassis power status | cut -b 18-20"
    )

    result = subprocess.run(cmd, shell=True, capture_output=True)
    power_state = result.stdout.decode("utf-8").strip()

    if power_state:
        mqtt_publish(state_topic, power_state)
        logging.debug("Server '%s' power state: %s", info["server_nodename"], power_state)


def supermicro_parse(sdr_class, raw_output):
    """Parse ipmitool SDR output for Supermicro servers."""
    try:
        values = raw_output.split("|")
        raw_value = values[4].strip()[:6].strip()
        numeric = re.sub(r"[^0-9.]", "", raw_value)

        if sdr_class == "frequency":
            return str(float(numeric) / 60) if numeric else ""

        return numeric
    except (IndexError, ValueError) as e:
        logging.error("Supermicro parse error: %s", e)
        return ""


def asus_parse(sdr_class, subclass, raw_output):
    """Parse ipmitool SDR output for ASUS servers."""
    try:
        lines = raw_output.split("\n")
        matching = [l for l in lines if l.startswith(subclass)]

        if not matching:
            logging.warning("No SDR line matching subclass '%s'.", subclass)
            return ""

        values = matching[0].split("|")
        raw_value = values[4].strip()[:6].strip()
        numeric = re.sub(r"[^0-9.]", "", raw_value)

        if sdr_class == "frequency":
            return str(float(numeric) / 60) if numeric else ""

        return numeric
    except (IndexError, ValueError) as e:
        logging.error("ASUS parse error: %s", e)
        return ""


def get_sdr_data(servers):
    """Collect and publish SDR sensor data for all servers."""
    for server in servers:
        name = server["name"]
        ip = server["host"]

        if ip not in guid_dict:
            continue

        identifier = guid_dict[ip]
        user = server["username"]
        passwd = server["password"]
        brand = server.get("brand", "OTHER").upper()

        for sdr in server.get("sdrs", []):
            sdr_name = sdr["name"]
            sdr_class = sdr["sdr_class"]
            sdr_value_entity = sdr["value"]
            subclass = sdr.get("subclass", "")

            cmd = (
                f'ipmitool -I lanplus -L User -H "{ip}" '
                f'-U "{user}" -P "{passwd}" sdr entity "{sdr_value_entity}"'
            )

            result = subprocess.run(cmd, shell=True, capture_output=True)
            raw = result.stdout.decode("utf-8").strip()

            if not raw:
                logging.warning(
                    "No SDR data for '%s' on server '%s'. Server may be off.",
                    sdr_name,
                    name,
                )
                continue

            if brand == "SUPERMICRO":
                value = supermicro_parse(sdr_class, raw)
            elif brand == "ASUS":
                value = asus_parse(sdr_class, subclass, raw)
            else:
                # Generic: try supermicro format
                value = supermicro_parse(sdr_class, raw)

            if not value or value in ("No", "Di"):
                logging.warning(
                    "Empty/invalid SDR value for '%s' on '%s'. Server may be off.",
                    sdr_name,
                    name,
                )
                continue

            state_topic = f"{HA_SENSOR_TOPIC}/{identifier}_{sdr_name}/state"
            mqtt_publish(state_topic, value)
            logging.debug("SDR '%s' on '%s': %s", sdr_name, name, value)


def switch_subscribe(servers):
    """Subscribe to switch command topics for power control."""
    for server in servers:
        if not server.get("enable_power_switch", False):
            continue

        name = server["name"]
        ip = server["host"]

        if ip not in guid_dict:
            continue

        identifier = guid_dict[ip]
        subscribe_topic = f"{HA_SWITCH_TOPIC}/{identifier}_{SWITCH_TOPIC}/set"
        client.subscribe(subscribe_topic, 2)
        logging.info("Subscribed to switch topic for '%s': %s", name, subscribe_topic)


def build_sdr_topic_types(servers):
    """Build a mapping of SDR names from server configurations."""
    sdr_names = set()
    for server in servers:
        for sdr in server.get("sdrs", []):
            sdr_names.add(sdr["name"])
    return sdr_names


def main():
    """Main entry point for the IPMI-MQTT add-on."""
    global guid_dict, complete_guid_dict, topic_dict, client, mqtt_host

    # Load configuration
    options = load_options()
    servers = options.get("servers", [])

    if not servers:
        logging.critical("No servers configured. Add servers in the add-on configuration.")
        sys.exit(1)

    logging.info("Configured with %d server(s).", len(servers))

    poll_interval = options.get("poll_interval", 300)

    # Get MQTT config
    mqtt_config = get_mqtt_config(options)
    mqtt_host = mqtt_config["host"]

    # Get GUIDs for all servers
    guid_dict, complete_guid_dict = get_guid(servers)

    if not guid_dict:
        logging.critical("No server GUIDs could be retrieved. Check IPMI connectivity.")
        sys.exit(1)

    # Build topic types from config
    sdr_topic_types = build_sdr_topic_types(servers)

    # Connect to MQTT
    try:
        mqtt.Client.connected_flag = False
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
            client_id="ipmi-mqtt-addon",
        )
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_publish = on_publish
        client.on_subscribe = on_subscribe

        if mqtt_config["username"]:
            client.username_pw_set(
                mqtt_config["username"], password=mqtt_config["password"]
            )

        client.loop_start()
        client.connect(mqtt_config["host"], mqtt_config["port"], 60)

        # Wait for connection
        timeout = 30
        while not client.connected_flag and timeout > 0:
            logging.info("Waiting for MQTT connection...")
            time.sleep(1)
            timeout -= 1

        if not client.connected_flag:
            logging.critical("Failed to connect to MQTT broker within 30 seconds.")
            sys.exit(1)

        logging.info("Connected to MQTT broker at %s:%s", mqtt_config["host"], mqtt_config["port"])
    except Exception as e:
        logging.critical("MQTT connection error: %s", e)
        sys.exit(1)

    # Initialize MQTT discovery entities
    # Power sensors (for servers with enable_power_sensor)
    power_servers = [s for s in servers if s.get("enable_power_sensor", True)]
    if power_servers:
        power_sdr_initialization(power_servers)

    # Power switches (for servers with enable_power_switch)
    switch_servers = [s for s in servers if s.get("enable_power_switch", False)]
    if switch_servers:
        switch_sdr_initialization(switch_servers)

    # SDR sensors
    sdr_servers = [s for s in servers if s.get("sdrs")]
    if sdr_servers:
        sensor_sdr_initialization(sdr_servers, sdr_topic_types)

    logging.info("MQTT discovery initialization complete.")

    # Subscribe to switch command topics
    if switch_servers:
        switch_subscribe(switch_servers)

    # Main polling loop
    logging.info("Starting polling loop (interval: %ds).", poll_interval)
    while True:
        try:
            # Collect power states
            if power_servers:
                get_power_data(servers)

            # Collect SDR sensor data
            if sdr_servers:
                get_sdr_data(servers)

            logging.info("Data collection complete. Sleeping %ds.", poll_interval)
            time.sleep(poll_interval)

        except Exception as e:
            logging.error("Error in polling loop: %s", e)
            time.sleep(10)


if __name__ == "__main__":
    # Configure logging to stdout for s6-overlay
    log_level_str = os.environ.get("LOG_LEVEL", "info").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    logging.basicConfig(
        level=log_level,
        format="[%(asctime)s] %(levelname)s [%(funcName)s:%(lineno)d] %(message)s",
        stream=sys.stdout,
    )

    logging.info("IPMI-MQTT Add-on starting...")
    main()
