# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""Pure helpers for MQTT subscription lists, Victron keepalive topics,
and topic→callback registration tables for the Watchdog.

No hard dependency on a live MQTT client for the builders — only
``register_topic_callbacks`` talks to the client.
"""

import json


def build_subscription_list(base_path_str, config):
    """Build MQTT subscription list for Victron CCGX (+ optional controller heartbeat).

    Args:
        base_path_str: e.g. "N/<vrm_id>"
        config: watchdog_config data

    Returns:
        list of (topic, qos) tuples
    """
    subscription_list = [
        (base_path_str + "/battery/#", 1),
        (base_path_str + "/grid/#", 1),
        (base_path_str + "/solarcharger/#", 1),
        (base_path_str + "/system/+/Ac/Consumption/#", 1),
        (base_path_str + "/settings/#", 1),
        (base_path_str + "/vebus/+/Mode", 1),
    ]

    heartbeat = config.get('controller_heartbeat_topic')
    if heartbeat and heartbeat != "none":
        subscription_list.append((heartbeat, 1))

    return subscription_list


def build_keepalive_selected_topics():
    """Return Victron topic filters requested on selective keepalive."""
    return [
        "battery/+/Dc/0/#",
        "battery/+/Soc",
        "battery/+/System/MaxCellVoltage",
        "battery/+/System/MinCellVoltage",
        "grid/+/Ac/Power",
        "grid/+/Ac/L1/Power",
        "grid/+/Ac/L1/Current",
        "grid/+/Ac/L2/Power",
        "grid/+/Ac/L2/Current",
        "grid/+/Ac/L3/Power",
        "grid/+/Ac/L3/Current",
        "system/+/Ac/Consumption/#",
        "solarcharger/+/Yield/Power",
        "solarcharger/+/Dc/0/#",
        "+/+/ProductId",
        "settings/+/Settings/CGwacs/#",
        "settings/+/Settings/SystemSetup/#",
        "vebus/+/Mode",
    ]


def build_keepalive_publish(vrm_id, keepalive_get_all_topics):
    """Build topic and payload for a Cerbo keepalive publish.

    Args:
        vrm_id: Victron VRM portal id
        keepalive_get_all_topics: 0 = selected topics JSON list, 1 = empty serial request

    Returns:
        (topic_string, payload_string) or None if mode invalid
    """
    if keepalive_get_all_topics == 1:
        return ("R/" + vrm_id + "/system/0/Serial", "")
    if keepalive_get_all_topics == 0:
        topics_list = build_keepalive_selected_topics()
        return ("R/" + vrm_id + "/keepalive", json.dumps(topics_list))
    return None


def _paho_msg_callback(handler):
    """Adapt a one-arg handler(msg) to paho's (client, userdata, msg) signature."""
    def callback(client, userdata, msg):
        handler(msg)
    return callback


def build_ccgx_topic_bindings(base_path_str, ingestion):
    """Build (topic, paho_callback) pairs for Victron CCGX data topics.

    Args:
        base_path_str: e.g. "N/<vrm_id>"
        ingestion: CcgxIngestion instance (handlers take msg only)

    Returns:
        list of (topic_string, callback) suitable for register_topic_callbacks
    """
    relative = [
        ("/grid/+/Ac/Power", ingestion.on_grid_power),
        ("/grid/+/Ac/L1/Power", ingestion.on_L1_power),
        ("/grid/+/Ac/L1/Current", ingestion.on_L1_current),
        ("/grid/+/Ac/L2/Power", ingestion.on_L2_power),
        ("/grid/+/Ac/L2/Current", ingestion.on_L2_current),
        ("/grid/+/Ac/L3/Power", ingestion.on_L3_power),
        ("/grid/+/Ac/L3/Current", ingestion.on_L3_current),
        ("/battery/+/Soc", ingestion.on_battery_soc),
        ("/battery/+/System/MaxCellVoltage", ingestion.on_battery_maxcellvoltage),
        ("/battery/+/System/MinCellVoltage", ingestion.on_battery_mincellvoltage),
        ("/battery/+/Dc/0/Temperature", ingestion.on_battery_temp),
        ("/battery/+/Dc/0/Current", ingestion.on_battery_current),
        ("/battery/+/Dc/0/Power", ingestion.on_battery_power),
        ("/battery/+/Dc/0/Voltage", ingestion.on_battery_voltage),
        ("/solarcharger/+/Yield/Power", ingestion.on_solarcharger_power),
        ("/solarcharger/+/Dc/0/#", ingestion.on_solarcharger_dc_values),
        ("/system/+/Ac/Consumption/#", ingestion.on_system_ac_consumption),
        ("/settings/+/Settings/CGwacs/#", ingestion.on_settings_cgwacs),
        ("/settings/+/Settings/SystemSetup/#", ingestion.on_settings_system_setup),
        ("/vebus/+/Mode", ingestion.on_multis_switch_mode),
    ]
    return [
        (base_path_str + path, _paho_msg_callback(handler))
        for path, handler in relative
    ]


def build_controller_heartbeat_binding(config, on_heartbeat):
    """Optional binding for the essBATT controller heartbeat topic.

    Args:
        config: watchdog_config
        on_heartbeat: callable(msg) invoked when a heartbeat arrives

    Returns:
        list of (topic, callback); empty if not configured
    """
    topic = config.get('controller_heartbeat_topic')
    if not topic or topic == "none":
        return []
    return [(topic, _paho_msg_callback(on_heartbeat))]


def register_topic_callbacks(mqtt_client, bindings):
    """Register (topic, callback) pairs via mqtt_client.message_callback_add."""
    for topic, callback in bindings:
        mqtt_client.message_callback_add(topic, callback)
