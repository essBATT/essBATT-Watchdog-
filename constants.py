# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""Constants for essBATT Watchdog."""

DEBUGGING_ON = False  # Switch for debugging (config path selection)

# Victron / Cerbo dbus-mqtt keepalive interval [s]
CERBO_KEEPALIVE_LENGTH = 30.0

# MQTT protocol keepalive to the local broker (paho connect keepalive=)
MQTT_BROKER_PROTOCOL_KEEPALIVE_S = 60
# Legacy alias
MQTT_SERVER_TIMEOUT_TIMESPAN = MQTT_BROKER_PROTOCOL_KEEPALIVE_S

# How long to wait for the first successful CONNACK before giving up
MQTT_INITIAL_CONNECT_TIMEOUT_S = 60.0

# Log a warning if broker has been down at least this long (and every interval after)
MQTT_DISCONNECT_WARN_AFTER_S = 30.0
MQTT_DISCONNECT_WARN_INTERVAL_S = 60.0

LOGLEVEL_NAME_TO_NUMBER = {
    'CRITICAL': 50, 'FATAL': 50, 'ERROR': 40, 'WARNING': 30, 'WARN': 30,
    'INFO': 20, 'DEBUG': 10, 'NOTSET': 0,
}

MULTIS_SWITCH_NUMBER_STRING_MAPPING = {
    '1': "CHARGER ON INVERTER OFF",
    '2': "INVERTER ON CHARGER OFF",
    '3': "INVERTER AND CHARGER ON",
    '4': "INVERTER AND CHARGER OFF",
}

# MultiPlus switch position used as hardware safe state (see Venus / vebus Mode)
SAFE_STATE_MULTIS_OFF = 4

# Default MultiPlus Mode when recovering / not in failure (On)
DEFAULT_MULTIS_ON = 3
