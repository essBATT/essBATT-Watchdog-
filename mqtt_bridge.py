# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""MQTT connection lifecycle, subscriptions, and topic-callback registration.

Owns the paho client and connection state so the watchdog can stay a
composition root + monitor cycle without MQTT plumbing noise.

Reconnect behaviour
-------------------
``loop_start()`` runs paho's network thread with auto-reconnect enabled.
On every successful CONNACK (first connect *and* reconnect) we re-subscribe
and re-register topic handlers (required with clean_session=True), then
invoke an optional ``on_connected`` callback so the app can send keepalive
and start timers.
"""

import time

import paho.mqtt.client as mqtt

import constants
from mqtt_helpers import (
    build_subscription_list,
    build_ccgx_topic_bindings,
    build_controller_heartbeat_binding,
    register_topic_callbacks,
)


STATE_DISCONNECTED = 'disconnected'
STATE_CONNECTING = 'connecting'
STATE_CONNECTED = 'connected'


class MqttBridge:
    """Connects to the local MQTT broker, subscribes, and wires topic handlers.

    Dependencies (config, ingestion, heartbeat handler) are injected so unit
    tests can run without a real broker.
    """

    def __init__(
        self,
        logger,
        config,
        ingestion,
        host='localhost',
        client_factory=None,
        sleep_fn=None,
        on_connected=None,
        time_fn=None,
        on_controller_heartbeat=None,
    ):
        """
        Args:
            logger: logger instance
            config: watchdog_config dict
            ingestion: CcgxIngestion instance for Victron topic handlers
            host: MQTT broker host (default localhost)
            client_factory: callable returning a paho-like client
            sleep_fn: sleep used while waiting for connection
            on_connected: optional callback(is_reconnect: bool) after subscribe
            time_fn: clock for timeouts / disconnect duration
            on_controller_heartbeat: optional handler(msg) for controller liveness
        """
        self.logger = logger
        self.config = config
        self.ingestion = ingestion
        self.host = host
        self._client_factory = client_factory or mqtt.Client
        self._sleep_fn = sleep_fn or time.sleep
        self._time_fn = time_fn or time.time
        self._on_connected = on_connected
        self._on_controller_heartbeat = on_controller_heartbeat

        self.client = None
        self.state = STATE_DISCONNECTED
        self._successful_connects = 0
        self._handlers_registered = False
        self.disconnected_since = None
        self._stopping = False

    @property
    def is_connected(self):
        return self.state == STATE_CONNECTED

    @property
    def connection_ok(self):
        return self.is_connected

    @connection_ok.setter
    def connection_ok(self, value):
        if value:
            self.state = STATE_CONNECTED
            self.disconnected_since = None
        else:
            self.state = STATE_DISCONNECTED
            if self.disconnected_since is None:
                self.disconnected_since = self._time_fn()

    @property
    def disconnected(self):
        return self.state != STATE_CONNECTED

    def disconnect_duration_s(self):
        if self.is_connected or self.disconnected_since is None:
            return 0.0
        return max(0.0, self._time_fn() - self.disconnected_since)

    def update_config(self, config):
        self.config = config

    def set_on_connected(self, callback):
        self._on_connected = callback

    def set_on_controller_heartbeat(self, callback):
        self._on_controller_heartbeat = callback

    def setup(self):
        """Create MQTT client, set credentials and lifecycle callbacks."""
        self.client = self._client_factory()
        self.client.username_pw_set(
            username=self.config['mqtt_username'],
            password=self.config['mqtt_password'],
        )
        if hasattr(self.client, 'reconnect_delay_set'):
            self.client.reconnect_delay_set(min_delay=1, max_delay=120)
        self.client.on_connect = self.on_connect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self._handlers_registered = False
        self._successful_connects = 0
        self._stopping = False
        return self.client

    def connect(self):
        """Connect to the broker and start the network loop (non-blocking)."""
        if self.client is None:
            self.setup()
        self.state = STATE_CONNECTING
        port = self.config['mqtt_server_COM_port']
        self.client.connect(
            self.host,
            port=port,
            keepalive=constants.MQTT_BROKER_PROTOCOL_KEEPALIVE_S,
            bind_address="",
        )
        self.logger.info(
            "essBATT watchdog: Try to connect to MQTT Server: "
            + self.host + " on port " + str(port)
            + " with protocol keepalive of "
            + str(constants.MQTT_BROKER_PROTOCOL_KEEPALIVE_S) + "s"
        )
        self.client.loop_start()

    def wait_until_connected(self, timeout=None):
        """Block until on_connect reports success, or timeout."""
        if timeout is None:
            timeout = constants.MQTT_INITIAL_CONNECT_TIMEOUT_S
        deadline = self._time_fn() + float(timeout)
        while not self.is_connected:
            if self._time_fn() >= deadline:
                self.logger.error(
                    "MQTT initial connect timed out after "
                    + str(timeout) + "s (no successful CONNACK)."
                )
                return False
            self.logger.info("Waiting for MQTT server connection...")
            self._sleep_fn(1)
        return True

    def subscribe_and_register_handlers(self):
        """Subscribe to Victron (+ heartbeat) topics and register callbacks.

        Safe to call on every CONNACK: subscriptions must be renewed after
        reconnect with clean_session=True; topic callbacks are registered once.
        """
        if self.client is None:
            return None

        base_path_str = "N/" + self.config['vrm_id']
        subscription_list = build_subscription_list(base_path_str, self.config)
        result, _mid = self.client.subscribe(subscription_list)
        self.logger.info("MQTT subscription function return value: " + str(result))

        if not self._handlers_registered:
            bindings = build_ccgx_topic_bindings(base_path_str, self.ingestion)
            if self._on_controller_heartbeat is not None:
                bindings.extend(
                    build_controller_heartbeat_binding(
                        self.config, self._on_controller_heartbeat
                    )
                )
            register_topic_callbacks(self.client, bindings)
            self._handlers_registered = True
            self.logger.debug(
                "Registered " + str(len(bindings)) + " MQTT topic callbacks."
            )
        return result

    def start(self, connect_timeout=None):
        """Full first-connect sequence: setup → connect → wait.

        Raises:
            OSError: network/connect failure from paho connect()
            TimeoutError: no successful CONNACK within connect_timeout
        """
        self.setup()
        self.connect()
        if not self.wait_until_connected(timeout=connect_timeout):
            self.stop()
            raise TimeoutError(
                "MQTT broker did not accept the connection within the timeout"
            )

    def stop(self):
        """Graceful MQTT shutdown: disconnect + stop network loop."""
        self._stopping = True
        if self.client is None:
            self.state = STATE_DISCONNECTED
            return
        try:
            self.client.disconnect()
        except Exception as e:
            self.logger.debug("MQTT disconnect during stop: " + str(e))
        try:
            self.client.loop_stop()
        except Exception as e:
            self.logger.debug("MQTT loop_stop during stop: " + str(e))
        self.state = STATE_DISCONNECTED

    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            self.logger.error(
                "Failed to connect to MQTT Server with result code " + str(rc)
            )
            self.state = STATE_DISCONNECTED
            if self.disconnected_since is None:
                self.disconnected_since = self._time_fn()
            return

        is_reconnect = self._successful_connects > 0
        self._successful_connects += 1
        self.state = STATE_CONNECTED
        self.disconnected_since = None

        session_present = None
        if isinstance(flags, dict):
            session_present = flags.get('session present')

        if is_reconnect:
            self.logger.info(
                "MQTT reconnected (result code " + str(rc)
                + ", session_present=" + str(session_present) + ")"
            )
        else:
            self.logger.info(
                "Success: Connected to MQTT Server with result code " + str(rc)
                + ("" if session_present is None else
                   ", session_present=" + str(session_present))
            )

        try:
            self.subscribe_and_register_handlers()
        except Exception:
            self.logger.exception("MQTT subscribe/register after connect failed")

        if self._on_connected is not None:
            try:
                self._on_connected(is_reconnect=is_reconnect)
            except Exception:
                self.logger.exception("on_connected application callback failed")

    def on_disconnect(self, client, userdata, rc):
        if self._stopping or rc == 0:
            self.logger.info("MQTT disconnected (rc=" + str(rc) + ").")
        else:
            self.logger.warning(
                "MQTT server disconnected unexpectedly. Reason code: " + str(rc)
                + ". paho will attempt auto-reconnect."
            )
        self.state = STATE_DISCONNECTED
        if self.disconnected_since is None:
            self.disconnected_since = self._time_fn()

    def on_message(self, client, userdata, msg):
        self.logger.debug(
            "New unused (!!!) MQTT message: " + msg.topic + " " + str(msg.payload)
        )

    def on_subscribe(self, client, userdata, mid, granted_qos):
        self.logger.debug("MQTT on_subscribe function called!")
