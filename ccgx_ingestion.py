# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""Ingest Victron dbus-mqtt messages into the shared CCGX_data structure.

All handlers accept a minimal msg-like object with .topic and .payload attributes
so they can be unit-tested without a real MQTT client.

Also notifies an optional activity callback so the failure monitor can track
last-seen timestamps for CCGX liveness.
"""

import json


class CcgxIngestion:
    """Updates ccgx_data from Victron MQTT topic callbacks."""

    def __init__(self, logger, ccgx_data, on_activity=None, time_fn=None):
        """
        Args:
            logger: logger instance
            ccgx_data: shared nested dict (grid, battery, …)
            on_activity: optional callback() invoked on every successful store
            time_fn: clock used only if on_activity needs time (not used here)
        """
        self.logger = logger
        self.ccgx_data = ccgx_data
        self._on_activity = on_activity
        self._time_fn = time_fn

    def set_on_activity(self, callback):
        """Set/replace activity hook (e.g. failure_monitor.note_ccgx_message)."""
        self._on_activity = callback

    def _notify_activity(self):
        if self._on_activity is not None:
            try:
                self._on_activity()
            except Exception:
                self.logger.exception('CCGX activity callback failed')

    def parse_victron_mqtt_value(self, msg):
        """Parse Victron JSON payload {"value": ...}. Returns value or None."""
        try:
            payload = json.loads(msg.payload)
        except json.JSONDecodeError:
            self.logger.warning(
                'Invalid JSON on MQTT topic ' + msg.topic + ': ' + str(msg.payload)
            )
            return None
        if 'value' not in payload:
            self.logger.warning('MQTT message without "value" on topic ' + msg.topic)
            return None
        return payload['value']

    def device_removed_from_bus(self, topic):
        """Clear device data when an empty / invalid payload indicates removal."""
        split_topic = topic.split('/')
        try:
            device_type = split_topic[2]
            device_instance = split_topic[3]
            if device_type != 'solarcharger':
                self.ccgx_data[device_type] = {}
            else:
                self.ccgx_data[device_type][device_instance] = {}
            self.logger.info(
                'Received json string without "value". Probably device removed from bus. '
                'Topic: ' + topic
            )
        except (KeyError, IndexError) as e:
            self.logger.error(
                'Deleting device instance due to empty payload failed: ' + str(e)
            )

    def _store_simple(self, category, key, msg):
        """Store json payload value under ccgx_data[category][key], or clear on error."""
        try:
            self.ccgx_data[category][key] = json.loads(msg.payload)['value']
            self._notify_activity()
        except (json.JSONDecodeError, KeyError, TypeError, IndexError):
            self.device_removed_from_bus(msg.topic)

    # ------------------------------------------------------------------
    # Grid
    # ------------------------------------------------------------------
    def on_grid_power(self, msg):
        self._store_simple('grid', 'grid_power_sum', msg)

    def on_L1_power(self, msg):
        self._store_simple('grid', 'L1_power', msg)

    def on_L1_current(self, msg):
        self._store_simple('grid', 'L1_current', msg)

    def on_L2_power(self, msg):
        self._store_simple('grid', 'L2_power', msg)

    def on_L2_current(self, msg):
        self._store_simple('grid', 'L2_current', msg)

    def on_L3_power(self, msg):
        self._store_simple('grid', 'L3_power', msg)

    def on_L3_current(self, msg):
        self._store_simple('grid', 'L3_current', msg)

    # ------------------------------------------------------------------
    # Battery
    # ------------------------------------------------------------------
    def on_battery_soc(self, msg):
        self._store_simple('battery', 'soc', msg)

    def on_battery_maxcellvoltage(self, msg):
        self._store_simple('battery', 'max_cell_voltage', msg)

    def on_battery_mincellvoltage(self, msg):
        self._store_simple('battery', 'min_cell_voltage', msg)

    def on_battery_temp(self, msg):
        self._store_simple('battery', 'temperature', msg)

    def on_battery_current(self, msg):
        self._store_simple('battery', 'current', msg)

    def on_battery_power(self, msg):
        self._store_simple('battery', 'power', msg)

    def on_battery_voltage(self, msg):
        self._store_simple('battery', 'voltage', msg)

    # ------------------------------------------------------------------
    # Solarcharger
    # ------------------------------------------------------------------
    def on_solarcharger_power(self, msg):
        split_topic = msg.topic.split('/')
        try:
            payload_value = json.loads(msg.payload)['value']
            solar_id = split_topic[3]
            if solar_id not in self.ccgx_data['solarcharger']:
                self.ccgx_data['solarcharger'][solar_id] = {}
            self.ccgx_data['solarcharger'][solar_id]['Power'] = payload_value
            self._notify_activity()
        except (json.JSONDecodeError, KeyError, TypeError, IndexError):
            self.device_removed_from_bus(msg.topic)

    def on_solarcharger_dc_values(self, msg):
        split_topic = msg.topic.split('/')
        try:
            payload_value = json.loads(msg.payload)['value']
            solar_id = split_topic[3]
            value_name = split_topic[6]
            if solar_id not in self.ccgx_data['solarcharger']:
                self.ccgx_data['solarcharger'][solar_id] = {}
            self.ccgx_data['solarcharger'][solar_id][value_name] = payload_value
            self._notify_activity()
        except (json.JSONDecodeError, KeyError, TypeError, IndexError):
            self.device_removed_from_bus(msg.topic)

    # ------------------------------------------------------------------
    # System / settings / vebus
    # ------------------------------------------------------------------
    def on_system_ac_consumption(self, msg):
        split_topic = msg.topic.split('/')
        try:
            phase_number = split_topic[6]
            measurement_name = split_topic[7]
        except IndexError:
            self.logger.error('Malformed system consumption topic: ' + msg.topic)
            return
        if phase_number != 'NumberOfPhases' and measurement_name == 'Power':
            payload_value = self.parse_victron_mqtt_value(msg)
            if payload_value is not None:
                value_name = phase_number + '_loads_power_consumption'
                self.ccgx_data['system'][value_name] = payload_value
                self._notify_activity()

    def on_settings_cgwacs(self, msg):
        split_topic = msg.topic.split('/')
        try:
            value_name = split_topic[6]
            settings_instance = split_topic[3]
        except IndexError:
            self.logger.error('Malformed settings CGwacs topic: ' + msg.topic)
            return
        payload_value = self.parse_victron_mqtt_value(msg)
        if payload_value is None:
            return
        self.ccgx_data['settings'][value_name] = payload_value
        if 'settings_base_path' not in self.ccgx_data:
            self.ccgx_data['settings_base_path'] = (
                'settings/' + settings_instance + '/Settings/'
            )
        self._notify_activity()

    def on_settings_system_setup(self, msg):
        split_topic = msg.topic.split('/')
        try:
            value_name = split_topic[6]
        except IndexError:
            self.logger.error('Malformed settings SystemSetup topic: ' + msg.topic)
            return
        payload_value = self.parse_victron_mqtt_value(msg)
        if payload_value is not None:
            self.ccgx_data['settings'][value_name] = payload_value
            self._notify_activity()

    def on_multis_switch_mode(self, msg):
        split_topic = msg.topic.split('/')
        try:
            instance_id = split_topic[3]
            value_name = split_topic[4]
        except IndexError:
            self.logger.error('Malformed vebus topic: ' + msg.topic)
            return
        payload_value = self.parse_victron_mqtt_value(msg)
        if payload_value is None:
            return
        if 'vebus' not in self.ccgx_data:
            self.ccgx_data['vebus'] = {}
        if instance_id not in self.ccgx_data['vebus']:
            self.ccgx_data['vebus'][instance_id] = {}
        self.ccgx_data['vebus'][instance_id][value_name] = payload_value
        self._notify_activity()
