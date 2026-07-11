# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""Publish Victron ESS Mode 2 setpoints, Multi switch commands, and keepalive over MQTT.

Used by the Watchdog to force a safe state when the controller or CCGX fails.
"""

import json

import constants
from mqtt_helpers import build_keepalive_publish


class VictronOutput:
    """Sends setpoints and keepalive to Venus OS / Cerbo via MQTT.

    Dependencies are injected so unit tests can mock the MQTT client.
    """

    def __init__(self, logger, mqtt_client, ccgx_data, setvalue_list, write_base_path):
        self.logger = logger
        self.mqtt_client = mqtt_client
        self.ccgx_data = ccgx_data
        self.setvalue_list = setvalue_list
        self.write_base_path = write_base_path

    def set_mqtt_client(self, mqtt_client):
        """Update MQTT client reference (set after connect)."""
        self.mqtt_client = mqtt_client

    def set_ccgx_value(self, set_val_name_str=None, set_val=0,
                       only_set_if_deviation_to_current_setting=True):
        """Set a Victron settings value over MQTT.

        Returns:
            0: ok but nothing sent
            1: value published
           -1: error
        """
        if set_val_name_str is None:
            self.logger.error('No set value name given!')
            return -1

        if self.mqtt_client is None:
            self.logger.error(
                set_val_name_str + ' setpoint sending failed: no MQTT client'
            )
            return -1

        settings = self.ccgx_data.get('settings', {})
        if set_val_name_str not in settings:
            # Still try to publish if we know the write path (first-time / cold start)
            if 'settings_base_path' not in self.ccgx_data:
                self.logger.warning(
                    set_val_name_str
                    + ': cannot publish yet (no settings_base_path / current value)'
                )
                return 0
            current = None
        else:
            current = settings[set_val_name_str]
            if only_set_if_deviation_to_current_setting and set_val == current:
                return 0

        if set_val_name_str not in self.setvalue_list:
            self.logger.error(
                set_val_name_str + ' not found in ess_setvalue_list.json'
            )
            return -1

        try:
            topic_str = (
                self.write_base_path
                + self.ccgx_data['settings_base_path']
                + self.setvalue_list[set_val_name_str]
            )
            payload_str = json.dumps({"value": set_val})
            self.mqtt_client.publish(topic=topic_str, payload=payload_str, qos=1, retain=0)
            self.logger.info(
                set_val_name_str + ': Published ' + payload_str + ' on ' + topic_str
                + ('' if current is None else
                   '. previous settings value: ' + str(current))
            )
            return 1
        except (TypeError, ValueError, KeyError) as e:
            self.logger.error(set_val_name_str + ' setpoint sending failed: ' + str(e))
            return -1

    def set_multis_switch_mode(self, switch_position):
        """Set Multi/vebus Mode: 1=Charger Only, 2=Inverter Only, 3=On, 4=Off.

        Returns:
            True if a publish was attempted, False if no vebus instance / no change
        """
        if self.mqtt_client is None:
            self.logger.error('Cannot set Multis switch: no MQTT client')
            return False

        if 'vebus' not in self.ccgx_data:
            self.logger.warning('Cannot set Multis switch: no vebus data yet')
            return False

        counter = 0
        current_instance_id = ''
        for key in self.ccgx_data['vebus']:
            current_instance_id = str(key)
            counter += 1

        if not current_instance_id:
            return False

        current_mode = self.ccgx_data['vebus'][current_instance_id].get('Mode')
        published = False
        if current_mode is None or current_mode != switch_position:
            topic_str = self.write_base_path + 'vebus/' + current_instance_id + '/Mode'
            payload_str = json.dumps({"value": switch_position})
            self.mqtt_client.publish(topic=topic_str, payload=payload_str, qos=1, retain=0)
            mapping = constants.MULTIS_SWITCH_NUMBER_STRING_MAPPING.get(
                str(switch_position), 'UNKNOWN'
            )
            self.logger.warning(
                '"Multis SWITCH" switched to ' + mapping
                + ' (value: ' + str(switch_position) + ')'
            )
            published = True

        if counter > 1:
            self.logger.error(
                'It seems that there is more than one instance of "vebus" available. '
                'This was not considered during development of the script and needs '
                'to be investigated!!!'
            )
        return published

    def send_keepalive(self, vrm_id, keepalive_get_all_topics):
        """Publish a Cerbo/Venus dbus-mqtt keepalive.

        See https://github.com/victronenergy/dbus-mqtt and ESS Mode 2 docs.
        """
        if self.mqtt_client is None:
            return False
        try:
            pub = build_keepalive_publish(vrm_id, keepalive_get_all_topics)
            if pub is None:
                self.logger.warning(
                    'Invalid keepalive_get_all_topics value: '
                    + str(keepalive_get_all_topics)
                )
                return False
            topic_string, payload = pub
            if keepalive_get_all_topics == 1:
                errcode = self.mqtt_client.publish(
                    topic_string, payload=payload, qos=0, retain=False
                )
                self.logger.debug(
                    "Keepalive (all topics) message send! Errorcode: "
                    + str(errcode) + ". Published topic: '" + topic_string
                )
            else:
                errcode = self.mqtt_client.publish(topic_string, payload)
                self.logger.debug(
                    "Keepalive (selected topics) message send! Errorcode: "
                    + str(errcode) + ". Published topic: '" + topic_string
                    + '. Payload: ' + payload
                )
            return True
        except (KeyError, TypeError, ValueError) as e:
            self.logger.warning(
                'Failed to publish keepalive message to Cerbo: ' + str(e)
            )
            return False
        except Exception:
            self.logger.exception(
                'Unexpected error while publishing keepalive to Cerbo'
            )
            return False
