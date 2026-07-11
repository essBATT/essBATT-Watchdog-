"""Tests for VictronOutput (setpoints, Multis mode, keepalive)."""

import json
from unittest.mock import MagicMock

import pytest

from victron_output import VictronOutput


@pytest.fixture
def setvalue_list():
    return {
        'AcPowerSetPoint': 'CGwacs/AcPowerSetPoint',
        'MaxChargeCurrent': 'SystemSetup/MaxChargeCurrent',
        'MaxDischargePower': 'CGwacs/MaxDischargePower',
    }


@pytest.fixture
def ccgx_data():
    return {
        'settings': {
            'AcPowerSetPoint': 0,
            'MaxChargeCurrent': 5,
            'MaxDischargePower': 300,
        },
        'settings_base_path': 'settings/0/Settings/',
        'vebus': {
            '276': {'Mode': 3},
        },
    }


@pytest.fixture
def mqtt_client():
    return MagicMock()


@pytest.fixture
def output(ccgx_data, setvalue_list, mqtt_client):
    return VictronOutput(
        logger=MagicMock(),
        mqtt_client=mqtt_client,
        ccgx_data=ccgx_data,
        setvalue_list=setvalue_list,
        write_base_path='W/vrm123/',
    )


def test_set_ccgx_value_publishes_on_change(output, mqtt_client):
    result = output.set_ccgx_value(
        'AcPowerSetPoint', 100, only_set_if_deviation_to_current_setting=True
    )
    assert result == 1
    mqtt_client.publish.assert_called_once()
    kwargs = mqtt_client.publish.call_args.kwargs
    assert kwargs['topic'] == 'W/vrm123/settings/0/Settings/CGwacs/AcPowerSetPoint'
    assert json.loads(kwargs['payload']) == {'value': 100}


def test_set_ccgx_value_skips_when_unchanged(output, mqtt_client):
    result = output.set_ccgx_value(
        'AcPowerSetPoint', 0, only_set_if_deviation_to_current_setting=True
    )
    assert result == 0
    mqtt_client.publish.assert_not_called()


def test_set_ccgx_value_force_publish_even_if_same(output, mqtt_client):
    result = output.set_ccgx_value(
        'AcPowerSetPoint', 0, only_set_if_deviation_to_current_setting=False
    )
    assert result == 1
    mqtt_client.publish.assert_called_once()


def test_set_ccgx_value_none_name_returns_error(output, mqtt_client):
    assert output.set_ccgx_value(None, 1) == -1
    mqtt_client.publish.assert_not_called()


def test_set_ccgx_value_no_mqtt_client(output, mqtt_client):
    output.mqtt_client = None
    assert output.set_ccgx_value('AcPowerSetPoint', 10) == -1


def test_set_ccgx_value_unknown_in_list_returns_error(output, mqtt_client, ccgx_data):
    ccgx_data['settings']['DoesNotExist'] = 1
    assert output.set_ccgx_value('DoesNotExist', 2) == -1


def test_set_ccgx_value_missing_setting_without_base_path(output, mqtt_client, ccgx_data):
    del ccgx_data['settings_base_path']
    del ccgx_data['settings']['AcPowerSetPoint']
    assert output.set_ccgx_value('AcPowerSetPoint', 5) == 0
    mqtt_client.publish.assert_not_called()


def test_set_ccgx_value_cold_start_with_base_path(output, mqtt_client, ccgx_data):
    """Publish even if current settings value unknown, when base path known."""
    del ccgx_data['settings']['AcPowerSetPoint']
    result = output.set_ccgx_value('AcPowerSetPoint', 7)
    assert result == 1
    mqtt_client.publish.assert_called_once()


def test_set_multis_switch_publishes_on_change(output, mqtt_client):
    published = output.set_multis_switch_mode(4)
    assert published is True
    kwargs = mqtt_client.publish.call_args.kwargs
    assert kwargs['topic'] == 'W/vrm123/vebus/276/Mode'
    assert json.loads(kwargs['payload']) == {'value': 4}


def test_set_multis_switch_skips_when_same(output, mqtt_client):
    assert output.set_multis_switch_mode(3) is False
    mqtt_client.publish.assert_not_called()


def test_set_multis_switch_no_vebus(output, mqtt_client, ccgx_data):
    del ccgx_data['vebus']
    assert output.set_multis_switch_mode(1) is False


def test_set_multis_switch_no_client(output):
    output.mqtt_client = None
    assert output.set_multis_switch_mode(4) is False


def test_set_mqtt_client_updates_reference(output):
    new_client = MagicMock()
    output.set_mqtt_client(new_client)
    output.set_ccgx_value(
        'MaxChargeCurrent', 10, only_set_if_deviation_to_current_setting=True
    )
    new_client.publish.assert_called_once()


def test_send_keepalive_selected_topics(output, mqtt_client):
    ok = output.send_keepalive('VRM123', 0)
    assert ok is True
    mqtt_client.publish.assert_called_once()
    args = mqtt_client.publish.call_args
    assert args.args[0] == 'R/VRM123/keepalive'
    assert 'battery/+/Soc' in json.loads(args.args[1])


def test_send_keepalive_all_topics_mode(output, mqtt_client):
    ok = output.send_keepalive('VRM123', 1)
    assert ok is True
    call = mqtt_client.publish.call_args
    assert call.args[0] == 'R/VRM123/system/0/Serial'
    assert call.kwargs.get('qos') == 0
    assert call.kwargs.get('payload') == ''


def test_send_keepalive_invalid_mode(output, mqtt_client):
    assert output.send_keepalive('VRM123', 99) is False
    mqtt_client.publish.assert_not_called()


def test_send_keepalive_no_client(output):
    output.mqtt_client = None
    assert output.send_keepalive('VRM123', 0) is False
