"""Tests for CcgxIngestion MQTT payload handlers."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ccgx_ingestion import CcgxIngestion


def _msg(topic, value=None, raw_payload=None):
    if raw_payload is not None:
        payload = raw_payload
    else:
        payload = json.dumps({'value': value}).encode('utf-8')
    return SimpleNamespace(topic=topic, payload=payload)


@pytest.fixture
def ccgx_data():
    return {
        'grid': {},
        'battery': {},
        'solarcharger': {},
        'settings': {},
        'system': {},
    }


@pytest.fixture
def activity():
    return MagicMock()


@pytest.fixture
def ingestion(ccgx_data, activity):
    return CcgxIngestion(MagicMock(), ccgx_data, on_activity=activity)


def test_battery_fields_and_activity(ingestion, ccgx_data, activity):
    ingestion.on_battery_soc(_msg('N/vrm/battery/0/Soc', 67.5))
    ingestion.on_battery_maxcellvoltage(
        _msg('N/vrm/battery/0/System/MaxCellVoltage', 3.45)
    )
    ingestion.on_battery_mincellvoltage(
        _msg('N/vrm/battery/0/System/MinCellVoltage', 3.22)
    )
    ingestion.on_battery_temp(_msg('N/vrm/battery/0/Dc/0/Temperature', 21.0))
    ingestion.on_battery_current(_msg('N/vrm/battery/0/Dc/0/Current', -5.0))
    ingestion.on_battery_power(_msg('N/vrm/battery/0/Dc/0/Power', -250))
    ingestion.on_battery_voltage(_msg('N/vrm/battery/0/Dc/0/Voltage', 51.2))

    assert ccgx_data['battery']['soc'] == 67.5
    assert ccgx_data['battery']['max_cell_voltage'] == 3.45
    assert ccgx_data['battery']['min_cell_voltage'] == 3.22
    assert ccgx_data['battery']['temperature'] == 21.0
    assert ccgx_data['battery']['current'] == -5.0
    assert ccgx_data['battery']['power'] == -250
    assert ccgx_data['battery']['voltage'] == 51.2
    assert activity.call_count == 7


def test_grid_phases(ingestion, ccgx_data):
    ingestion.on_grid_power(_msg('N/vrm/grid/30/Ac/Power', -120))
    ingestion.on_L1_power(_msg('N/vrm/grid/30/Ac/L1/Power', -40))
    ingestion.on_L1_current(_msg('N/vrm/grid/30/Ac/L1/Current', 1.1))
    ingestion.on_L2_power(_msg('N/vrm/grid/30/Ac/L2/Power', -50))
    ingestion.on_L2_current(_msg('N/vrm/grid/30/Ac/L2/Current', 1.2))
    ingestion.on_L3_power(_msg('N/vrm/grid/30/Ac/L3/Power', -30))
    ingestion.on_L3_current(_msg('N/vrm/grid/30/Ac/L3/Current', 1.0))
    assert ccgx_data['grid']['grid_power_sum'] == -120
    assert ccgx_data['grid']['L1_power'] == -40
    assert ccgx_data['grid']['L3_current'] == 1.0


def test_invalid_payload_clears_device(ingestion, ccgx_data):
    ccgx_data['battery']['soc'] = 50
    ingestion.on_battery_soc(_msg('N/vrm/battery/0/Soc', raw_payload=b'not-json'))
    assert ccgx_data['battery'] == {}


def test_solarcharger_power_and_dc(ingestion, ccgx_data):
    ingestion.on_solarcharger_power(
        _msg('N/vrm/solarcharger/279/Yield/Power', 400)
    )
    ingestion.on_solarcharger_dc_values(
        _msg('N/vrm/solarcharger/279/Dc/0/Current', 8.5)
    )
    assert ccgx_data['solarcharger']['279']['Power'] == 400
    assert ccgx_data['solarcharger']['279']['Current'] == 8.5


def test_solarcharger_removed_clears_only_that_instance(ingestion, ccgx_data):
    ccgx_data['solarcharger']['279'] = {'Power': 100}
    ccgx_data['solarcharger']['280'] = {'Power': 50}
    ingestion.on_solarcharger_power(
        _msg('N/vrm/solarcharger/279/Yield/Power', raw_payload=b'{}')
    )
    assert ccgx_data['solarcharger']['279'] == {}
    assert ccgx_data['solarcharger']['280']['Power'] == 50


def test_system_ac_consumption_power(ingestion, ccgx_data):
    ingestion.on_system_ac_consumption(
        _msg('N/vrm/system/0/Ac/Consumption/L1/Power', 220)
    )
    assert ccgx_data['system']['L1_loads_power_consumption'] == 220


def test_system_ac_consumption_ignores_number_of_phases(ingestion, ccgx_data):
    ingestion.on_system_ac_consumption(
        _msg('N/vrm/system/0/Ac/Consumption/NumberOfPhases/Power', 3)
    )
    assert 'NumberOfPhases_loads_power_consumption' not in ccgx_data['system']


def test_system_ac_consumption_malformed_topic(ingestion, ccgx_data):
    ingestion.on_system_ac_consumption(_msg('N/vrm/system/0/Ac', 1))
    assert ccgx_data['system'] == {}


def test_settings_cgwacs_sets_base_path(ingestion, ccgx_data):
    ingestion.on_settings_cgwacs(
        _msg('N/vrm/settings/0/Settings/CGwacs/AcPowerSetPoint', 0)
    )
    assert ccgx_data['settings']['AcPowerSetPoint'] == 0
    assert ccgx_data['settings_base_path'] == 'settings/0/Settings/'


def test_settings_system_setup(ingestion, ccgx_data):
    ingestion.on_settings_system_setup(
        _msg('N/vrm/settings/0/Settings/SystemSetup/MaxChargeCurrent', 50)
    )
    assert ccgx_data['settings']['MaxChargeCurrent'] == 50


def test_multis_switch_mode(ingestion, ccgx_data):
    ingestion.on_multis_switch_mode(_msg('N/vrm/vebus/276/Mode', 3))
    assert ccgx_data['vebus']['276']['Mode'] == 3


def test_parse_victron_mqtt_value_missing_value_key(ingestion):
    msg = _msg('N/vrm/x', raw_payload=b'{"other": 1}')
    assert ingestion.parse_victron_mqtt_value(msg) is None


def test_activity_callback_exception_is_swallowed(ccgx_data):
    bad = MagicMock(side_effect=RuntimeError('boom'))
    ing = CcgxIngestion(MagicMock(), ccgx_data, on_activity=bad)
    ing.on_battery_soc(_msg('N/vrm/battery/0/Soc', 10))
    assert ccgx_data['battery']['soc'] == 10


def test_set_on_activity_replaces_hook(ccgx_data):
    first = MagicMock()
    second = MagicMock()
    ing = CcgxIngestion(MagicMock(), ccgx_data, on_activity=first)
    ing.set_on_activity(second)
    ing.on_grid_power(_msg('N/vrm/grid/0/Ac/Power', 1))
    first.assert_not_called()
    second.assert_called_once()
