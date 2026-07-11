"""Tests for CcgxDataMapper (watchdog subset of fields)."""

from unittest.mock import MagicMock

from data_mapper import CcgxDataMapper


def _full_ccgx():
    return {
        'grid': {'grid_power_sum': 100},
        'battery': {
            'soc': 55,
            'max_cell_voltage': 3.40,
            'min_cell_voltage': 3.28,
            'current': 3.5,
            'power': 180,
            'voltage': 52.0,
            'temperature': 22.0,
        },
    }


def test_full_data_maps_and_marks_available():
    mapper = CcgxDataMapper(MagicMock())
    local = {}
    mapper.read_values_to_local_dict(_full_ccgx(), local)

    assert local['all_CCGX_values_available'] is True
    assert local['battery_soc'] == 55
    assert local['battery_max_cell_voltage'] == 3.40
    assert local['battery_min_cell_voltage'] == 3.28
    assert local['battery_current'] == 3.5
    assert local['battery_voltage'] == 52.0
    assert local['battery_power'] == 180
    assert local['battery_temperature'] == 22.0
    assert local['grid_power_sum'] == 100


def test_missing_battery_soc_marks_incomplete():
    data = _full_ccgx()
    del data['battery']['soc']
    local = {}
    CcgxDataMapper(MagicMock()).read_values_to_local_dict(data, local)
    assert local['all_CCGX_values_available'] is False
    assert 'battery_soc' not in local


def test_missing_temperature_marks_incomplete():
    data = _full_ccgx()
    del data['battery']['temperature']
    local = {}
    CcgxDataMapper(MagicMock()).read_values_to_local_dict(data, local)
    assert local['all_CCGX_values_available'] is False


def test_missing_grid_is_ok():
    data = _full_ccgx()
    del data['grid']
    local = {}
    CcgxDataMapper(MagicMock()).read_values_to_local_dict(data, local)
    assert local['all_CCGX_values_available'] is True
    assert 'grid_power_sum' not in local


def test_empty_battery_incomplete():
    local = {}
    CcgxDataMapper(MagicMock()).read_values_to_local_dict({'battery': {}}, local)
    assert local['all_CCGX_values_available'] is False
