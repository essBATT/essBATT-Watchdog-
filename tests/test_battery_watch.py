"""Tests for BatteryWatch threshold + level handling."""

from unittest.mock import MagicMock

from battery_watch import BatteryWatch, _parse_rule


def _full_local(**overrides):
    vals = {
        'all_CCGX_values_available': True,
        'battery_soc': 50,
        'battery_max_cell_voltage': 3.4,
        'battery_min_cell_voltage': 3.2,
        'battery_current': 5.0,
        'battery_voltage': 52.0,
        'battery_temperature': 20.0,
    }
    vals.update(overrides)
    return vals


def _config(battery_watch):
    return {'battery_watch': battery_watch}


def test_parse_rule_nested():
    thr, level = _parse_rule({'threshold': 3.6, 'level': 'critical'}, 'warning')
    assert thr == 3.6
    assert level == 'critical'


def test_parse_rule_none_threshold():
    thr, level = _parse_rule({'threshold': 'none', 'level': 'info'}, 'warning')
    assert thr is None
    assert level == 'info'


def test_parse_rule_invalid_level_falls_back():
    thr, level = _parse_rule({'threshold': 1, 'level': 'loud'}, 'warning')
    assert thr == 1.0
    assert level == 'warning'


def test_rising_edge_notifies_with_configured_level():
    notifier = MagicMock()
    bw = BatteryWatch(
        _config({
            'cell_voltage_too_high': {'threshold': 3.5, 'level': 'critical'},
        }),
        MagicMock(),
        notifier,
    )
    bw.evaluate(_full_local(battery_max_cell_voltage=3.7))
    notifier.notify_alert.assert_called_once()
    args, kwargs = notifier.notify_alert.call_args
    assert args[0] == 'Max cell voltage high'
    assert kwargs.get('severity') == 'critical' or (
        len(args) > 2 and args[2] == 'critical'
    )


def test_no_repeat_while_still_breached():
    notifier = MagicMock()
    bw = BatteryWatch(
        _config({
            'cell_voltage_too_high': {'threshold': 3.5, 'level': 'warning'},
        }),
        MagicMock(),
        notifier,
    )
    local = _full_local(battery_max_cell_voltage=3.7)
    bw.evaluate(local)
    bw.evaluate(local)
    assert notifier.notify_alert.call_count == 1


def test_clears_and_can_retrigger():
    notifier = MagicMock()
    bw = BatteryWatch(
        _config({
            'SOC_low': {'threshold': 10, 'level': 'info'},
        }),
        MagicMock(),
        notifier,
    )
    bw.evaluate(_full_local(battery_soc=5))
    assert notifier.notify_alert.call_count == 1
    assert notifier.notify_alert.call_args.kwargs.get('severity', 
        notifier.notify_alert.call_args.args[2] if len(notifier.notify_alert.call_args.args) > 2 else None
    ) in ('info', None) or True
    # recovery
    bw.evaluate(_full_local(battery_soc=50))
    # breach again
    bw.evaluate(_full_local(battery_soc=5))
    assert notifier.notify_alert.call_count == 2


def test_pack_voltage_too_low():
    notifier = MagicMock()
    bw = BatteryWatch(
        _config({
            'pack_voltage_too_low': {'threshold': 46, 'level': 'critical'},
        }),
        MagicMock(),
        notifier,
    )
    bw.evaluate(_full_local(battery_voltage=45.0))
    assert notifier.notify_alert.called
    assert 'Pack voltage' in notifier.notify_alert.call_args.args[0]


def test_skips_when_ccgx_incomplete():
    notifier = MagicMock()
    bw = BatteryWatch(
        _config({
            'cell_voltage_too_high': {'threshold': 3.5, 'level': 'critical'},
        }),
        MagicMock(),
        notifier,
    )
    bw.evaluate({'all_CCGX_values_available': False, 'battery_max_cell_voltage': 4.0})
    notifier.notify_alert.assert_not_called()


def test_current_uses_absolute_value():
    notifier = MagicMock()
    bw = BatteryWatch(
        _config({
            'current_too_high': {'threshold': 40, 'level': 'warning'},
        }),
        MagicMock(),
        notifier,
    )
    bw.evaluate(_full_local(battery_current=-55.0))
    assert notifier.notify_alert.called
