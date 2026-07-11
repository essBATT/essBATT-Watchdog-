"""Lightweight tests for watchdog_cycle_update (mocked collaborators)."""

from unittest.mock import MagicMock

from essBATT_watchdog import essBATT_watchdog


def _bare_watchdog():
    """Build instance without running full __init__ (no disk / MQTT)."""
    wd = essBATT_watchdog.__new__(essBATT_watchdog)
    wd.logger = MagicMock()
    wd.mqtt_bridge = MagicMock()
    wd.mqtt_bridge.is_connected = True
    wd.data_mapper = MagicMock()
    wd.failure_monitor = MagicMock()
    wd.battery_watch = MagicMock()
    wd.safe_state = MagicMock()
    wd.safe_state.active = False
    wd.safe_state.last_reason = ''
    wd.notifier = MagicMock()
    wd.CCGX_data = {'battery': {}, 'grid': {}}
    return wd


def test_cycle_skips_when_mqtt_disconnected():
    wd = _bare_watchdog()
    wd.mqtt_bridge.is_connected = False
    wd.watchdog_cycle_update()
    wd.failure_monitor.evaluate.assert_not_called()


def test_cycle_enters_safe_state_on_new_timeout():
    wd = _bare_watchdog()
    wd.failure_monitor.evaluate.return_value = {
        'newly_failed': ['controller_timeout (no heartbeat for 100s)'],
        'any_failure': True,
    }
    wd.battery_watch.evaluate.return_value = {
        'active': [],
        'critical_active': [],
        'newly_critical': [],
    }

    wd.watchdog_cycle_update()

    wd.safe_state.enter.assert_called_once()
    assert 'controller_timeout' in wd.safe_state.enter.call_args.kwargs['reason']
    wd.notifier.notify_alert.assert_called_once()
    assert wd.notifier.notify_alert.call_args.kwargs['severity'] == 'critical'


def test_cycle_enters_safe_state_on_battery_critical():
    wd = _bare_watchdog()
    wd.failure_monitor.evaluate.return_value = {
        'newly_failed': [],
        'any_failure': False,
    }
    wd.battery_watch.evaluate.return_value = {
        'active': ['cell_voltage_too_high'],
        'critical_active': ['cell_voltage_too_high'],
        'newly_critical': ['cell_voltage_too_high'],
    }

    wd.watchdog_cycle_update()

    wd.safe_state.enter.assert_called_once()
    assert 'battery_critical' in wd.safe_state.enter.call_args.kwargs['reason']
    # Battery notify is done inside battery_watch; cycle does not re-notify failure
    wd.notifier.notify_alert.assert_not_called()


def test_cycle_reasserts_while_critical_holds():
    wd = _bare_watchdog()
    wd.safe_state.active = True
    wd.safe_state.last_reason = 'battery_critical (x)'
    wd.failure_monitor.evaluate.return_value = {
        'newly_failed': [],
        'any_failure': False,
    }
    wd.battery_watch.evaluate.return_value = {
        'active': ['pack_voltage_too_low'],
        'critical_active': ['pack_voltage_too_low'],
        'newly_critical': [],
    }

    wd.watchdog_cycle_update()

    wd.safe_state.enter.assert_called_once_with(reason='battery_critical (x)')
    wd.safe_state.clear.assert_not_called()


def test_cycle_clears_when_all_healthy():
    wd = _bare_watchdog()
    wd.safe_state.active = True
    wd.failure_monitor.evaluate.return_value = {
        'newly_failed': [],
        'any_failure': False,
    }
    wd.battery_watch.evaluate.return_value = {
        'active': [],
        'critical_active': [],
        'newly_critical': [],
    }

    wd.watchdog_cycle_update()

    wd.safe_state.clear.assert_called_once()
    wd.notifier.notify_recovery.assert_called_once()


def test_cycle_exception_enters_safe_state():
    wd = _bare_watchdog()
    wd.data_mapper.read_values_to_local_dict.side_effect = RuntimeError('boom')

    wd.watchdog_cycle_update()

    wd.safe_state.enter.assert_called()
    assert wd.safe_state.enter.call_args.kwargs['reason'] == 'watchdog cycle exception'
    wd.notifier.notify_alert.assert_called()
