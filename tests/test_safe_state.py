"""Tests for safe-state policy and SafeStateManager actions."""

from unittest.mock import MagicMock

from safe_state import SafeStateManager, resolve_safe_state_action


def _failure(newly=None, any_failure=False):
    return {
        'newly_failed': list(newly or []),
        'any_failure': any_failure,
    }


def _battery(newly_critical=None, critical_active=None):
    return {
        'newly_critical': list(newly_critical or []),
        'critical_active': list(critical_active or []),
        'active': list(critical_active or []),
    }


def test_enter_on_new_controller_timeout():
    d = resolve_safe_state_action(
        _failure(newly=['controller_timeout'], any_failure=True),
        _battery(),
        safe_state_active=False,
    )
    assert d['action'] == 'enter'
    assert 'controller_timeout' in d['reason']
    assert d['notify_failure'] is True


def test_enter_on_new_battery_critical():
    d = resolve_safe_state_action(
        _failure(),
        _battery(
            newly_critical=['cell_voltage_too_high'],
            critical_active=['cell_voltage_too_high'],
        ),
        safe_state_active=False,
    )
    assert d['action'] == 'enter'
    assert 'battery_critical' in d['reason']
    assert 'cell_voltage_too_high' in d['reason']
    assert d['notify_failure'] is False


def test_warning_battery_does_not_enter():
    d = resolve_safe_state_action(
        _failure(),
        {
            'newly_critical': [],
            'critical_active': [],
            'active': ['battery_too_hot'],
        },
        safe_state_active=False,
    )
    assert d['action'] == 'none'


def test_reassert_while_critical_battery_holds():
    d = resolve_safe_state_action(
        _failure(),
        _battery(critical_active=['pack_voltage_too_low']),
        safe_state_active=True,
    )
    assert d['action'] == 'reassert'
    assert d['notify_failure'] is False


def test_hold_safe_when_timeout_clears_but_battery_critical():
    """Do not clear safe state if critical battery still active."""
    d = resolve_safe_state_action(
        _failure(any_failure=False),
        _battery(critical_active=['cell_voltage_too_low']),
        safe_state_active=True,
    )
    assert d['action'] == 'reassert'


def test_hold_safe_when_battery_clears_but_timeout_remains():
    d = resolve_safe_state_action(
        _failure(any_failure=True),
        _battery(),
        safe_state_active=True,
    )
    assert d['action'] == 'reassert'


def test_clear_only_when_both_healthy():
    d = resolve_safe_state_action(
        _failure(any_failure=False),
        _battery(),
        safe_state_active=True,
    )
    assert d['action'] == 'clear'


def test_combined_new_failure_and_battery_critical():
    d = resolve_safe_state_action(
        _failure(newly=['ccgx_timeout'], any_failure=True),
        _battery(
            newly_critical=['pack_voltage_too_low'],
            critical_active=['pack_voltage_too_low'],
        ),
        safe_state_active=False,
    )
    assert d['action'] == 'enter'
    assert 'ccgx_timeout' in d['reason']
    assert 'battery_critical' in d['reason']
    assert d['notify_failure'] is True


def _manager(config=None, victron=None):
    cfg = {
        'switch_off_multis_if_failure_is_detected': 1,
        'ac_power_set_point_if_failure_is_detected': 5,
        'zero_charge_discharge_limits_if_failure_is_detected': 1,
    }
    if config:
        cfg.update(config)
    vo = victron or MagicMock()
    vo.set_ccgx_value.return_value = 1
    vo.set_multis_switch_mode.return_value = True
    return SafeStateManager(cfg, MagicMock(), vo), vo


def test_enter_applies_setpoints_zero_limits_and_multis_off():
    mgr, vo = _manager()
    attempted = mgr.enter(reason='controller_timeout')
    assert attempted is True
    assert mgr.active is True
    assert mgr.last_reason == 'controller_timeout'

    names = [
        c.kwargs['set_val_name_str'] for c in vo.set_ccgx_value.call_args_list
    ]
    assert 'AcPowerSetPoint' in names
    assert 'MaxChargeCurrent' in names
    assert 'MaxDischargePower' in names
    vo.set_multis_switch_mode.assert_called_once_with(4)


def test_enter_skips_disabled_actions():
    mgr, vo = _manager({
        'switch_off_multis_if_failure_is_detected': 0,
        'ac_power_set_point_if_failure_is_detected': None,
        'zero_charge_discharge_limits_if_failure_is_detected': 0,
    })
    # force None for ac setpoint (update_config reads key)
    mgr.ac_power_setpoint = None
    mgr.zero_charge_discharge = False
    mgr.switch_off_multis = False
    attempted = mgr.enter(reason='test')
    assert attempted is False
    vo.set_ccgx_value.assert_not_called()
    vo.set_multis_switch_mode.assert_not_called()
    assert mgr.active is True


def test_clear_resets_flag_without_hardware_restore():
    mgr, vo = _manager()
    mgr.enter(reason='x')
    vo.reset_mock()
    mgr.clear(reason='recovered')
    assert mgr.active is False
    assert mgr.last_reason == ''
    vo.set_ccgx_value.assert_not_called()
    vo.set_multis_switch_mode.assert_not_called()


def test_clear_when_already_inactive_is_noop():
    mgr, vo = _manager()
    mgr.clear()
    assert mgr.active is False
