"""Tests for safe-state policy (timeouts + critical battery)."""

from safe_state import resolve_safe_state_action


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
