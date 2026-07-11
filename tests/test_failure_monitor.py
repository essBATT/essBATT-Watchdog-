"""Tests for FailureMonitor timeout detection."""

from unittest.mock import MagicMock

from failure_monitor import FailureMonitor


class FakeClock:
    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def _monitor(clock, **cfg):
    config = {
        'essBATT_controller_timeout_detection_duration': 95,
        'victron_CCGX_timeout_detection_duration': 60,
        'controller_heartbeat_topic': 'essbatt/controller/heartbeat',
    }
    config.update(cfg)
    return FailureMonitor(config, MagicMock(), time_fn=clock)


def test_no_failure_immediately_after_boot():
    clock = FakeClock()
    fm = _monitor(clock)
    status = fm.evaluate()
    assert status['any_failure'] is False
    assert status['newly_failed'] == []
    assert status['controller_failed'] is False
    assert status['ccgx_failed'] is False


def test_controller_timeout_reports_newly_failed():
    clock = FakeClock()
    fm = _monitor(clock)
    clock.advance(95)
    # Refresh CCGX after the wait so only the controller times out
    fm.note_ccgx_message()
    status = fm.evaluate()
    assert status['controller_failed'] is True
    assert status['ccgx_failed'] is False
    assert status['any_failure'] is True
    assert len(status['newly_failed']) == 1
    assert 'controller_timeout' in status['newly_failed'][0]


def test_controller_timeout_not_repeated_as_newly_failed():
    clock = FakeClock()
    fm = _monitor(clock)
    clock.advance(100)
    fm.note_ccgx_message()
    first = fm.evaluate()
    second = fm.evaluate()
    assert first['newly_failed']
    assert second['newly_failed'] == []
    assert second['controller_failed'] is True


def test_controller_heartbeat_clears_failure():
    clock = FakeClock()
    fm = _monitor(clock)
    clock.advance(100)
    fm.note_ccgx_message()
    fm.evaluate()
    assert fm.controller_failed is True

    fm.note_controller_heartbeat()
    status = fm.evaluate()
    assert status['controller_failed'] is False
    assert status['any_failure'] is False


def test_ccgx_timeout_and_activity_resume():
    clock = FakeClock()
    fm = _monitor(clock)
    clock.advance(60)
    status = fm.evaluate()
    assert status['ccgx_failed'] is True
    assert any('ccgx_timeout' in r for r in status['newly_failed'])

    fm.note_ccgx_message()
    status = fm.evaluate()
    assert status['ccgx_failed'] is False


def test_heartbeat_topic_none_disables_controller_check():
    clock = FakeClock()
    fm = _monitor(clock, controller_heartbeat_topic='none')
    clock.advance(200)
    status = fm.evaluate()
    assert status['controller_failed'] is False
    # CCGX timeout still active
    assert status['ccgx_failed'] is True


def test_both_timeouts_together():
    clock = FakeClock()
    fm = _monitor(clock)
    clock.advance(100)
    status = fm.evaluate()
    assert status['controller_failed'] is True
    assert status['ccgx_failed'] is True
    assert len(status['newly_failed']) == 2
