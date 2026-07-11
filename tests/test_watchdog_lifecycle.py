"""Lifecycle / plumbing tests for essBATT_watchdog.

Covers composition-root paths that cycle tests skip:
__init__ early exits, run() error paths, stop/timers, keepalive gates,
disconnect warnings, MQTT connect hook. No real broker or threads.
"""

import copy
import signal
from unittest.mock import MagicMock, patch

import constants
from config_manager import ConfigManager
from essBATT_watchdog import essBATT_watchdog


class _DummyTimer:
    def __init__(self, interval, function, *args, **kwargs):
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def start(self):
        self.is_running = True

    def stop(self):
        self.is_running = False


def _base_config(**overrides):
    cfg = {
        'config_version': 2.1,
        'vrm_id': 'test_vrm_id',
        'debug_level': 'DEBUG',
        'mqtt_username': 'test',
        'mqtt_password': 'test',
        'mqtt_server_COM_port': 1883,
        'watchdog_update_rate': 2.0,
        'script_alive_logging_interval': 86400,
        'keepalive_get_all_topics': 0,
        'controller_heartbeat_topic': 'essbatt/controller/heartbeat',
        'essBATT_controller_timeout_detection_duration': 95,
        'victron_CCGX_timeout_detection_duration': 60,
        'send_keep_alive_if_essBATT_controller_is_down': 1,
        'switch_off_multis_if_failure_is_detected': 1,
        'ac_power_set_point_if_failure_is_detected': 5,
        'zero_charge_discharge_limits_if_failure_is_detected': 1,
        'battery_watch': {
            'cell_voltage_too_high': {'threshold': 3.6, 'level': 'critical'},
        },
        'notifications': {
            'mail': {'enabled': 0},
            'telegram': {'enabled': 0},
            'pushover': {'enabled': 0},
        },
    }
    cfg.update(overrides)
    return cfg


_SETVALUE = {
    'AcPowerSetPoint': 'CGwacs/AcPowerSetPoint',
    'MaxChargeCurrent': 'SystemSetup/MaxChargeCurrent',
    'MaxDischargePower': 'CGwacs/MaxDischargePower',
}


def _build_watchdog(
    *,
    config=None,
    setvalue=None,
    config_ok=True,
    setvalue_ok=True,
):
    """Construct watchdog with patched ConfigManager + DummyTimer + fake MQTT."""
    config = copy.deepcopy(config if config is not None else _base_config())
    setvalue = copy.deepcopy(setvalue if setvalue is not None else _SETVALUE)
    logger = MagicMock()

    def load_config(self):
        self.config_data_loaded_correctly = config_ok
        return copy.deepcopy(config) if config_ok else {}

    def load_setvalue_list(self):
        self.setvalue_list_loaded_correctly = setvalue_ok
        return copy.deepcopy(setvalue) if setvalue_ok else {}

    with patch('essBATT_watchdog.RepeatedTimer', _DummyTimer), \
         patch.object(ConfigManager, 'load_config', load_config), \
         patch.object(ConfigManager, 'load_setvalue_list', load_setvalue_list), \
         patch('essBATT_watchdog.MqttBridge') as MockBridge:
        bridge = MagicMock()
        bridge.is_connected = True
        bridge.client = MagicMock()
        bridge.disconnect_duration_s.return_value = 0.0
        MockBridge.return_value = bridge
        wd = essBATT_watchdog(logger)
        wd.mqtt_bridge = bridge
    return wd, logger, bridge


def _bare_watchdog(**attrs):
    """Minimal instance without full __init__."""
    wd = essBATT_watchdog.__new__(essBATT_watchdog)
    wd.logger = MagicMock()
    wd._running = False
    wd._timers_started = False
    wd._last_disconnect_warn_at = None
    wd.watchdog_config_data = _base_config()
    wd.mqtt_bridge = MagicMock()
    wd.mqtt_bridge.is_connected = True
    wd.mqtt_bridge.client = MagicMock()
    wd.mqtt_bridge.disconnect_duration_s.return_value = 0.0
    wd.victron_output = MagicMock()
    wd.failure_monitor = MagicMock()
    wd.failure_monitor.controller_failed = False
    wd.failure_monitor.ccgx_failed = False
    wd.safe_state = MagicMock()
    wd.safe_state.active = False
    wd.rt_keep_alive_obj = None
    wd.rt_watchdog_update_obj = None
    wd.rt_print_status_obj = None
    for k, v in attrs.items():
        setattr(wd, k, v)
    return wd


# --- __init__ ---

def test_init_wires_domain_services():
    wd, logger, bridge = _build_watchdog()
    assert wd.watchdog_config_data_loaded_correctly is True
    assert wd.ess_setvalue_list_loaded_correctly is True
    assert wd.write_base_path == 'W/test_vrm_id/'
    assert wd.failure_monitor is not None
    assert wd.notifier is not None
    assert wd.battery_watch is not None
    assert wd.safe_state is not None
    assert wd.mqtt_bridge is bridge
    logger.setLevel.assert_called()


def test_init_stops_when_config_load_fails():
    wd, _, _ = _build_watchdog(config_ok=False)
    assert wd.watchdog_config_data_loaded_correctly is False
    assert not hasattr(wd, 'failure_monitor')


def test_init_stops_when_setvalue_load_fails():
    wd, _, _ = _build_watchdog(setvalue_ok=False)
    assert wd.watchdog_config_data_loaded_correctly is True
    assert wd.ess_setvalue_list_loaded_correctly is False
    assert not hasattr(wd, 'failure_monitor')


# --- run / stop ---

def test_run_handles_mqtt_timeout():
    wd, logger, bridge = _build_watchdog()
    bridge.start.side_effect = TimeoutError('no connack')
    wd.run()
    assert wd._running is False
    logger.error.assert_called()
    assert 'timeout' in logger.error.call_args.args[0].lower()


def test_run_handles_oserror():
    wd, logger, bridge = _build_watchdog()
    bridge.start.side_effect = OSError('network down')
    wd.run()
    assert wd._running is False
    logger.error.assert_called()


def test_run_handles_unexpected_exception():
    wd, logger, bridge = _build_watchdog()
    bridge.start.side_effect = RuntimeError('boom')
    wd.run()
    assert wd._running is False
    logger.exception.assert_called()


def test_run_loop_exits_when_running_cleared():
    wd, _, bridge = _build_watchdog()

    def start_and_stop(**kwargs):
        wd._running = False

    bridge.start.side_effect = start_and_stop
    with patch('essBATT_watchdog.time.sleep') as sleep:
        # If loop runs, stop after first sleep would keep spinning — start clears flag
        wd.run()
    bridge.start.assert_called_once()
    sleep.assert_not_called()


def test_run_loop_calls_disconnect_warn_then_exits():
    wd, _, bridge = _build_watchdog()
    calls = {'n': 0}

    def start(**kwargs):
        pass

    def sleep(_s):
        calls['n'] += 1
        if calls['n'] >= 1:
            wd._running = False

    bridge.start.side_effect = start
    with patch('essBATT_watchdog.time.sleep', side_effect=sleep), \
         patch.object(wd, '_maybe_warn_long_disconnect') as warn, \
         patch.object(wd, '_install_signal_handlers'):
        wd.run()
    warn.assert_called()
    assert calls['n'] == 1


def test_stop_stops_timers_and_mqtt():
    wd = _bare_watchdog()
    t1, t2, t3 = MagicMock(), MagicMock(), MagicMock()
    wd.rt_keep_alive_obj = t1
    wd.rt_watchdog_update_obj = t2
    wd.rt_print_status_obj = t3
    wd._timers_started = True
    wd._running = True

    wd.stop()

    assert wd._running is False
    t1.stop.assert_called_once()
    t2.stop.assert_called_once()
    t3.stop.assert_called_once()
    wd.mqtt_bridge.stop.assert_called_once()
    assert wd._timers_started is False


def test_stop_timer_exception_is_logged():
    wd = _bare_watchdog()
    bad = MagicMock()
    bad.stop.side_effect = RuntimeError('timer dead')
    wd.rt_keep_alive_obj = bad
    wd.stop()
    wd.logger.exception.assert_called()


def test_stop_without_mqtt_bridge_attr():
    wd = _bare_watchdog()
    del wd.mqtt_bridge
    wd.stop()
    assert wd._running is False


# --- signals / timers / mqtt hook ---

def test_install_signal_handlers_sets_running_false():
    wd = _bare_watchdog()
    wd._running = True
    handlers = {}

    def fake_signal(sig, handler):
        handlers[sig] = handler

    with patch('essBATT_watchdog.signal.signal', side_effect=fake_signal):
        wd._install_signal_handlers()

    assert signal.SIGTERM in handlers
    assert signal.SIGINT in handlers
    handlers[signal.SIGTERM](signal.SIGTERM, None)
    assert wd._running is False
    wd.logger.warning.assert_called()


def test_signal_handler_falls_back_when_signum_unknown():
    wd = _bare_watchdog()
    wd._running = True
    handlers = {}

    def fake_signal(sig, handler):
        handlers[sig] = handler

    with patch('essBATT_watchdog.signal.signal', side_effect=fake_signal):
        wd._install_signal_handlers()

    # Force Signals(signum).name to fail → str(signum) path
    with patch('essBATT_watchdog.signal.Signals', side_effect=ValueError('bad')):
        handlers[signal.SIGINT](99, None)
    assert wd._running is False
    assert '99' in wd.logger.warning.call_args.args[0]


def test_install_signal_handlers_tolerates_value_error():
    wd = _bare_watchdog()

    def boom(sig, handler):
        raise ValueError('not main thread')

    with patch('essBATT_watchdog.signal.signal', side_effect=boom):
        wd._install_signal_handlers()
    wd.logger.debug.assert_called()


def test_start_timers_idempotent():
    wd = _bare_watchdog()
    with patch('essBATT_watchdog.RepeatedTimer', _DummyTimer):
        wd._start_timers()
        first = wd.rt_watchdog_update_obj
        wd._start_timers()
        assert wd.rt_watchdog_update_obj is first
    assert wd._timers_started is True
    assert isinstance(wd.rt_keep_alive_obj, _DummyTimer)
    assert wd.rt_watchdog_update_obj.interval == 2.0


def test_on_mqtt_connected_starts_timers_and_sets_client():
    wd = _bare_watchdog()
    client = MagicMock()
    wd.mqtt_bridge.client = client
    with patch.object(wd, '_start_timers') as start_timers, \
         patch.object(wd, 'send_keepalive_to_cerbo') as ka:
        wd._on_mqtt_connected(is_reconnect=False)
    wd.victron_output.set_mqtt_client.assert_called_once_with(client)
    ka.assert_called_once()
    start_timers.assert_called_once()


def test_on_mqtt_connected_reconnect_logs_and_skips_timer_restart():
    wd = _bare_watchdog()
    wd._timers_started = True
    with patch.object(wd, '_start_timers') as start_timers, \
         patch.object(wd, 'send_keepalive_to_cerbo'):
        wd._on_mqtt_connected(is_reconnect=True)
    start_timers.assert_not_called()
    wd.logger.info.assert_called()
    assert 'restored' in wd.logger.info.call_args.args[0].lower() or \
        'MQTT session restored' in wd.logger.info.call_args.args[0]


# --- disconnect warn ---

def test_maybe_warn_clears_when_connected():
    wd = _bare_watchdog()
    wd._last_disconnect_warn_at = 123.0
    wd.mqtt_bridge.is_connected = True
    wd._maybe_warn_long_disconnect()
    assert wd._last_disconnect_warn_at is None
    wd.logger.warning.assert_not_called()


def test_maybe_warn_skips_short_disconnect():
    wd = _bare_watchdog()
    wd.mqtt_bridge.is_connected = False
    wd.mqtt_bridge.disconnect_duration_s.return_value = 5.0
    wd._maybe_warn_long_disconnect()
    wd.logger.warning.assert_not_called()


def test_maybe_warn_logs_after_threshold():
    wd = _bare_watchdog()
    wd.mqtt_bridge.is_connected = False
    wd.mqtt_bridge.disconnect_duration_s.return_value = (
        constants.MQTT_DISCONNECT_WARN_AFTER_S + 1
    )
    with patch('essBATT_watchdog.time.time', return_value=5000.0):
        wd._maybe_warn_long_disconnect()
    wd.logger.warning.assert_called_once()
    assert wd._last_disconnect_warn_at == 5000.0


def test_maybe_warn_rate_limited():
    wd = _bare_watchdog()
    wd.mqtt_bridge.is_connected = False
    wd.mqtt_bridge.disconnect_duration_s.return_value = 100.0
    wd._last_disconnect_warn_at = 1000.0
    with patch('essBATT_watchdog.time.time', return_value=1000.0 + 10.0):
        wd._maybe_warn_long_disconnect()
    wd.logger.warning.assert_not_called()


# --- keepalive ---

def test_keepalive_skipped_when_disconnected():
    wd = _bare_watchdog()
    wd.mqtt_bridge.is_connected = False
    wd.send_keepalive_to_cerbo()
    wd.victron_output.send_keepalive.assert_not_called()


def test_keepalive_skipped_when_controller_alive():
    wd = _bare_watchdog()
    wd.failure_monitor.controller_failed = False
    wd.watchdog_config_data['send_keep_alive_if_essBATT_controller_is_down'] = 1
    wd.watchdog_config_data['controller_heartbeat_topic'] = 'essbatt/controller/heartbeat'
    wd.send_keepalive_to_cerbo()
    wd.victron_output.send_keepalive.assert_not_called()


def test_keepalive_sent_when_controller_failed():
    wd = _bare_watchdog()
    wd.failure_monitor.controller_failed = True
    wd.send_keepalive_to_cerbo()
    wd.victron_output.send_keepalive.assert_called_once_with('test_vrm_id', 0)


def test_keepalive_always_when_heartbeat_disabled():
    wd = _bare_watchdog()
    wd.failure_monitor.controller_failed = False
    wd.watchdog_config_data['controller_heartbeat_topic'] = 'none'
    wd.send_keepalive_to_cerbo()
    wd.victron_output.send_keepalive.assert_called_once()


def test_keepalive_always_when_gate_disabled():
    wd = _bare_watchdog()
    wd.failure_monitor.controller_failed = False
    wd.watchdog_config_data['send_keep_alive_if_essBATT_controller_is_down'] = 0
    wd.send_keepalive_to_cerbo()
    wd.victron_output.send_keepalive.assert_called_once()


# --- alive log ---

def test_print_alive_status():
    wd = _bare_watchdog()
    wd.mqtt_bridge.is_connected = False
    wd.safe_state.active = True
    wd.failure_monitor.controller_failed = True
    wd.failure_monitor.ccgx_failed = False
    wd.print_alive_status_to_logger()
    msg = wd.logger.info.call_args.args[0]
    assert 'DISCONNECTED' in msg
    assert 'ACTIVE' in msg
    assert 'True' in msg


# --- nested cycle failure ---

def test_cycle_exception_when_safe_state_also_fails():
    from unittest.mock import MagicMock as MM
    wd = essBATT_watchdog.__new__(essBATT_watchdog)
    wd.logger = MagicMock()
    wd.mqtt_bridge = MagicMock()
    wd.mqtt_bridge.is_connected = True
    wd.data_mapper = MagicMock()
    wd.data_mapper.read_values_to_local_dict.side_effect = RuntimeError('map')
    wd.safe_state = MagicMock()
    wd.safe_state.enter.side_effect = RuntimeError('safe fail')
    wd.notifier = MagicMock()
    wd.failure_monitor = MagicMock()
    wd.battery_watch = MagicMock()
    wd.CCGX_data = {}

    wd.watchdog_cycle_update()
    # Outer exception + inner failure to enter safe state
    assert wd.logger.exception.call_count >= 2
