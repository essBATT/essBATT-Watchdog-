# This is free and unencumbered software released into the public domain.

# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.

# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# For more information, please refer to <http://unlicense.org/>

"""essBATT watchdog — composition root and monitor cycle.

Read this file top-down:
  1. ``essBATT_watchdog.__init__``  — wire modules together
  2. ``run`` / ``stop``             — process lifetime
  3. ``watchdog_cycle_update``      — one monitor tick

Domain modules (failure detection, safe state, notifications) live outside
this file. MQTT plumbing matches the controller's robust MqttBridge pattern.
"""

import logging
from logging.handlers import RotatingFileHandler
import signal
import time

import constants
from utils import RepeatedTimer
from config_manager import ConfigManager
from ccgx_ingestion import CcgxIngestion
from data_mapper import CcgxDataMapper
from victron_output import VictronOutput
from mqtt_bridge import MqttBridge
from failure_monitor import FailureMonitor
from safe_state import SafeStateManager
from notifier import Notifier
from battery_watch import BatteryWatch


class essBATT_watchdog:
    """Composition root + watchdog monitor cycle.

    Process lifetime follows ``self._running`` (signals / intentional stop),
    not momentary MQTT connectivity — short broker interruptions are
    tolerated while paho auto-reconnects and resubscribes.
    """

    # ==================================================================
    # Composition
    # ==================================================================

    def __init__(self, logger):
        self.logger = logger
        self._running = False
        self._timers_started = False
        self._last_disconnect_warn_at = None

        self.watchdog_config_data = {}
        self.ess_setvalue_list = {}
        self.watchdog_config_data_loaded_correctly = False
        self.ess_setvalue_list_loaded_correctly = False

        self.CCGX_data = {
            'grid': {}, 'battery': {}, 'solarcharger': {},
            'settings': {}, 'system': {},
        }

        self.config_manager = ConfigManager(self.logger, debug=constants.DEBUGGING_ON)

        self.watchdog_config_data = self.config_manager.load_config()
        self.watchdog_config_data_loaded_correctly = (
            self.config_manager.config_data_loaded_correctly
        )
        if not self.watchdog_config_data_loaded_correctly:
            return

        self.ess_setvalue_list = self.config_manager.load_setvalue_list()
        self.ess_setvalue_list_loaded_correctly = (
            self.config_manager.setvalue_list_loaded_correctly
        )
        if not self.ess_setvalue_list_loaded_correctly:
            return

        self.write_base_path = (
            'W/' + self.watchdog_config_data.get('vrm_id', 'unknown') + '/'
        )

        self.logger.setLevel(
            constants.LOGLEVEL_NAME_TO_NUMBER[
                self.watchdog_config_data.get('debug_level', 'INFO')
            ]
        )
        self.logger.info(
            'Effective logger level: ' + str(self.logger.getEffectiveLevel())
        )

        # Domain services
        self.failure_monitor = FailureMonitor(self.watchdog_config_data, self.logger)
        self.notifier = Notifier(self.watchdog_config_data, self.logger)
        self.data_mapper = CcgxDataMapper(self.logger)
        self.battery_watch = BatteryWatch(
            self.watchdog_config_data, self.logger, self.notifier
        )

        self.ingestion = CcgxIngestion(
            self.logger,
            self.CCGX_data,
            on_activity=self.failure_monitor.note_ccgx_message,
        )
        self.victron_output = VictronOutput(
            self.logger,
            mqtt_client=None,
            ccgx_data=self.CCGX_data,
            setvalue_list=self.ess_setvalue_list,
            write_base_path=self.write_base_path,
        )
        self.safe_state = SafeStateManager(
            self.watchdog_config_data, self.logger, self.victron_output
        )

        self.mqtt_bridge = MqttBridge(
            self.logger,
            self.watchdog_config_data,
            self.ingestion,
            on_connected=self._on_mqtt_connected,
            on_controller_heartbeat=self.failure_monitor.note_controller_heartbeat,
        )

        # Started after first successful MQTT connect
        self.rt_keep_alive_obj = None
        self.rt_watchdog_update_obj = None
        self.rt_print_status_obj = None

    # ==================================================================
    # MAIN LOGIC — process lifetime
    # ==================================================================

    def run(self):
        """Connect MQTT, then keep the process alive until stop/signal."""
        self._running = True
        self._install_signal_handlers()
        try:
            self.mqtt_bridge.start(
                connect_timeout=constants.MQTT_INITIAL_CONNECT_TIMEOUT_S
            )

            ####### MAIN WATCHDOG LOOP #############
            while self._running:
                self._maybe_warn_long_disconnect()
                time.sleep(1)
            ########################################

        except TimeoutError as e:
            self.logger.error('MQTT connection failed (timeout): ' + str(e))
            self._running = False
        except OSError as e:
            self.logger.error('MQTT connection failed (network/OS): ' + str(e))
            self._running = False
        except Exception:
            self.logger.exception('Unexpected error during MQTT setup / main loop')
            self._running = False

    def stop(self):
        """Stop background timers and MQTT (used on shutdown)."""
        self._running = False
        for timer_attr in (
            'rt_keep_alive_obj',
            'rt_watchdog_update_obj',
            'rt_print_status_obj',
        ):
            timer = getattr(self, timer_attr, None)
            if timer is not None:
                try:
                    timer.stop()
                except Exception:
                    self.logger.exception('Error stopping timer ' + timer_attr)
        if hasattr(self, 'mqtt_bridge') and self.mqtt_bridge is not None:
            self.mqtt_bridge.stop()
        self._timers_started = False

    # ==================================================================
    # MAIN LOGIC — watchdog cycle (called by timer while running)
    # ==================================================================

    def watchdog_cycle_update(self):
        """One monitor tick: map inputs → failure checks → safe state / notify.

        Skips work while MQTT is down. Domain exceptions are logged so the
        timer thread keeps living.
        """
        if not self.mqtt_bridge.is_connected:
            return

        try:
            local_values = {}
            self.data_mapper.read_values_to_local_dict(self.CCGX_data, local_values)

            # --- liveness of controller + CCGX ---
            status = self.failure_monitor.evaluate()

            if status['newly_failed']:
                reason = '; '.join(status['newly_failed'])
                self.safe_state.enter(reason=reason)
                self.notifier.notify_alert(
                    'essBATT Watchdog failure',
                    reason,
                    severity='critical',
                )
            elif status['any_failure'] and self.safe_state.active:
                # Re-assert safe state periodically while failure persists
                self.safe_state.enter(reason=self.safe_state.last_reason)
            elif not status['any_failure'] and self.safe_state.active:
                self.safe_state.clear(reason='all monitored sources healthy again')
                self.notifier.notify_recovery(
                    'essBATT Watchdog recovery',
                    'Controller and CCGX timeouts cleared. Safe-state flag cleared '
                    '(hardware not auto-restored).',
                )

            # --- optional Cerbo keepalive if controller is down ---
            # (periodic keepalive timer handles the steady case; see below)

            # --- battery threshold notifications ---
            self.battery_watch.evaluate(local_values)

        except Exception:
            self.logger.exception(
                'Unhandled exception in watchdog cycle — attempting safe state'
            )
            try:
                self.safe_state.enter(reason='watchdog cycle exception')
                self.notifier.notify_alert(
                    'essBATT Watchdog internal error',
                    'Unhandled exception in watchdog_cycle_update; safe state requested.',
                    severity='critical',
                )
            except Exception:
                self.logger.exception('Failed to enter safe state after cycle exception')

    # ==================================================================
    # INTERNALS — plumbing only (signals, timers, MQTT hooks)
    # ==================================================================

    def _install_signal_handlers(self):
        def _handler(signum, frame):
            try:
                name = signal.Signals(signum).name
            except Exception:
                name = str(signum)
            self.logger.warning(
                'Received signal ' + name + ' — shutting down cleanly.'
            )
            self._running = False

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, _handler)
            except (ValueError, OSError) as e:
                self.logger.debug(
                    'Could not install handler for ' + str(sig) + ': ' + str(e)
                )

    def _start_timers(self):
        if self._timers_started:
            return
        self.rt_keep_alive_obj = RepeatedTimer(
            constants.CERBO_KEEPALIVE_LENGTH, self.send_keepalive_to_cerbo
        )
        self.rt_watchdog_update_obj = RepeatedTimer(
            self.watchdog_config_data.get('watchdog_update_rate', 2.0),
            self.watchdog_cycle_update,
        )
        self.rt_print_status_obj = RepeatedTimer(
            self.watchdog_config_data.get('script_alive_logging_interval', 86400),
            self.print_alive_status_to_logger,
        )
        self._timers_started = True
        self.logger.info(
            'Background timers started (watchdog cycle, keepalive, alive log).'
        )

    def _on_mqtt_connected(self, is_reconnect=False):
        """After every successful CONNACK + subscribe: client, keepalive, timers."""
        if self.mqtt_bridge.client is not None:
            self.victron_output.set_mqtt_client(self.mqtt_bridge.client)
        self.send_keepalive_to_cerbo()
        if not self._timers_started:
            self._start_timers()
        if is_reconnect:
            self.logger.info(
                'MQTT session restored: resubscribed, keepalive sent, monitoring continues.'
            )

    def _maybe_warn_long_disconnect(self):
        if self.mqtt_bridge.is_connected:
            self._last_disconnect_warn_at = None
            return
        duration = self.mqtt_bridge.disconnect_duration_s()
        if duration < constants.MQTT_DISCONNECT_WARN_AFTER_S:
            return
        now = time.time()
        if (
            self._last_disconnect_warn_at is not None
            and (now - self._last_disconnect_warn_at)
            < constants.MQTT_DISCONNECT_WARN_INTERVAL_S
        ):
            return
        self._last_disconnect_warn_at = now
        self.logger.warning(
            'MQTT still disconnected for '
            + str(int(duration))
            + 's — watchdog cycle paused; waiting for paho auto-reconnect.'
        )

    def send_keepalive_to_cerbo(self):
        """Timer entry: Cerbo keepalive while connected.

        If ``send_keep_alive_if_essBATT_controller_is_down`` is 1, keepalive
        is only sent when the controller is considered failed (or when
        controller heartbeat monitoring is disabled — always send so CCGX
        data keeps flowing for the watchdog itself).
        """
        if not self.mqtt_bridge.is_connected:
            return

        only_when_controller_down = (
            self.watchdog_config_data.get(
                'send_keep_alive_if_essBATT_controller_is_down', 1
            )
            == 1
        )
        heartbeat_configured = (
            self.watchdog_config_data.get('controller_heartbeat_topic', 'none')
            not in (None, 'none', '')
        )

        if only_when_controller_down and heartbeat_configured:
            if not self.failure_monitor.controller_failed:
                # Controller is alive; it should own Cerbo keepalive.
                return

        self.victron_output.send_keepalive(
            self.watchdog_config_data.get('vrm_id'),
            self.watchdog_config_data.get('keepalive_get_all_topics', 0),
        )

    def print_alive_status_to_logger(self):
        connected = 'connected' if self.mqtt_bridge.is_connected else 'DISCONNECTED'
        safe = 'ACTIVE' if self.safe_state.active else 'inactive'
        self.logger.info(
            'essBATT watchdog is up and running! MQTT: '
            + connected
            + ', safe_state: '
            + safe
            + ', controller_failed: '
            + str(self.failure_monitor.controller_failed)
            + ', ccgx_failed: '
            + str(self.failure_monitor.ccgx_failed)
        )


# ======================================================================
# Entry point
# ======================================================================

if __name__ == '__main__':
    log_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s'
    )
    my_handler = RotatingFileHandler(
        'essBATT_watchdog.log',
        mode='a',
        maxBytes=50 * 1024 * 1024,
        backupCount=1,
        encoding=None,
        delay=0,
    )
    my_handler.setFormatter(log_formatter)
    my_handler.setLevel(logging.DEBUG)
    app_log = logging.getLogger('root')
    app_log.setLevel(logging.INFO)
    app_log.addHandler(my_handler)

    watchdog_obj = essBATT_watchdog(app_log)

    if (
        watchdog_obj.watchdog_config_data_loaded_correctly is True
        and watchdog_obj.ess_setvalue_list_loaded_correctly is True
    ):
        try:
            watchdog_obj.run()
        finally:
            watchdog_obj.logger.warning(
                'essBATT watchdog: Shutdown. Stopping timers and MQTT.'
            )
            watchdog_obj.stop()
    else:
        watchdog_obj.logger.warning(
            'essBATT watchdog not running and needs restart!'
        )
        if hasattr(watchdog_obj, 'stop'):
            watchdog_obj.stop()
