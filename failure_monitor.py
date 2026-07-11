# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""Detect timeouts of essBATT controller and Victron CCGX data streams.

This module is intentionally side-effect free regarding MQTT / notifications:
it only tracks timestamps and reports a structured failure state. The
composition root decides when to enter safe state and notify the user.
"""

import time


class FailureMonitor:
    """Tracks liveness of controller heartbeat and CCGX MQTT traffic."""

    def __init__(self, config, logger, time_fn=None):
        self.logger = logger
        self._time_fn = time_fn or time.time
        self.update_config(config)

        now = self._time_fn()
        # Start "seen" clocks at init so we do not false-alarm immediately
        # after boot before the first message arrives.
        self.last_controller_heartbeat_at = now
        self.last_ccgx_message_at = now

        self.controller_failed = False
        self.ccgx_failed = False

    def update_config(self, config):
        self.config = config
        self.controller_timeout_s = float(
            config.get('essBATT_controller_timeout_detection_duration', 95)
        )
        # typo kept as alias for the historical config key
        self.ccgx_timeout_s = float(
            config.get(
                'victron_CCGX_timeout_detection_duration',
                config.get('victron_CCGX_timout_detection_duration', 60),
            )
        )
        self.heartbeat_topic = config.get('controller_heartbeat_topic', 'none')

    def note_controller_heartbeat(self, msg=None):
        """Called when a controller heartbeat MQTT message is received."""
        self.last_controller_heartbeat_at = self._time_fn()
        if self.controller_failed:
            self.logger.info(
                'Controller heartbeat resumed — clearing controller_failed flag'
            )
        self.controller_failed = False

    def note_ccgx_message(self):
        """Called on any successful Victron CCGX data ingestion."""
        self.last_ccgx_message_at = self._time_fn()
        if self.ccgx_failed:
            self.logger.info(
                'CCGX MQTT traffic resumed — clearing ccgx_failed flag'
            )
        self.ccgx_failed = False

    def evaluate(self):
        """Recompute failure flags based on timeouts.

        Returns:
            dict with keys:
              controller_failed, ccgx_failed,
              controller_age_s, ccgx_age_s,
              any_failure, newly_failed (list of reason strings)
        """
        now = self._time_fn()
        controller_age = now - self.last_controller_heartbeat_at
        ccgx_age = now - self.last_ccgx_message_at

        newly_failed = []

        # Controller heartbeat is optional until the controller publishes one.
        # If topic is "none", controller timeout checks are disabled.
        if self.heartbeat_topic and self.heartbeat_topic != 'none':
            if controller_age >= self.controller_timeout_s:
                if not self.controller_failed:
                    newly_failed.append(
                        'controller_timeout (no heartbeat for '
                        + str(int(controller_age)) + 's)'
                    )
                self.controller_failed = True
        else:
            self.controller_failed = False

        if ccgx_age >= self.ccgx_timeout_s:
            if not self.ccgx_failed:
                newly_failed.append(
                    'ccgx_timeout (no Victron MQTT data for '
                    + str(int(ccgx_age)) + 's)'
                )
            self.ccgx_failed = True

        any_failure = self.controller_failed or self.ccgx_failed
        return {
            'controller_failed': self.controller_failed,
            'ccgx_failed': self.ccgx_failed,
            'controller_age_s': controller_age,
            'ccgx_age_s': ccgx_age,
            'any_failure': any_failure,
            'newly_failed': newly_failed,
        }
