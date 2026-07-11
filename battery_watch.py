# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""Battery threshold monitoring for user notifications.

Reads thresholds from ``watchdog_config.json`` → ``battery_watch`` and compares
them to mapped local_values. Does not control the ESS; only raises alerts
via the injected Notifier.
"""


def _is_numeric_threshold(value):
    """Return True if config value is a number (not 'none' / None / empty)."""
    if value is None or value == 'none' or value == '':
        return False
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


class BatteryWatch:
    """Compares battery metrics against configured notification thresholds."""

    def __init__(self, config, logger, notifier):
        self.logger = logger
        self.notifier = notifier
        self._active_alerts = set()  # prevent spam while condition persists
        self.update_config(config)

    def update_config(self, config):
        self.config = config
        self.thresholds = config.get('battery_watch', {})

    def evaluate(self, local_values):
        """Check thresholds; notify on rising edge of each alert condition.

        Returns:
            list of currently active alert reason strings
        """
        if not local_values.get('all_CCGX_values_available', False):
            return list(self._active_alerts)

        active_now = set()
        thr = self.thresholds

        checks = [
            (
                'max_cell_high',
                local_values.get('battery_max_cell_voltage'),
                thr.get('voltage_too_high_notification'),
                'gt',
                'Max cell voltage high',
            ),
            (
                'min_cell_low',
                local_values.get('battery_min_cell_voltage'),
                thr.get('voltage_too_low_notification'),
                'lt',
                'Min cell voltage low',
            ),
            (
                'temp_high',
                local_values.get('battery_temperature'),
                thr.get('battery_too_hot_notification'),
                'gt',
                'Battery temperature high',
            ),
            (
                'temp_low',
                local_values.get('battery_temperature'),
                thr.get('battery_too_cold_notification'),
                'lt',
                'Battery temperature low',
            ),
            (
                'soc_low',
                local_values.get('battery_soc'),
                thr.get('SOC_low_notification'),
                'lt',
                'Battery SOC low',
            ),
            (
                'soc_high',
                local_values.get('battery_soc'),
                thr.get('SOC_high_notification'),
                'gt',
                'Battery SOC high',
            ),
            (
                'current_high',
                abs(local_values.get('battery_current', 0) or 0),
                thr.get('current_too_high_notification'),
                'gt',
                'Battery current high',
            ),
        ]

        for key, value, threshold, op, title in checks:
            if value is None or not _is_numeric_threshold(threshold):
                continue
            threshold = float(threshold)
            breached = (value > threshold) if op == 'gt' else (value < threshold)
            if not breached:
                continue
            active_now.add(key)
            if key not in self._active_alerts:
                body = (
                    title + ': value=' + str(value)
                    + ', threshold=' + str(threshold)
                )
                self.logger.warning(body)
                self.notifier.notify_alert(title, body, severity='warning')

        recovered = self._active_alerts - active_now
        for key in recovered:
            self.logger.info('Battery alert cleared: ' + key)

        self._active_alerts = active_now
        return list(self._active_alerts)
