# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""Battery threshold monitoring for user notifications.

Reads rules from ``watchdog_config.json`` → ``battery_watch``. Each rule is:

  "cell_voltage_too_high": { "threshold": 3.6, "level": "critical" }

``level`` is ``info`` | ``warning`` | ``critical`` and selects which
notification channels fire (see Notifier). ``threshold: "none"`` disables
a rule.
"""

from notifier import severity_rank


_VALID_LEVELS = frozenset({'info', 'warning', 'critical'})

# (config_key, local_values key, compare op, human title, default level)
_CHECK_SPECS = (
    (
        'cell_voltage_too_high',
        'battery_max_cell_voltage',
        'gt',
        'Max cell voltage high',
        'critical',
    ),
    (
        'cell_voltage_too_low',
        'battery_min_cell_voltage',
        'lt',
        'Min cell voltage low',
        'critical',
    ),
    (
        'pack_voltage_too_low',
        'battery_voltage',
        'lt',
        'Pack voltage low',
        'critical',
    ),
    (
        'battery_too_hot',
        'battery_temperature',
        'gt',
        'Battery temperature high',
        'warning',
    ),
    (
        'battery_too_cold',
        'battery_temperature',
        'lt',
        'Battery temperature low',
        'warning',
    ),
    (
        'SOC_low',
        'battery_soc',
        'lt',
        'Battery SOC low',
        'warning',
    ),
    (
        'SOC_high',
        'battery_soc',
        'gt',
        'Battery SOC high',
        'info',
    ),
    (
        'current_too_high',
        'battery_current_abs',
        'gt',
        'Battery current high',
        'warning',
    ),
)


def _is_numeric_threshold(value):
    """Return True if config value is a number (not 'none' / None / empty)."""
    if value is None or value == 'none' or value == '':
        return False
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _parse_rule(raw, default_level):
    """Normalize a battery_watch entry to (threshold, level) or (None, level).

    Accepts nested ``{threshold, level}`` only (greenfield config).
    """
    if not isinstance(raw, dict):
        return None, default_level
    threshold = raw.get('threshold')
    level = str(raw.get('level', default_level) or default_level).strip().lower()
    if level not in _VALID_LEVELS:
        level = default_level
    if not _is_numeric_threshold(threshold):
        return None, level
    return float(threshold), level


class BatteryWatch:
    """Compares battery metrics against configured notification thresholds."""

    def __init__(self, config, logger, notifier):
        self.logger = logger
        self.notifier = notifier
        self._active_alerts = set()  # prevent spam while condition persists
        self.update_config(config)

    def update_config(self, config):
        self.config = config
        self.rules = config.get('battery_watch', {}) or {}

    def evaluate(self, local_values):
        """Check thresholds; notify on rising edge of each alert condition.

        Returns:
            list of currently active alert keys
        """
        if not local_values.get('all_CCGX_values_available', False):
            return list(self._active_alerts)

        # Convenience: abs current for over-current check
        current = local_values.get('battery_current')
        if current is not None:
            local_values['battery_current_abs'] = abs(current)

        active_now = set()

        for cfg_key, value_key, op, title, default_level in _CHECK_SPECS:
            threshold, level = _parse_rule(
                self.rules.get(cfg_key), default_level
            )
            if threshold is None:
                continue
            value = local_values.get(value_key)
            if value is None:
                continue
            breached = (value > threshold) if op == 'gt' else (value < threshold)
            if not breached:
                continue
            active_now.add(cfg_key)
            if cfg_key not in self._active_alerts:
                body = (
                    title + ': value=' + str(value)
                    + ', threshold=' + str(threshold)
                    + ', level=' + level
                )
                log_fn = (
                    self.logger.error if severity_rank(level) >= severity_rank('critical')
                    else self.logger.warning if severity_rank(level) >= severity_rank('warning')
                    else self.logger.info
                )
                log_fn(body)
                self.notifier.notify_alert(title, body, severity=level)

        recovered = self._active_alerts - active_now
        for key in recovered:
            self.logger.info('Battery alert cleared: ' + key)

        self._active_alerts = active_now
        return list(self._active_alerts)
