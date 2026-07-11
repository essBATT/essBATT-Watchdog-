# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""Bring the Victron ESS into a configured safe state on detected failures.

Triggers (composition root decides via ``resolve_safe_state_action``):
  - controller / CCGX timeouts (FailureMonitor)
  - battery_watch rules with level ``critical``
  - unhandled exceptions in the monitor cycle

Actions (driven by watchdog_config.json):
  - optionally set AcPowerSetPoint
  - optionally switch Multis off (vebus Mode = 4)
  - charge/discharge limits can be zeroed later if needed

Idempotent: repeated calls while already active only re-assert setpoints.
"""

import constants


def resolve_safe_state_action(failure_status, battery_status, safe_state_active):
    """Decide enter / reassert / clear from failure + battery critical state.

    Args:
        failure_status: dict from FailureMonitor.evaluate()
        battery_status: dict from BatteryWatch.evaluate()
        safe_state_active: current SafeStateManager.active flag

    Returns:
        dict with:
          action: 'enter' | 'reassert' | 'clear' | 'none'
          reason: str (for enter; else '')
          notify_failure: bool (True if a new controller/CCGX failure should
              trigger the generic failure notification)
    """
    newly_failed = list(failure_status.get('newly_failed') or [])
    newly_critical = list(battery_status.get('newly_critical') or [])
    critical_active = list(battery_status.get('critical_active') or [])

    reason_parts = list(newly_failed)
    if newly_critical:
        reason_parts.append(
            'battery_critical (' + ', '.join(newly_critical) + ')'
        )

    hold = bool(failure_status.get('any_failure')) or bool(critical_active)

    if reason_parts:
        return {
            'action': 'enter',
            'reason': '; '.join(reason_parts),
            'notify_failure': bool(newly_failed),
        }
    if hold and safe_state_active:
        return {
            'action': 'reassert',
            'reason': '',
            'notify_failure': False,
        }
    if not hold and safe_state_active:
        return {
            'action': 'clear',
            'reason': '',
            'notify_failure': False,
        }
    return {
        'action': 'none',
        'reason': '',
        'notify_failure': False,
    }


class SafeStateManager:
    """Applies and tracks the hardware/software safe state via VictronOutput."""

    def __init__(self, config, logger, victron_output):
        self.logger = logger
        self.victron_output = victron_output
        self.active = False
        self.last_reason = ''
        self.update_config(config)

    def update_config(self, config):
        self.config = config
        self.switch_off_multis = (
            config.get('switch_off_multis_if_failure_is_detected', 0) == 1
        )
        self.ac_power_setpoint = config.get(
            'ac_power_set_point_if_failure_is_detected', None
        )
        self.zero_charge_discharge = (
            config.get('zero_charge_discharge_limits_if_failure_is_detected', 1) == 1
        )

    def enter(self, reason=''):
        """Enter (or re-assert) safe state. Returns True if actions were attempted."""
        self.active = True
        self.last_reason = reason or self.last_reason or 'unspecified'
        self.logger.error(
            'ENTERING SAFE STATE — reason: ' + self.last_reason
        )

        attempted = False

        if self.ac_power_setpoint is not None:
            rc = self.victron_output.set_ccgx_value(
                set_val_name_str='AcPowerSetPoint',
                set_val=self.ac_power_setpoint,
                only_set_if_deviation_to_current_setting=True,
            )
            attempted = attempted or (rc == 1)

        if self.zero_charge_discharge:
            rc_c = self.victron_output.set_ccgx_value(
                set_val_name_str='MaxChargeCurrent',
                set_val=0,
                only_set_if_deviation_to_current_setting=True,
            )
            rc_d = self.victron_output.set_ccgx_value(
                set_val_name_str='MaxDischargePower',
                set_val=0,
                only_set_if_deviation_to_current_setting=True,
            )
            attempted = attempted or (rc_c == 1) or (rc_d == 1)

        if self.switch_off_multis:
            published = self.victron_output.set_multis_switch_mode(
                constants.SAFE_STATE_MULTIS_OFF
            )
            attempted = attempted or published

        return attempted

    def clear(self, reason=''):
        """Mark safe state as cleared (does not auto-restore Multis On).

        Restoring normal operation is intentionally manual / controller-driven
        so the watchdog never re-enables hardware after a real fault.
        """
        if self.active:
            self.logger.info(
                'Safe state flag cleared'
                + ((' — ' + reason) if reason else '')
                + '. Hardware setpoints are NOT automatically restored.'
            )
        self.active = False
        self.last_reason = ''
