# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""Bring the Victron ESS into a configured safe state on detected failures.

Actions (driven by watchdog_config.json):
  - optionally set AcPowerSetPoint
  - optionally switch Multis off (vebus Mode = 4)
  - charge/discharge limits can be zeroed later if needed

Idempotent: repeated calls while already active only re-assert setpoints.
"""

import constants


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
