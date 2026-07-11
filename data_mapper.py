# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""Map raw Victron/CCGX MQTT data into a flat local_values dict for monitoring.

Watchdog needs fewer fields than the controller control cycle, but keeps the
same naming so battery thresholds and future logic stay consistent.
"""


class CcgxDataMapper:
    """Transforms self.CCGX_data-style nested dicts into flat local_values."""

    def __init__(self, logger):
        self.logger = logger

    def read_values_to_local_dict(self, ccgx_data, local_values):
        """Fill local_values from ccgx_data for watchdog checks.

        Sets ``all_CCGX_values_available`` when the core battery fields needed
        for battery_watch are present.
        """
        local_values['all_CCGX_values_available'] = True
        battery = ccgx_data.get('battery', {})

        self._copy_required(battery, 'soc', local_values, 'battery_soc')
        self._copy_required(
            battery, 'max_cell_voltage', local_values, 'battery_max_cell_voltage'
        )
        self._copy_required(
            battery, 'min_cell_voltage', local_values, 'battery_min_cell_voltage'
        )
        self._copy_required(battery, 'current', local_values, 'battery_current')
        self._copy_required(battery, 'voltage', local_values, 'battery_voltage')
        self._copy_required(battery, 'power', local_values, 'battery_power')
        self._copy_required(
            battery, 'temperature', local_values, 'battery_temperature'
        )

        if 'grid_power_sum' in ccgx_data.get('grid', {}):
            local_values['grid_power_sum'] = ccgx_data['grid']['grid_power_sum']

        return local_values

    def _copy_required(self, source, source_key, local_values, dest_key):
        if source_key in source:
            local_values[dest_key] = source[source_key]
        else:
            local_values['all_CCGX_values_available'] = False
