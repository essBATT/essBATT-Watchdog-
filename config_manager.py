# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""Configuration management for essBATT Watchdog.

Loads ``watchdog_config.json`` and ``ess_setvalue_list.json``.

Optional local secrets overlay (never commit this file):
  ``watchdog_config.local.json`` is deep-merged on top of the base config.
  Put Telegram tokens / chat_id only there (file is gitignored).
"""

import copy
import json
from pathlib import Path

import constants


def deep_merge(base, overlay):
    """Recursively merge overlay into a copy of base (dicts only)."""
    result = copy.deepcopy(base)
    for key, value in (overlay or {}).items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


class ConfigManager:
    """Manages loading of watchdog configuration and Victron setvalue list."""

    def __init__(self, logger, debug=False):
        self.logger = logger
        self.debug = debug
        self.config_data_loaded_correctly = False
        self.setvalue_list_loaded_correctly = False

    def _get_config_path(self, debug_path, prod_path):
        if self.debug:
            return Path(debug_path)
        return Path(prod_path)

    def _read_json_file(self, debug_path, prod_path, description, required=True):
        file_path = self._get_config_path(debug_path, prod_path)
        try:
            with open(file_path, encoding='utf-8') as f:
                data = json.load(f)
            self.logger.debug(f'{description} loaded successfully from {file_path}')
            return data
        except FileNotFoundError:
            if required:
                self.logger.error(f'{description} not found at: {file_path}')
            else:
                self.logger.debug(f'{description} not present (optional): {file_path}')
        except json.JSONDecodeError as e:
            self.logger.error(f'{description} contains invalid JSON: {e}')
        except OSError as e:
            self.logger.error(f'Could not read {description}: {e}')
        return None

    def load_config(self):
        """Load base config, then optional local secrets overlay."""
        data = self._read_json_file(
            './smarthome_projects/essBATT-Watchdog-/watchdog_config.json',
            'watchdog_config.json',
            'watchdog_config.json',
            required=True,
        )
        if data is None:
            self.config_data_loaded_correctly = False
            return {}

        local = self._read_json_file(
            './smarthome_projects/essBATT-Watchdog-/watchdog_config.local.json',
            'watchdog_config.local.json',
            'watchdog_config.local.json',
            required=False,
        )
        if local:
            data = deep_merge(data, local)
            self.logger.info(
                'Merged watchdog_config.local.json (local secrets/overrides).'
            )

        self.config_data_loaded_correctly = True
        return data

    def load_setvalue_list(self):
        """Load ess_setvalue_list.json (Victron write path suffixes)."""
        data = self._read_json_file(
            './smarthome_projects/essBATT-Watchdog-/ess_setvalue_list.json',
            'ess_setvalue_list.json',
            'ess_setvalue_list.json',
        )
        if data is None:
            self.setvalue_list_loaded_correctly = False
            return {}
        self.setvalue_list_loaded_correctly = True
        return data
