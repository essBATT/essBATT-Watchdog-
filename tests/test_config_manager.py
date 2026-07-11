"""Tests for ConfigManager deep-merge / local secrets overlay."""

import json
from pathlib import Path
from unittest.mock import MagicMock

from config_manager import ConfigManager, deep_merge


def test_deep_merge_nested_overlay():
    base = {
        'a': 1,
        'notifications': {
            'telegram': {'enabled': 0, 'bot_token': 'PLACEHOLDER', 'chat_id': ''},
            'mail': {'enabled': 0},
        },
    }
    overlay = {
        'notifications': {
            'telegram': {'enabled': 1, 'bot_token': 'secret', 'chat_id': '42'},
        },
    }
    merged = deep_merge(base, overlay)
    assert merged['a'] == 1
    assert merged['notifications']['mail']['enabled'] == 0
    assert merged['notifications']['telegram']['enabled'] == 1
    assert merged['notifications']['telegram']['bot_token'] == 'secret'
    assert merged['notifications']['telegram']['chat_id'] == '42'
    # base not mutated
    assert base['notifications']['telegram']['bot_token'] == 'PLACEHOLDER'


def test_load_config_merges_local_file(tmp_path, monkeypatch):
    base = {
        'vrm_id': 'x',
        'notifications': {
            'telegram': {'enabled': 0, 'bot_token': 'YOUR_BOT_TOKEN', 'chat_id': 'YOUR_CHAT_ID'},
        },
    }
    local = {
        'notifications': {
            'telegram': {'enabled': 1, 'bot_token': 'tok', 'chat_id': '99'},
        },
    }
    (tmp_path / 'watchdog_config.json').write_text(json.dumps(base), encoding='utf-8')
    (tmp_path / 'watchdog_config.local.json').write_text(
        json.dumps(local), encoding='utf-8'
    )

    cm = ConfigManager(MagicMock(), debug=False)
    # Force prod paths to tmp_path
    monkeypatch.chdir(tmp_path)
    data = cm.load_config()
    assert cm.config_data_loaded_correctly is True
    assert data['notifications']['telegram']['enabled'] == 1
    assert data['notifications']['telegram']['bot_token'] == 'tok'
    assert data['notifications']['telegram']['chat_id'] == '99'


def test_load_config_works_without_local_file(tmp_path, monkeypatch):
    base = {'vrm_id': 'only-base', 'notifications': {}}
    (tmp_path / 'watchdog_config.json').write_text(json.dumps(base), encoding='utf-8')
    monkeypatch.chdir(tmp_path)
    cm = ConfigManager(MagicMock(), debug=False)
    data = cm.load_config()
    assert data['vrm_id'] == 'only-base'


def test_load_config_missing_base_returns_empty(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cm = ConfigManager(MagicMock(), debug=False)
    data = cm.load_config()
    assert data == {}
    assert cm.config_data_loaded_correctly is False


def test_load_config_invalid_json(tmp_path, monkeypatch):
    (tmp_path / 'watchdog_config.json').write_text('{not json', encoding='utf-8')
    monkeypatch.chdir(tmp_path)
    cm = ConfigManager(MagicMock(), debug=False)
    data = cm.load_config()
    assert data == {}
    assert cm.config_data_loaded_correctly is False


def test_load_setvalue_list_ok_and_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cm = ConfigManager(MagicMock(), debug=False)
    assert cm.load_setvalue_list() == {}
    assert cm.setvalue_list_loaded_correctly is False

    (tmp_path / 'ess_setvalue_list.json').write_text(
        json.dumps({'AcPowerSetPoint': 'CGwacs/AcPowerSetPoint'}),
        encoding='utf-8',
    )
    data = cm.load_setvalue_list()
    assert data['AcPowerSetPoint'] == 'CGwacs/AcPowerSetPoint'
    assert cm.setvalue_list_loaded_correctly is True


def test_debug_path_selection(tmp_path, monkeypatch):
    """debug=True uses the smarthome_projects path prefix (may not exist)."""
    cm = ConfigManager(MagicMock(), debug=True)
    path = cm._get_config_path(
        './smarthome_projects/essBATT-Watchdog-/watchdog_config.json',
        'watchdog_config.json',
    )
    assert 'smarthome_projects' in str(path)
