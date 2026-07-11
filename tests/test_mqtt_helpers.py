"""Tests for pure mqtt_helpers builders."""

import json
from unittest.mock import MagicMock

from mqtt_helpers import (
    build_subscription_list,
    build_keepalive_selected_topics,
    build_keepalive_publish,
    build_ccgx_topic_bindings,
    build_controller_heartbeat_binding,
    register_topic_callbacks,
)


def test_subscription_list_basic_without_heartbeat():
    subs = build_subscription_list('N/test_vrm', {'controller_heartbeat_topic': 'none'})
    topics = [t for t, _qos in subs]
    assert 'N/test_vrm/battery/#' in topics
    assert 'N/test_vrm/grid/#' in topics
    assert 'N/test_vrm/vebus/+/Mode' in topics
    assert len(subs) == 6
    assert all(qos == 1 for _, qos in subs)


def test_subscription_list_includes_controller_heartbeat():
    subs = build_subscription_list(
        'N/vrm1',
        {'controller_heartbeat_topic': 'essbatt/controller/heartbeat'},
    )
    topics = [t for t, _ in subs]
    assert 'essbatt/controller/heartbeat' in topics
    assert len(subs) == 7


def test_keepalive_selected_topics_contains_critical_paths():
    topics = build_keepalive_selected_topics()
    assert 'battery/+/Soc' in topics
    assert 'battery/+/System/MaxCellVoltage' in topics
    assert 'vebus/+/Mode' in topics
    assert 'grid/+/Ac/Power' in topics


def test_keepalive_publish_all_topics_mode():
    topic, payload = build_keepalive_publish('VRM123', 1)
    assert topic == 'R/VRM123/system/0/Serial'
    assert payload == ''


def test_keepalive_publish_selected_mode():
    topic, payload = build_keepalive_publish('VRM123', 0)
    assert topic == 'R/VRM123/keepalive'
    parsed = json.loads(payload)
    assert 'battery/+/Soc' in parsed


def test_keepalive_publish_invalid_mode():
    assert build_keepalive_publish('VRM123', 99) is None


def test_ccgx_topic_bindings_cover_battery_and_grid():
    ingestion = MagicMock()
    bindings = build_ccgx_topic_bindings('N/vrm', ingestion)
    topics = [t for t, _ in bindings]
    assert 'N/vrm/battery/+/Soc' in topics
    assert 'N/vrm/grid/+/Ac/Power' in topics
    assert 'N/vrm/vebus/+/Mode' in topics
    # Callbacks are paho-style (client, userdata, msg)
    cb = dict(bindings)['N/vrm/battery/+/Soc']
    msg = MagicMock()
    cb(None, None, msg)
    ingestion.on_battery_soc.assert_called_once_with(msg)


def test_controller_heartbeat_binding_optional():
    assert build_controller_heartbeat_binding(
        {'controller_heartbeat_topic': 'none'}, MagicMock()
    ) == []
    bindings = build_controller_heartbeat_binding(
        {'controller_heartbeat_topic': 'essbatt/controller/heartbeat'},
        MagicMock(),
    )
    assert len(bindings) == 1
    assert bindings[0][0] == 'essbatt/controller/heartbeat'


def test_register_topic_callbacks():
    client = MagicMock()
    cb = MagicMock()
    register_topic_callbacks(client, [('topic/a', cb), ('topic/b', cb)])
    assert client.message_callback_add.call_count == 2
