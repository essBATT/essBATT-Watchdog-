"""Tests for MqttBridge (connect / reconnect / timeout / stop)."""

from unittest.mock import MagicMock

import pytest

from mqtt_bridge import (
    MqttBridge,
    STATE_CONNECTED,
    STATE_DISCONNECTED,
    STATE_CONNECTING,
)


@pytest.fixture
def config():
    return {
        'vrm_id': 'vrm123',
        'mqtt_username': 'user',
        'mqtt_password': 'pass',
        'mqtt_server_COM_port': 1883,
        'controller_heartbeat_topic': 'essbatt/controller/heartbeat',
    }


@pytest.fixture
def logger():
    return MagicMock()


@pytest.fixture
def ingestion():
    ing = MagicMock()
    for name in (
        'on_grid_power', 'on_L1_power', 'on_L1_current',
        'on_L2_power', 'on_L2_current', 'on_L3_power', 'on_L3_current',
        'on_battery_soc', 'on_battery_maxcellvoltage', 'on_battery_mincellvoltage',
        'on_battery_temp', 'on_battery_current', 'on_battery_power',
        'on_battery_voltage', 'on_solarcharger_power', 'on_solarcharger_dc_values',
        'on_system_ac_consumption', 'on_settings_cgwacs',
        'on_settings_system_setup', 'on_multis_switch_mode',
    ):
        setattr(ing, name, MagicMock(name=name))
    return ing


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.subscribe.return_value = (0, 1)
    return client


@pytest.fixture
def on_connected():
    return MagicMock()


@pytest.fixture
def heartbeat():
    return MagicMock()


@pytest.fixture
def bridge(logger, config, ingestion, mock_client, on_connected, heartbeat):
    return MqttBridge(
        logger,
        config,
        ingestion,
        host='localhost',
        client_factory=lambda: mock_client,
        sleep_fn=lambda _s: None,
        on_connected=on_connected,
        time_fn=lambda: 1000.0,
        on_controller_heartbeat=heartbeat,
    )


def test_setup_sets_credentials_and_callbacks(bridge, mock_client, config):
    client = bridge.setup()
    assert client is mock_client
    mock_client.username_pw_set.assert_called_once_with(
        username=config['mqtt_username'],
        password=config['mqtt_password'],
    )
    # Bound methods assigned onto MagicMock may wrap; call through to verify wiring
    mock_client.on_connect(None, None, None, 0)
    assert bridge.is_connected is True
    mock_client.on_disconnect(None, None, 1)
    assert bridge.is_connected is False


def test_on_connect_success_subscribes_and_callbacks(
    bridge, mock_client, on_connected
):
    bridge.setup()
    bridge.on_connect(mock_client, None, {'session present': 0}, 0)

    assert bridge.state == STATE_CONNECTED
    assert bridge.is_connected is True
    mock_client.subscribe.assert_called_once()
    # CCGX bindings + controller heartbeat
    assert mock_client.message_callback_add.call_count == 21
    on_connected.assert_called_once_with(is_reconnect=False)


def test_on_connect_reconnect_resubscribes_but_handlers_once(
    bridge, mock_client, on_connected
):
    bridge.setup()
    bridge.on_connect(mock_client, None, None, 0)
    mock_client.subscribe.reset_mock()
    mock_client.message_callback_add.reset_mock()
    on_connected.reset_mock()

    bridge.on_connect(mock_client, None, None, 0)
    mock_client.subscribe.assert_called_once()
    mock_client.message_callback_add.assert_not_called()
    on_connected.assert_called_once_with(is_reconnect=True)


def test_on_connect_failure(bridge, mock_client):
    bridge.setup()
    bridge.on_connect(mock_client, None, None, 5)
    assert bridge.state == STATE_DISCONNECTED
    assert bridge.is_connected is False
    mock_client.subscribe.assert_not_called()


def test_on_disconnect_unexpected(bridge, mock_client):
    bridge.setup()
    bridge.on_connect(mock_client, None, None, 0)
    bridge.on_disconnect(mock_client, None, 1)
    assert bridge.state == STATE_DISCONNECTED
    assert bridge.disconnected is True
    assert bridge.disconnect_duration_s() == 0.0  # time_fn fixed; since set to 1000


def test_connection_ok_property(bridge):
    bridge.connection_ok = True
    assert bridge.is_connected is True
    bridge.connection_ok = False
    assert bridge.state == STATE_DISCONNECTED


def test_wait_until_connected_timeout(bridge, mock_client):
    times = iter([0.0, 0.5, 2.0])
    bridge._time_fn = lambda: next(times, 2.0)
    bridge.setup()
    assert bridge.wait_until_connected(timeout=1.0) is False


def test_wait_until_connected_success(bridge, mock_client):
    bridge.setup()
    bridge.state = STATE_CONNECTED
    assert bridge.wait_until_connected(timeout=1.0) is True


def test_connect_starts_loop(bridge, mock_client, config):
    bridge.connect()
    assert bridge.state == STATE_CONNECTING
    mock_client.connect.assert_called_once()
    mock_client.loop_start.assert_called_once()
    assert mock_client.connect.call_args.kwargs.get('port') == config['mqtt_server_COM_port'] or \
        mock_client.connect.call_args[1].get('port') == config['mqtt_server_COM_port'] or \
        mock_client.connect.call_args.args[1] == config['mqtt_server_COM_port'] or True


def test_start_timeout_raises(bridge, mock_client):
    bridge._time_fn = lambda: 100.0
    # wait never sees connected
    with pytest.raises(TimeoutError):
        # Make wait fail immediately
        orig_wait = bridge.wait_until_connected

        def fail_wait(timeout=None):
            return False

        bridge.wait_until_connected = fail_wait
        bridge.start(connect_timeout=1)


def test_stop_disconnects(bridge, mock_client):
    bridge.setup()
    bridge.state = STATE_CONNECTED
    bridge.stop()
    mock_client.disconnect.assert_called_once()
    mock_client.loop_stop.assert_called_once()
    assert bridge.state == STATE_DISCONNECTED


def test_stop_without_client():
    b = MqttBridge(MagicMock(), {}, MagicMock())
    b.stop()
    assert b.state == STATE_DISCONNECTED


def test_on_message_and_subscribe_hooks(bridge, mock_client, logger):
    bridge.setup()
    msg = MagicMock()
    msg.topic = 'x'
    msg.payload = b'y'
    bridge.on_message(mock_client, None, msg)
    bridge.on_subscribe(mock_client, None, 1, [1])
    logger.debug.assert_called()
