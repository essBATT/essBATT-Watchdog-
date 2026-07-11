"""Tests for RepeatedTimer (mocked threading, no real delays)."""

from unittest.mock import patch

from utils import RepeatedTimer


def test_repeated_timer_initialization(mocker):
    mock_timer = mocker.patch('threading.Timer')
    mock_function = mocker.Mock()

    rt = RepeatedTimer(5.0, mock_function, 'arg1', kwarg='value')

    assert rt.interval == 5.0
    assert rt.function is mock_function
    assert rt.args == ('arg1',)
    assert rt.kwargs == {'kwarg': 'value'}
    assert rt.is_running is True
    mock_timer.assert_called_once()


def test_repeated_timer_stop(mocker):
    mock_timer_instance = mocker.Mock()
    mocker.patch('threading.Timer', return_value=mock_timer_instance)

    rt = RepeatedTimer(1.0, lambda: None)
    rt.stop()

    mock_timer_instance.cancel.assert_called_once()
    assert rt.is_running is False


def test_repeated_timer_callback(mocker):
    mock_function = mocker.Mock()
    mocker.patch('time.time', return_value=100.0)

    with patch('threading.Timer'):
        rt = RepeatedTimer(10.0, mock_function)
        rt._run()
        mock_function.assert_called_once_with()
