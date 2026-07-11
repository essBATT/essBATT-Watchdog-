"""Tests for Notifier (Telegram delivery)."""

import json
import io
from unittest.mock import MagicMock

import pytest

from notifier import Notifier


class _FakeResponse:
    def __init__(self, body, status=200):
        self._body = body if isinstance(body, bytes) else body.encode('utf-8')
        self.status = status

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _base_config(telegram=None, mail_enabled=0):
    return {
        'notification_mail_address': 'your.mailaddress@abc.com',
        'notifications': {
            'mail': {'enabled': mail_enabled},
            'telegram': telegram or {
                'enabled': 1,
                'bot_token': '123:ABC',
                'chat_id': '999001',
                'timeout_s': 5,
            },
        },
    }


def test_telegram_disabled_does_not_call_api():
    urlopen = MagicMock()
    n = Notifier(_base_config(telegram={'enabled': 0, 'bot_token': 't', 'chat_id': '1'}),
                 MagicMock(), urlopen_fn=urlopen)
    n.notify_alert('t', 'b', severity='error')
    urlopen.assert_not_called()


def test_telegram_missing_chat_id_logs_error():
    urlopen = MagicMock()
    logger = MagicMock()
    n = Notifier(
        _base_config(telegram={'enabled': 1, 'bot_token': '123:ABC', 'chat_id': ''}),
        logger,
        urlopen_fn=urlopen,
    )
    n.notify_alert('title', 'body')
    urlopen.assert_not_called()
    assert logger.error.called
    assert 'chat_id' in logger.error.call_args.args[0]


def test_telegram_send_success():
    urlopen = MagicMock(
        return_value=_FakeResponse(json.dumps({'ok': True, 'result': {'message_id': 1}}))
    )
    logger = MagicMock()
    n = Notifier(_base_config(), logger, urlopen_fn=urlopen)
    n.notify_alert('Cell high', 'max=3.7', severity='warning')

    urlopen.assert_called_once()
    request = urlopen.call_args.args[0]
    assert request.full_url.endswith('/bot123:ABC/sendMessage')
    assert request.get_method() == 'POST'
    payload = json.loads(request.data.decode('utf-8'))
    assert payload['chat_id'] == '999001'
    assert '[WARNING] Cell high' in payload['text']
    assert 'max=3.7' in payload['text']
    assert payload['disable_web_page_preview'] is True
    assert any('Telegram alert sent' in str(c.args[0]) for c in logger.info.call_args_list)


def test_telegram_api_not_ok():
    urlopen = MagicMock(
        return_value=_FakeResponse(
            json.dumps({'ok': False, 'description': 'Bad Request: chat not found'})
        )
    )
    logger = MagicMock()
    n = Notifier(_base_config(), logger, urlopen_fn=urlopen)
    n.notify_alert('t', 'b')
    assert logger.error.called
    assert 'chat not found' in logger.error.call_args.args[0]


def test_telegram_env_overrides_config(monkeypatch):
    monkeypatch.setenv('ESSBATT_TELEGRAM_BOT_TOKEN', 'env-token')
    monkeypatch.setenv('ESSBATT_TELEGRAM_CHAT_ID', 'env-chat')
    urlopen = MagicMock(
        return_value=_FakeResponse(json.dumps({'ok': True, 'result': {}}))
    )
    n = Notifier(
        _base_config(telegram={
            'enabled': 1,
            'bot_token': 'config-token',
            'chat_id': 'config-chat',
        }),
        MagicMock(),
        urlopen_fn=urlopen,
    )
    n.notify_alert('t', 'b')
    request = urlopen.call_args.args[0]
    assert '/botenv-token/sendMessage' in request.full_url
    payload = json.loads(request.data.decode('utf-8'))
    assert payload['chat_id'] == 'env-chat'


def test_notify_recovery_uses_info_severity():
    urlopen = MagicMock(
        return_value=_FakeResponse(json.dumps({'ok': True, 'result': {}}))
    )
    n = Notifier(_base_config(), MagicMock(), urlopen_fn=urlopen)
    n.notify_recovery('back', 'ok again')
    payload = json.loads(urlopen.call_args.args[0].data.decode('utf-8'))
    assert payload['text'].startswith('[INFO]')
