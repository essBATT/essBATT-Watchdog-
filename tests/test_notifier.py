"""Tests for Notifier (Telegram + Mail + Pushover delivery)."""

import json
import urllib.parse
from email.message import EmailMessage
from unittest.mock import MagicMock

import pytest
import smtplib

from notifier import Notifier, format_severity_label


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


class _FakeSMTP:
    """Minimal SMTP stand-in for unit tests."""

    instances = []

    def __init__(self, host, port, timeout=None, context=None, **kwargs):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.started_tls = False
        self.logged_in = None
        self.sent = []
        self.quit_called = False
        _FakeSMTP.instances.append(self)

    def ehlo(self):
        return (250, b'ok')

    def starttls(self, context=None):
        self.started_tls = True
        return (220, b'ready')

    def login(self, user, password):
        self.logged_in = (user, password)
        return (235, b'ok')

    def send_message(self, msg):
        self.sent.append(msg)

    def quit(self):
        self.quit_called = True

    def close(self):
        pass


def _base_config(telegram=None, mail=None, pushover=None):
    return {
        'notifications': {
            'mail': mail if mail is not None else {'enabled': 0},
            'telegram': telegram if telegram is not None else {
                'enabled': 1,
                'bot_token': '123:ABC',
                'chat_id': '999001',
                'timeout_s': 5,
            },
            'pushover': pushover if pushover is not None else {'enabled': 0},
        },
    }


def _pushover_cfg(**overrides):
    cfg = {
        'enabled': 1,
        'api_token': 'app-token',
        'user_key': 'user-key',
        'min_severity': 'critical',
        'priority_critical': 2,
        'priority_error': 1,
        'priority_warning': 0,
        'priority_info': 0,
        'retry_s': 60,
        'expire_s': 900,
        'sound': 'siren',
        'timeout_s': 5,
    }
    cfg.update(overrides)
    return cfg


def _mail_cfg(**overrides):
    cfg = {
        'enabled': 1,
        'to_address': 'user@example.com',
        'smtp_host': 'smtp.test.local',
        'smtp_port': 587,
        'smtp_user': 'smtpuser',
        'smtp_password': 'secret',
        'from_address': 'watchdog@example.com',
        'use_tls': 1,
        'use_ssl': 0,
        'timeout_s': 5,
    }
    cfg.update(overrides)
    return cfg


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def test_telegram_disabled_does_not_call_api():
    urlopen = MagicMock()
    n = Notifier(
        _base_config(telegram={'enabled': 0, 'bot_token': 't', 'chat_id': '1'}),
        MagicMock(),
        urlopen_fn=urlopen,
    )
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
    assert payload['text'].startswith('⚠️ WARNING Cell high')
    assert 'max=3.7' in payload['text']
    assert payload['disable_web_page_preview'] is True
    assert any('Telegram alert sent' in str(c.args[0]) for c in logger.info.call_args_list)


def test_critical_telegram_message_uses_alert_emoji():
    urlopen = MagicMock(
        return_value=_FakeResponse(json.dumps({'ok': True, 'result': {}}))
    )
    n = Notifier(_base_config(), MagicMock(), urlopen_fn=urlopen)
    n.notify_alert('Controller down', 'timeout 95s', severity='critical')
    payload = json.loads(urlopen.call_args.args[0].data.decode('utf-8'))
    assert payload['text'].startswith('🚨 CRITICAL Controller down')
    assert 'timeout 95s' in payload['text']


def test_format_severity_label():
    assert format_severity_label('critical') == '🚨 CRITICAL'
    assert format_severity_label('WARNING') == '⚠️ WARNING'
    assert format_severity_label('info') == 'ℹ️ INFO'


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
    assert payload['text'].startswith('ℹ️ INFO')


# ---------------------------------------------------------------------------
# Mail
# ---------------------------------------------------------------------------

def test_mail_disabled_does_not_use_smtp():
    factory = MagicMock()
    n = Notifier(
        _base_config(telegram={'enabled': 0}, mail={'enabled': 0}),
        MagicMock(),
        smtp_factory=factory,
    )
    n.notify_alert('t', 'b')
    factory.assert_not_called()


def test_mail_incomplete_config_logs_error():
    logger = MagicMock()
    factory = MagicMock()
    n = Notifier(
        _base_config(
            telegram={'enabled': 0},
            mail={'enabled': 1, 'smtp_host': 'smtp.example.com'},  # placeholder host
        ),
        logger,
        smtp_factory=factory,
    )
    ok = n._send_mail('t', 'b', 'error')
    assert ok is False
    factory.assert_not_called()
    assert logger.error.called


def test_mail_send_success_starttls_and_login():
    _FakeSMTP.instances.clear()

    def factory(host, port, timeout_s, use_ssl):
        assert use_ssl is False
        assert host == 'smtp.test.local'
        assert port == 587
        return _FakeSMTP(host, port, timeout=timeout_s)

    logger = MagicMock()
    n = Notifier(
        _base_config(telegram={'enabled': 0}, mail=_mail_cfg()),
        logger,
        smtp_factory=factory,
    )
    ok = n._send_mail('Cell high', 'max=3.7', 'warning')
    assert ok is True
    assert len(_FakeSMTP.instances) == 1
    smtp = _FakeSMTP.instances[0]
    assert smtp.started_tls is True
    assert smtp.logged_in == ('smtpuser', 'secret')
    assert len(smtp.sent) == 1
    msg = smtp.sent[0]
    assert isinstance(msg, EmailMessage)
    assert msg['To'] == 'user@example.com'
    assert msg['From'] == 'watchdog@example.com'
    assert '⚠️ WARNING' in msg['Subject']
    assert 'Cell high' in msg['Subject']
    assert 'max=3.7' in msg.get_content()
    assert smtp.quit_called is True
    assert any('Mail alert sent' in str(c.args[0]) for c in logger.info.call_args_list)


def test_mail_env_overrides(monkeypatch):
    _FakeSMTP.instances.clear()
    monkeypatch.setenv('ESSBATT_MAIL_TO', 'env-to@example.com')
    monkeypatch.setenv('ESSBATT_MAIL_SMTP_HOST', 'smtp.env.local')
    monkeypatch.setenv('ESSBATT_MAIL_SMTP_USER', 'envuser')
    monkeypatch.setenv('ESSBATT_MAIL_SMTP_PASSWORD', 'envpass')
    monkeypatch.setenv('ESSBATT_MAIL_FROM', 'env-from@example.com')

    def factory(host, port, timeout_s, use_ssl):
        assert host == 'smtp.env.local'
        return _FakeSMTP(host, port, timeout=timeout_s)

    n = Notifier(
        _base_config(telegram={'enabled': 0}, mail=_mail_cfg()),
        MagicMock(),
        smtp_factory=factory,
    )
    assert n._send_mail('t', 'b', 'error') is True
    smtp = _FakeSMTP.instances[0]
    assert smtp.logged_in == ('envuser', 'envpass')
    assert smtp.sent[0]['To'] == 'env-to@example.com'
    assert smtp.sent[0]['From'] == 'env-from@example.com'


def test_mail_auth_error_logged():
    class AuthFailSMTP(_FakeSMTP):
        def login(self, user, password):
            raise smtplib.SMTPAuthenticationError(535, b'auth failed')

    def factory(host, port, timeout_s, use_ssl):
        return AuthFailSMTP(host, port, timeout=timeout_s)

    logger = MagicMock()
    n = Notifier(
        _base_config(telegram={'enabled': 0}, mail=_mail_cfg()),
        logger,
        smtp_factory=factory,
    )
    assert n._send_mail('t', 'b', 'error') is False
    assert any('authentication failed' in str(c.args[0]).lower()
               for c in logger.error.call_args_list)


def test_mail_ssl_port_skips_starttls():
    _FakeSMTP.instances.clear()

    def factory(host, port, timeout_s, use_ssl):
        assert use_ssl is True
        assert port == 465
        return _FakeSMTP(host, port, timeout=timeout_s)

    n = Notifier(
        _base_config(
            telegram={'enabled': 0},
            mail=_mail_cfg(smtp_port=465, use_ssl=1, use_tls=0),
        ),
        MagicMock(),
        smtp_factory=factory,
    )
    assert n._send_mail('t', 'b', 'info') is True
    assert _FakeSMTP.instances[0].started_tls is False


# ---------------------------------------------------------------------------
# Pushover
# ---------------------------------------------------------------------------

def test_pushover_disabled_does_not_call_api():
    urlopen = MagicMock()
    n = Notifier(
        _base_config(telegram={'enabled': 0}, pushover={'enabled': 0}),
        MagicMock(),
        urlopen_fn=urlopen,
    )
    n.notify_alert('t', 'b', severity='critical')
    urlopen.assert_not_called()


def test_pushover_skips_below_min_severity():
    urlopen = MagicMock()
    n = Notifier(
        _base_config(telegram={'enabled': 0}, pushover=_pushover_cfg()),
        MagicMock(),
        urlopen_fn=urlopen,
    )
    # warning < critical min
    assert n._send_pushover('t', 'b', 'warning') is False
    urlopen.assert_not_called()


def test_pushover_critical_emergency_payload():
    urlopen = MagicMock(
        return_value=_FakeResponse(json.dumps({'status': 1, 'request': 'abc'}))
    )
    logger = MagicMock()
    n = Notifier(
        _base_config(telegram={'enabled': 0}, pushover=_pushover_cfg()),
        logger,
        urlopen_fn=urlopen,
    )
    assert n._send_pushover('Controller down', 'no heartbeat', 'critical') is True

    urlopen.assert_called_once()
    request = urlopen.call_args.args[0]
    assert request.full_url == 'https://api.pushover.net/1/messages.json'
    assert request.get_method() == 'POST'
    form = urllib.parse.parse_qs(request.data.decode('utf-8'))
    assert form['token'] == ['app-token']
    assert form['user'] == ['user-key']
    assert form['priority'] == ['2']
    assert form['retry'] == ['60']
    assert form['expire'] == ['900']
    assert form['sound'] == ['siren']
    assert form['title'][0].startswith('🚨 CRITICAL Controller down')
    assert form['message'] == ['no heartbeat']
    assert any('Pushover alert sent' in str(c.args[0]) for c in logger.info.call_args_list)


def test_pushover_missing_credentials_logs_error():
    urlopen = MagicMock()
    logger = MagicMock()
    n = Notifier(
        _base_config(
            telegram={'enabled': 0},
            pushover=_pushover_cfg(api_token='YOUR_PUSHOVER_APP_TOKEN'),
        ),
        logger,
        urlopen_fn=urlopen,
    )
    assert n._send_pushover('t', 'b', 'critical') is False
    urlopen.assert_not_called()
    assert logger.error.called


def test_pushover_env_overrides(monkeypatch):
    monkeypatch.setenv('ESSBATT_PUSHOVER_TOKEN', 'env-app')
    monkeypatch.setenv('ESSBATT_PUSHOVER_USER', 'env-user')
    urlopen = MagicMock(
        return_value=_FakeResponse(json.dumps({'status': 1}))
    )
    n = Notifier(
        _base_config(
            telegram={'enabled': 0},
            pushover=_pushover_cfg(api_token='cfg-app', user_key='cfg-user'),
        ),
        MagicMock(),
        urlopen_fn=urlopen,
    )
    assert n._send_pushover('t', 'b', 'critical') is True
    form = urllib.parse.parse_qs(urlopen.call_args.args[0].data.decode('utf-8'))
    assert form['token'] == ['env-app']
    assert form['user'] == ['env-user']


def test_pushover_api_error_status():
    urlopen = MagicMock(
        return_value=_FakeResponse(
            json.dumps({'status': 0, 'errors': ['application token is invalid']})
        )
    )
    logger = MagicMock()
    n = Notifier(
        _base_config(telegram={'enabled': 0}, pushover=_pushover_cfg()),
        logger,
        urlopen_fn=urlopen,
    )
    assert n._send_pushover('t', 'b', 'critical') is False
    assert any('invalid' in str(c.args[0]).lower() for c in logger.error.call_args_list)


def test_notify_alert_critical_hits_pushover_and_telegram():
    """Both channels fire for critical when enabled."""
    calls = []

    def urlopen(request, timeout=None):
        calls.append(request.full_url)
        if 'pushover.net' in request.full_url:
            return _FakeResponse(json.dumps({'status': 1}))
        return _FakeResponse(json.dumps({'ok': True, 'result': {}}))

    n = Notifier(
        _base_config(
            telegram={
                'enabled': 1,
                'bot_token': '123:ABC',
                'chat_id': '999',
            },
            pushover=_pushover_cfg(),
        ),
        MagicMock(),
        urlopen_fn=urlopen,
    )
    n.notify_alert('fail', 'reason', severity='critical')
    assert any('api.telegram.org' in u for u in calls)
    assert any('pushover.net' in u for u in calls)


def test_warning_does_not_use_pushover_even_if_enabled():
    urlopen = MagicMock(
        return_value=_FakeResponse(json.dumps({'ok': True, 'result': {}}))
    )
    n = Notifier(
        _base_config(
            telegram={
                'enabled': 1,
                'bot_token': '123:ABC',
                'chat_id': '999',
            },
            pushover=_pushover_cfg(min_severity='warning'),
        ),
        MagicMock(),
        urlopen_fn=urlopen,
    )
    n.notify_alert('warm', 't=36', severity='warning')
    # telegram only — no pushover for warning policy
    assert urlopen.called
    for call in urlopen.call_args_list:
        assert 'pushover.net' not in call.args[0].full_url


def test_channels_for_severity_policy():
    n = Notifier(_base_config(telegram={'enabled': 0}), MagicMock())
    assert n.channels_for_severity('info') == frozenset({'mail', 'telegram'})
    assert n.channels_for_severity('warning') == frozenset({'mail', 'telegram'})
    assert n.channels_for_severity('critical') == frozenset(
        {'mail', 'telegram', 'pushover'}
    )
