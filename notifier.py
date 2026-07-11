# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""User notification channels for essBATT Watchdog (Telegram, Mail, Pushover).

Telegram: Bot API ``sendMessage`` (HTTPS, stdlib).
Mail: SMTP via ``smtplib`` + ``email.message`` (stdlib).
Pushover: ``https://api.pushover.net/1/messages.json`` (HTTPS form POST, stdlib).

Credentials may come from config (prefer ``watchdog_config.local.json``) or
environment variables (env wins when set):

  ESSBATT_TELEGRAM_BOT_TOKEN
  ESSBATT_TELEGRAM_CHAT_ID
  ESSBATT_MAIL_TO
  ESSBATT_MAIL_FROM
  ESSBATT_MAIL_SMTP_HOST
  ESSBATT_MAIL_SMTP_PORT
  ESSBATT_MAIL_SMTP_USER
  ESSBATT_MAIL_SMTP_PASSWORD
  ESSBATT_PUSHOVER_TOKEN
  ESSBATT_PUSHOVER_USER
"""

import json
import os
import smtplib
import ssl
import urllib.error
import urllib.parse
import urllib.request
from email.message import EmailMessage


TELEGRAM_API_BASE = 'https://api.telegram.org'
PUSHOVER_API_URL = 'https://api.pushover.net/1/messages.json'

# Values treated as "not configured" for mail
_MAIL_PLACEHOLDERS = frozenset({
    '',
    'your.mailaddress@abc.com',
    'smtp.example.com',
    'essbatt-watchdog@example.com',
    'YOUR_SMTP_USER',
    'YOUR_SMTP_PASSWORD',
})

_PUSHOVER_PLACEHOLDERS = frozenset({
    '',
    'YOUR_PUSHOVER_APP_TOKEN',
    'YOUR_PUSHOVER_USER_KEY',
})

# Visual severity markers (especially useful in Telegram notification previews)
_SEVERITY_EMOJI = {
    'critical': '🚨',
    'error': '❌',
    'warning': '⚠️',
    'info': 'ℹ️',
}

_SEVERITY_RANK = {
    'info': 0,
    'warning': 1,
    'error': 2,
    'critical': 3,
}

# Which channels may fire for each severity (only if that channel is enabled).
# info/warning → Mail + Telegram; critical → all configured channels incl. Pushover.
_CHANNELS_BY_SEVERITY = {
    'info': frozenset({'mail', 'telegram'}),
    'warning': frozenset({'mail', 'telegram'}),
    'error': frozenset({'mail', 'telegram'}),
    'critical': frozenset({'mail', 'telegram', 'pushover'}),
}


def format_severity_label(severity):
    """Return e.g. '🚨 CRITICAL' for notification titles/bodies."""
    key = str(severity or 'error').strip().lower()
    emoji = _SEVERITY_EMOJI.get(key, '▪️')
    return emoji + ' ' + key.upper()


def severity_rank(severity):
    """Numeric rank for min-severity filtering (unknown → error)."""
    key = str(severity or 'error').strip().lower()
    return _SEVERITY_RANK.get(key, _SEVERITY_RANK['error'])


class Notifier:
    """Dispatches alerts to configured notification backends."""

    def __init__(self, config, logger, urlopen_fn=None, smtp_factory=None):
        """
        Args:
            config: watchdog_config dict
            logger: logger instance
            urlopen_fn: injectable urllib opener (tests)
            smtp_factory: optional callable(host, port, timeout_s, use_ssl)
                returning an SMTP-like object (tests). Default uses smtplib.
        """
        self.logger = logger
        self._urlopen = urlopen_fn or urllib.request.urlopen
        self._smtp_factory = smtp_factory
        self.update_config(config)

    def update_config(self, config):
        self.config = config
        notifications = config.get('notifications', {})
        self.telegram_cfg = dict(notifications.get('telegram', {}) or {})
        self.mail_cfg = dict(notifications.get('mail', {}) or {})
        self.pushover_cfg = dict(notifications.get('pushover', {}) or {})

        # --- Telegram env overrides ---
        env_token = os.environ.get('ESSBATT_TELEGRAM_BOT_TOKEN', '').strip()
        env_chat = os.environ.get('ESSBATT_TELEGRAM_CHAT_ID', '').strip()
        if env_token:
            self.telegram_cfg['bot_token'] = env_token
        if env_chat:
            self.telegram_cfg['chat_id'] = env_chat

        # --- Mail env overrides ---
        env_map = {
            'ESSBATT_MAIL_TO': 'to_address',
            'ESSBATT_MAIL_FROM': 'from_address',
            'ESSBATT_MAIL_SMTP_HOST': 'smtp_host',
            'ESSBATT_MAIL_SMTP_PORT': 'smtp_port',
            'ESSBATT_MAIL_SMTP_USER': 'smtp_user',
            'ESSBATT_MAIL_SMTP_PASSWORD': 'smtp_password',
        }
        for env_key, cfg_key in env_map.items():
            val = os.environ.get(env_key, '').strip()
            if val:
                if cfg_key == 'smtp_port':
                    try:
                        self.mail_cfg[cfg_key] = int(val)
                    except ValueError:
                        self.logger.warning(
                            'Ignoring invalid ' + env_key + '=' + val
                        )
                else:
                    self.mail_cfg[cfg_key] = val

        # --- Pushover env overrides ---
        env_po_token = os.environ.get('ESSBATT_PUSHOVER_TOKEN', '').strip()
        env_po_user = os.environ.get('ESSBATT_PUSHOVER_USER', '').strip()
        if env_po_token:
            self.pushover_cfg['api_token'] = env_po_token
        if env_po_user:
            self.pushover_cfg['user_key'] = env_po_user

        self.telegram_enabled = self.telegram_cfg.get('enabled', 0) == 1
        self.mail_enabled = self.mail_cfg.get('enabled', 0) == 1
        self.pushover_enabled = self.pushover_cfg.get('enabled', 0) == 1

    def channels_for_severity(self, severity):
        """Return the set of channel names allowed for this severity."""
        key = str(severity or 'error').strip().lower()
        return _CHANNELS_BY_SEVERITY.get(
            key, _CHANNELS_BY_SEVERITY['error']
        )

    def notify_alert(self, title, body, severity='error'):
        """Send an alert on enabled channels allowed for this severity.

        Channel policy (when the channel itself is enabled in config):
          * info / warning / error → mail + telegram
          * critical → mail + telegram + pushover

        Args:
            title: short subject / headline
            body: longer description
            severity: 'info' | 'warning' | 'error' | 'critical'
        """
        label = format_severity_label(severity)
        text = label + ' ' + str(title) + ' — ' + str(body)
        self.logger.warning('NOTIFY: ' + text)

        channels = self.channels_for_severity(severity)
        if self.mail_enabled and 'mail' in channels:
            self._send_mail(title, body, severity)
        if self.telegram_enabled and 'telegram' in channels:
            self._send_telegram(title, body, severity)
        if self.pushover_enabled and 'pushover' in channels:
            self._send_pushover(title, body, severity)

    def notify_recovery(self, title, body):
        """Notify that a previously failing condition recovered."""
        self.notify_alert(title, body, severity='info')

    # ------------------------------------------------------------------
    # Mail (SMTP)
    # ------------------------------------------------------------------
    def _mail_settings(self):
        """Return dict of SMTP settings or None if incomplete."""
        to_addr = str(self.mail_cfg.get('to_address') or '').strip()
        host = str(self.mail_cfg.get('smtp_host') or '').strip()
        from_addr = str(self.mail_cfg.get('from_address') or '').strip()
        user = str(self.mail_cfg.get('smtp_user') or '').strip()
        password = str(self.mail_cfg.get('smtp_password') or '')

        if not to_addr or to_addr in _MAIL_PLACEHOLDERS:
            return None
        if not host or host in _MAIL_PLACEHOLDERS:
            return None
        if not from_addr or from_addr in _MAIL_PLACEHOLDERS:
            return None

        try:
            port = int(self.mail_cfg.get('smtp_port', 587))
        except (TypeError, ValueError):
            port = 587

        use_ssl = self.mail_cfg.get('use_ssl', 0) == 1
        # Default: STARTTLS on 587 when not using implicit SSL
        use_tls = self.mail_cfg.get('use_tls', 0 if use_ssl else 1) == 1
        timeout_s = float(self.mail_cfg.get('timeout_s', 15))

        return {
            'to_address': to_addr,
            'from_address': from_addr,
            'smtp_host': host,
            'smtp_port': port,
            'smtp_user': user if user and user not in _MAIL_PLACEHOLDERS else '',
            'smtp_password': password if password not in _MAIL_PLACEHOLDERS else '',
            'use_ssl': use_ssl,
            'use_tls': use_tls,
            'timeout_s': timeout_s,
        }

    def _create_smtp(self, host, port, timeout_s, use_ssl):
        if self._smtp_factory is not None:
            return self._smtp_factory(host, port, timeout_s, use_ssl)
        if use_ssl:
            context = ssl.create_default_context()
            return smtplib.SMTP_SSL(host, port, timeout=timeout_s, context=context)
        return smtplib.SMTP(host, port, timeout=timeout_s)

    def _send_mail(self, title, body, severity):
        """Send an email via SMTP. Returns True on success."""
        settings = self._mail_settings()
        if settings is None:
            self.logger.error(
                'Mail enabled but incomplete SMTP config. Need '
                'notifications.mail.to_address, smtp_host, from_address. '
                'Prefer watchdog_config.local.json or ESSBATT_MAIL_* env vars.'
            )
            return False

        label = format_severity_label(severity)
        subject = '[essBATT] ' + label + ' ' + str(title)
        text_body = (
            'Severity: ' + label + '\n'
            + 'Title: ' + str(title) + '\n\n'
            + str(body) + '\n'
        )

        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = settings['from_address']
        msg['To'] = settings['to_address']
        msg.set_content(text_body)

        smtp = None
        try:
            smtp = self._create_smtp(
                settings['smtp_host'],
                settings['smtp_port'],
                settings['timeout_s'],
                settings['use_ssl'],
            )
            # Some mocks / servers need ehlo before starttls
            if hasattr(smtp, 'ehlo'):
                try:
                    smtp.ehlo()
                except Exception:
                    pass
            if settings['use_tls'] and not settings['use_ssl']:
                context = ssl.create_default_context()
                smtp.starttls(context=context)
                if hasattr(smtp, 'ehlo'):
                    try:
                        smtp.ehlo()
                    except Exception:
                        pass
            if settings['smtp_user']:
                smtp.login(settings['smtp_user'], settings['smtp_password'])
            smtp.send_message(msg)
            self.logger.info(
                'Mail alert sent (to=' + settings['to_address'] + ').'
            )
            return True
        except smtplib.SMTPAuthenticationError:
            self.logger.error(
                'Mail SMTP authentication failed (check smtp_user / smtp_password).'
            )
            return False
        except smtplib.SMTPException as e:
            self.logger.error('Mail SMTP error: ' + str(e))
            return False
        except (OSError, TimeoutError) as e:
            self.logger.error('Mail network/OS error: ' + str(e))
            return False
        except Exception:
            self.logger.exception('Unexpected error while sending mail')
            return False
        finally:
            if smtp is not None:
                try:
                    smtp.quit()
                except Exception:
                    try:
                        smtp.close()
                    except Exception:
                        pass

    # ------------------------------------------------------------------
    # Telegram
    # ------------------------------------------------------------------
    def _telegram_credentials(self):
        """Return (bot_token, chat_id) or (None, None) if incomplete."""
        token = str(self.telegram_cfg.get('bot_token') or '').strip()
        chat_id = str(self.telegram_cfg.get('chat_id') or '').strip()
        if not token or token in ('', 'YOUR_BOT_TOKEN'):
            return None, None
        if not chat_id or chat_id in ('', 'YOUR_CHAT_ID'):
            return None, None
        return token, chat_id

    def _send_telegram(self, title, body, severity):
        """Send a message via Telegram Bot API (sendMessage)."""
        token, chat_id = self._telegram_credentials()
        if token is None:
            self.logger.error(
                'Telegram enabled but bot_token or chat_id is missing/placeholder. '
                'Set notifications.telegram in watchdog_config.local.json or env '
                'ESSBATT_TELEGRAM_BOT_TOKEN / ESSBATT_TELEGRAM_CHAT_ID. '
                'Note: chat_id is a numeric id (or @channel), NOT the t.me bot URL.'
            )
            return False

        label = format_severity_label(severity)
        # Emoji first so it shows in the notification preview line
        text = label + ' ' + str(title) + '\n' + str(body)
        # Telegram limit ~4096 characters
        if len(text) > 4000:
            text = text[:3997] + '...'

        url = TELEGRAM_API_BASE + '/bot' + token + '/sendMessage'
        payload = json.dumps({
            'chat_id': chat_id,
            'text': text,
            'disable_web_page_preview': True,
        }).encode('utf-8')
        request = urllib.request.Request(
            url,
            data=payload,
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        timeout_s = float(self.telegram_cfg.get('timeout_s', 10))

        try:
            with self._urlopen(request, timeout=timeout_s) as response:
                raw = response.read()
                try:
                    data = json.loads(raw.decode('utf-8'))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    self.logger.error(
                        'Telegram API returned non-JSON response: ' + str(raw[:200])
                    )
                    return False
                if not data.get('ok'):
                    self.logger.error(
                        'Telegram sendMessage failed: '
                        + str(data.get('description', data))
                    )
                    return False
                self.logger.info(
                    'Telegram alert sent (chat_id=' + str(chat_id) + ').'
                )
                return True
        except urllib.error.HTTPError as e:
            detail = ''
            try:
                detail = e.read().decode('utf-8', errors='replace')[:300]
            except Exception:
                pass
            self.logger.error(
                'Telegram HTTP error ' + str(e.code) + ': ' + detail
            )
            return False
        except urllib.error.URLError as e:
            self.logger.error('Telegram network error: ' + str(e.reason))
            return False
        except Exception:
            self.logger.exception('Unexpected error while sending Telegram message')
            return False

    # ------------------------------------------------------------------
    # Pushover
    # ------------------------------------------------------------------
    def _pushover_credentials(self):
        """Return (api_token, user_key) or (None, None) if incomplete."""
        token = str(self.pushover_cfg.get('api_token') or '').strip()
        user = str(self.pushover_cfg.get('user_key') or '').strip()
        if not token or token in _PUSHOVER_PLACEHOLDERS:
            return None, None
        if not user or user in _PUSHOVER_PLACEHOLDERS:
            return None, None
        return token, user

    def _pushover_meets_min_severity(self, severity):
        min_sev = str(
            self.pushover_cfg.get('min_severity', 'critical')
        ).strip().lower()
        return severity_rank(severity) >= severity_rank(min_sev)

    def _pushover_priority_for(self, severity):
        """Map severity to Pushover priority (-2…2)."""
        sev = str(severity or 'error').strip().lower()
        key = 'priority_' + sev
        default = {
            'critical': 2,
            'error': 1,
            'warning': 0,
            'info': 0,
        }.get(sev, 0)
        try:
            return int(self.pushover_cfg.get(key, default))
        except (TypeError, ValueError):
            return default

    def _send_pushover(self, title, body, severity):
        """Send a Pushover notification. Returns True on success.

        Critical alerts default to emergency priority (2) with retry/expire so
        the app keeps alerting until the user acknowledges.
        """
        if not self._pushover_meets_min_severity(severity):
            self.logger.debug(
                'Pushover skipped (severity '
                + str(severity)
                + ' below min_severity '
                + str(self.pushover_cfg.get('min_severity', 'critical'))
                + ').'
            )
            return False

        token, user = self._pushover_credentials()
        if token is None:
            self.logger.error(
                'Pushover enabled but api_token or user_key is missing/placeholder. '
                'Set notifications.pushover in watchdog_config.local.json or env '
                'ESSBATT_PUSHOVER_TOKEN / ESSBATT_PUSHOVER_USER.'
            )
            return False

        label = format_severity_label(severity)
        po_title = label + ' ' + str(title)
        message = str(body)
        if len(message) > 1000:
            message = message[:997] + '...'

        priority = self._pushover_priority_for(severity)
        form = {
            'token': token,
            'user': user,
            'title': po_title[:250],
            'message': message if message else po_title,
            'priority': str(priority),
        }

        sound = str(self.pushover_cfg.get('sound') or '').strip()
        if sound and sound != 'none':
            form['sound'] = sound

        # Emergency priority requires retry (>=30s) and expire (<=10800s)
        if priority == 2:
            try:
                retry_s = int(self.pushover_cfg.get('retry_s', 60))
            except (TypeError, ValueError):
                retry_s = 60
            try:
                expire_s = int(self.pushover_cfg.get('expire_s', 900))
            except (TypeError, ValueError):
                expire_s = 900
            retry_s = max(30, retry_s)
            expire_s = max(retry_s, min(10800, expire_s))
            form['retry'] = str(retry_s)
            form['expire'] = str(expire_s)

        data = urllib.parse.urlencode(form).encode('utf-8')
        request = urllib.request.Request(
            PUSHOVER_API_URL,
            data=data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            method='POST',
        )
        timeout_s = float(self.pushover_cfg.get('timeout_s', 15))

        try:
            with self._urlopen(request, timeout=timeout_s) as response:
                raw = response.read()
                try:
                    result = json.loads(raw.decode('utf-8'))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    self.logger.error(
                        'Pushover API returned non-JSON response: ' + str(raw[:200])
                    )
                    return False
                if result.get('status') != 1:
                    # Never log tokens; errors list is enough
                    self.logger.error(
                        'Pushover send failed: ' + str(result.get('errors', result))
                    )
                    return False
                self.logger.info(
                    'Pushover alert sent (priority='
                    + str(priority)
                    + ', severity='
                    + str(severity)
                    + ').'
                )
                return True
        except urllib.error.HTTPError as e:
            detail = ''
            try:
                detail = e.read().decode('utf-8', errors='replace')[:300]
            except Exception:
                pass
            self.logger.error(
                'Pushover HTTP error ' + str(e.code) + ': ' + detail
            )
            return False
        except urllib.error.URLError as e:
            self.logger.error('Pushover network error: ' + str(e.reason))
            return False
        except Exception:
            self.logger.exception('Unexpected error while sending Pushover message')
            return False
