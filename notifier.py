# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""User notification channels for essBATT Watchdog (Telegram, Mail, …).

Telegram uses the Bot API ``sendMessage`` endpoint over HTTPS (stdlib only).
Credentials may come from ``watchdog_config.json`` or environment variables
(preferred for secrets that must not be committed):

  ESSBATT_TELEGRAM_BOT_TOKEN
  ESSBATT_TELEGRAM_CHAT_ID
"""

import json
import os
import urllib.error
import urllib.request


TELEGRAM_API_BASE = 'https://api.telegram.org'


class Notifier:
    """Dispatches alerts to configured notification backends."""

    def __init__(self, config, logger, urlopen_fn=None):
        """
        Args:
            config: watchdog_config dict
            logger: logger instance
            urlopen_fn: injectable urllib opener (tests); default urllib.request.urlopen
        """
        self.logger = logger
        self._urlopen = urlopen_fn or urllib.request.urlopen
        self.update_config(config)

    def update_config(self, config):
        self.config = config
        self.mail_address = config.get('notification_mail_address')
        notifications = config.get('notifications', {})
        self.telegram_cfg = dict(notifications.get('telegram', {}) or {})
        self.mail_cfg = dict(notifications.get('mail', {}) or {})

        # Env overrides win over config (keeps secrets out of git)
        env_token = os.environ.get('ESSBATT_TELEGRAM_BOT_TOKEN', '').strip()
        env_chat = os.environ.get('ESSBATT_TELEGRAM_CHAT_ID', '').strip()
        if env_token:
            self.telegram_cfg['bot_token'] = env_token
        if env_chat:
            self.telegram_cfg['chat_id'] = env_chat

        self.telegram_enabled = self.telegram_cfg.get('enabled', 0) == 1
        if 'enabled' in self.mail_cfg:
            self.mail_enabled = self.mail_cfg.get('enabled') == 1
        else:
            self.mail_enabled = bool(
                self.mail_address
                and self.mail_address != 'your.mailaddress@abc.com'
            )

    def notify_alert(self, title, body, severity='error'):
        """Send an alert on all enabled channels.

        Args:
            title: short subject / headline
            body: longer description
            severity: 'info' | 'warning' | 'error' | 'critical'
        """
        text = '[' + severity.upper() + '] ' + title + ' — ' + body
        self.logger.warning('NOTIFY: ' + text)

        if self.mail_enabled:
            self._send_mail(title, body, severity)
        if self.telegram_enabled:
            self._send_telegram(title, body, severity)

    def notify_recovery(self, title, body):
        """Notify that a previously failing condition recovered."""
        self.notify_alert(title, body, severity='info')

    def _send_mail(self, title, body, severity):
        """TODO: implement SMTP / API mail delivery."""
        self.logger.info(
            'Mail notification stub (to='
            + str(self.mail_address)
            + '): ' + title + ' | ' + body
        )

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
                'Set notifications.telegram in watchdog_config.json or env '
                'ESSBATT_TELEGRAM_BOT_TOKEN / ESSBATT_TELEGRAM_CHAT_ID. '
                'Note: chat_id is a numeric id (or @channel), NOT the t.me bot URL.'
            )
            return False

        text = (
            '[' + str(severity).upper() + '] ' + str(title) + '\n' + str(body)
        )
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
                    # Never log the full token; description is enough
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
