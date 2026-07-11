# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""User notification channels for essBATT Watchdog (Telegram, Mail, …).

Scaffold only: real channel implementations will be added later.
All notify_* methods are safe no-ops that log the intended message so the
rest of the watchdog can be developed and tested without credentials.
"""


class Notifier:
    """Dispatches alerts to configured notification backends."""

    def __init__(self, config, logger):
        self.logger = logger
        self.update_config(config)

    def update_config(self, config):
        self.config = config
        self.mail_address = config.get('notification_mail_address')
        notifications = config.get('notifications', {})
        self.telegram_enabled = notifications.get('telegram', {}).get('enabled', 0) == 1
        self.mail_enabled = notifications.get('mail', {}).get(
            'enabled',
            1 if self.mail_address and self.mail_address != 'your.mailaddress@abc.com'
            else 0,
        )
        self.telegram_cfg = notifications.get('telegram', {})
        self.mail_cfg = notifications.get('mail', {})

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

    def _send_telegram(self, title, body, severity):
        """TODO: implement Telegram Bot API delivery."""
        chat_id = self.telegram_cfg.get('chat_id', 'unset')
        self.logger.info(
            'Telegram notification stub (chat_id='
            + str(chat_id)
            + '): ' + title + ' | ' + body
        )
