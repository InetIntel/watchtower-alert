import logging

from watchtower.alert.consumers import AbstractConsumer


class LogConsumer(AbstractConsumer):

    loggers = {
        'normal': logging.info,
        'warning': logging.warn,
        'critical': logging.error,
    }

    def handle_alert(self, alert):
        log_str = "ALERT: %s %s %d (%s)" %\
                  (alert.level.upper(), alert.name, alert.time,
                   alert.expression)
        self.loggers[alert.level](log_str)

        for v in alert.violations:
            log_str = "VIOLATION: %s Time: %d %s Value: %s History Value: %s" %\
                      (alert.level.upper(), alert.time, v.expression, v.value,
                       v.history_value)
            self.loggers[alert.level](log_str)

    def handle_error(self, error):
        log_str = "ERROR: %s %s %d %s %s" % (error.type, error.name,
                                             error.time, error.expression,
                                             error.message)
        logging.error(log_str)

    def handle_timer(self, now):
        logging.info("TIMER: periodic timer fired at %d" % now)
