import logging
import _pytimeseries

from . import AbstractConsumer


class TimeseriesConsumer(AbstractConsumer):

    defaults = {
        'interval': 60,
        'backends': ['ascii'],
        'ascii-opts': "",
        'metric_prefix': 'projects.ioda.alerts',
        'level_leaf': 'alert_level',
        'delta_leaf': 'delta_pct_x100',
        'producer_repeat_interval': 7200,  # 2 hours
        'producer_max_interval': 600,
        'alert_reset_timeout': 7860,
    }

    level_values = {
        'normal': 0,
        'warning': 1,
        'critical': 2,
    }

    def __init__(self, config):
        super(TimeseriesConsumer, self).__init__(self.defaults)
        if config:
            self.config.update(config)
        self.alert_state = {}
        self.ts = None
        self.no_alert_timeout = self.config['alert_reset_timeout']

    def start(self):
        # [alert.name] => 'int_start', 'last_time', 'kp'
        self._init_ts()
        logging.debug("Missed alert timeout: %s" % self.no_alert_timeout)

    def _init_ts(self):
        logging.info("Initializing PyTimeseries")
        self.ts = _pytimeseries.Timeseries()
        for name in self.config['backends']:
            logging.info("Enabling timeseries backend '%s'" % name)
            be = self.ts.get_backend_by_name(name)
            if not be:
                logging.error("Could not enable TS backend %s" % name)
            opts = self.config[name+'-opts'] if name+'-opts' in self.config else ""
            self.ts.enable_backend(be, opts)

        logging.debug("Creating new Key Package")

    def handle_alert(self, alert):
        # get the state for this alert type
        if alert.name in self.alert_state:
            state = self.alert_state[alert.name]
        else:
            state = {
                'int_start': self.compute_interval_start(alert.time),
                'last_time': alert.time,
                'kp': self.ts.new_keypackage(reset=False),
                'violations_last_times': {}  # violation_idx: violation_last_time
            }
            self.alert_state[alert.name] = state

        self._maybe_flush_kp(state, alert.time)

        # we need meta, so make sure it is loaded
        alert.annotate_violations()
        not_updated_viols = dict(state['violations_last_times'])
        for v in alert.violations:
            if v.meta is None:
                continue

            # create the alert_level metric
            key = self._build_key(alert, v, self.config['level_leaf'])
            # logging.debug("Key: %s" % key)
            idx = state['kp'].get_key(key)
            if idx is None:
                idx = state['kp'].add_key(key)
            state['kp'].set(idx, self.level_values[alert.level])
            # Update last modified time for this metric
            state['violations_last_times'][key] = alert.time
            not_updated_viols.pop(key, None)

            # create the delta_pct leaf
            key = self._build_key(alert, v, self.config['delta_leaf'])
            # logging.debug("Key: %s" % key)
            idx = state['kp'].get_key(key)
            if idx is None:
                idx = state['kp'].add_key(key)
            delta_pct = 0
            if alert.level != 'normal':
                # compute percentage drop then * 100 to allow storage in int
                delta_pct = int((abs(v.history_value - v.value) / max(v.history_value, v.value)) * 100 * 100)
            state['kp'].set(idx, delta_pct)
            # Update last modified time for this metric
            state['violations_last_times'][key] = alert.time
            not_updated_viols.pop(key, None)

        self._reset_violations_level(not_updated_viols, state['kp'], alert.time)

    def _build_key(self, alert, violation, leaf):
        # "projects.ioda.alerts.[ALERT-FQID].[META-FQID].alert_level
        return '.'\
            .join((self.config['metric_prefix'], alert.fqid,
                   violation.meta['fqid'],
                   leaf)).encode()

    def _maybe_flush_kp(self, state, time):
        this_int_start = self.compute_interval_start(time)
        if time < state['last_time']:
            logging.error('Time is going backwards! Time: %d Last Time: %d'
                          % (time, state['last_time']))
            return
        state['last_time'] = time
        if not state['int_start']:
            state['int_start'] = this_int_start
            return
        if this_int_start <= state['int_start']:
            return

        while state['int_start'] < this_int_start:
            state['kp'].flush(state['int_start'])
            state['int_start'] += self.config['interval']

    def compute_interval_start(self, time):
        return int(time / self.config['interval']) * self.config['interval']

    def handle_error(self, error):
        pass

    def handle_timer(self, now):
        logging.debug("Flushing all KPs...")
        # flush the kps
        for name, state in self.alert_state.items():
            logging.debug("Flushing KP for %s" % name)

            self._reset_violations_level(state['violations_last_times'], state['kp'], now)
            state['kp'].flush(state['int_start'])

    def _reset_violations_level(self, violations, kp, now):
        """Reset level of a series to normal when no violation of it is received
        for too long, assuming it has came back to normal.

        :param dict violations:
        :param int now:
        """
        if not self.no_alert_timeout:
            return
        for key, last_time in violations.items():
            if now - last_time >= self.no_alert_timeout:
                idx = kp.get_key(key)
                kp.set(idx, self.level_values['normal'])
