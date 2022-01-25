import logging
import traceroutehelper

from watchtower.alert.consumers import AbstractConsumer


class TracerouteConsumer(AbstractConsumer):

    default = {
        'timeout': 30
    }

    TEMP_TARGET = {
        'address': '192.172.226.97',
        'prefix': '192.172.226.0/24',
        'label': 'gibi'
    }

    def __init__(self, config):
        super(TracerouteConsumer, self).__init__(config)
        self.config = self.default.update(self.config) if self.config else self.default

    def _init_tracer(self):
        self.tracer = traceroutehelper.ArkTrace()
        self.tracer.set_timeout(self.config['timeout'])

    def handle_alert(self, alert):
        logging.debug("traceroute handling alert")
        # TODO: move this measurement step into a worker thread so that we
        # TODO: can continue
        self._init_tracer()
        for v in alert.violations:
            # TODO: figure out what targets to probe for this expression
            # TODO: E.g., look up the country code in the MDDB
            self.tracer.add_ip_address(**self.TEMP_TARGET)
        # TODO: modify traceroutehelper to return objects...
        self.tracer.start_measurements_all_monitors()
        self.tracer.print_results()

    def handle_error(self, error):
        pass  # we don't care about errors

    def handle_timer(self, now):
        pass
