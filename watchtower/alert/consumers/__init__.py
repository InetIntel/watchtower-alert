import abc


class AbstractConsumer(metaclass=abc.ABCMeta):
    def __init__(self, config):
        self.config = config

    def start(self):
        pass

    @abc.abstractmethod
    def handle_alert(self, alert):
        pass

    @abc.abstractmethod
    def handle_error(self, error):
        pass

    @abc.abstractmethod
    def handle_timer(self, now):
        pass

# When adding a consumer here, also add to _init_plugins method in
# watchtower.alert.consumer.py
# TODO: make adding consumer more dynamic
from .log import LogConsumer
# from watchtower.alert.consumers.email import EmailConsumer
from .database import DatabaseConsumer
# from watchtower.alert.consumers.traceroute import TracerouteConsumer
from .timeseries import TimeseriesConsumer
from .slack import SlackConsumer