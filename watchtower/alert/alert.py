import json
import requests
import sys

# Shut requests up
import warnings
warnings.filterwarnings('once', r'.*InsecurePlatformWarning.*')
import logging
logging.getLogger("requests").setLevel(logging.WARNING)

class Alert:

    LEVELS = ['critical', 'warning', 'normal', 'error']
    IODA_ENTITY_API = "http://api.ioda.inetintel.cc.gatech.edu/v2/entities"

    def __init__(self, fqid, name, level, time, expression, history_expression,
                 method, violations=None):
        self.fqid = fqid
        self.name = name
        self.level = level
        self.time = time
        self.expression = expression
        self.history_expression = history_expression
        self.method = method
        self.violations = violations

        self.violations_annotated = False

    def __repr__(self):
        return json.dumps(self.as_dict())

    @classmethod
    def from_json(cls, json_str):
        obj = json.loads(json_str)
        # convert violations to objects
        obj['violations'] = [Violation(**viol) for viol in obj['violations']]
        return Alert(**obj)

    def as_dict(self):
        return {
            'fqid': self.fqid,
            'name': self.name,
            'level': self.level,
            'time': self.time,
            'expression': self.expression,
            'history_expression': self.history_expression,
            'method': self.method,
            'violations': [v.as_dict() for v in self.violations],
        }

    def annotate_violations(self):
        if self.violations_annotated:
            return
        # collect all the expressions from violations that don't already have
        # a meta set
        expressions = set()
        for v in self.violations:
            if v.meta is None and "/" in v.expression:
                expressions.add(v.expression)

        if not len(expressions):
            # nothing to do...
            self.violations_annotated = True
            return

        metas = {}
        for exp in expressions:
            resp = requests.get(self.IODA_ENTITY_API + "/" + exp)
            try:
                res = resp.json()
            except Exception as e:
                logging.error('IODA entity annotation %s failed with JSON decode error: %s' % (exp, e.msg))
                continue

            if not res or 'data' not in res or not res['data']:
                logging.error('IODA entity annotation %s failed with error: %s' %
                               (exp, res['error'] if res else None))
                continue

            try:
                metas[exp] = {
                    "meta_type": res["data"][0]["type"],
                    "fqid": res["data"][0]["attrs"]["fqid"],
                    "meta_code": res["data"][0]["code"]
                }
            except Exception as e:
                logging.error('Unable to parse IODA entity annotation %s: %s' % (exp, e))

        # now assign meta to each violation
        for v in self.violations:
            if v.expression in metas:
                v.meta = metas[v.expression]
        self.violations_annotated = True

    @property
    def fqid(self):
        return self._fqid

    @fqid.setter
    def fqid(self, v):
        self._fqid = v

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, v):
        self._name = v

    @property
    def level(self):
        return self._level

    @level.setter
    def level(self, v):
        if v not in self.LEVELS:
            raise TypeError('Alert level must be one of %s' % self.LEVELS)
        self._level = v

    @property
    def time(self):
        return self._time

    @time.setter
    def time(self, v):
        if not isinstance(v, int):
            raise TypeError('Alert time must be an integer (UTC epoch time)')
        self._time = v

    @property
    def expression(self):
        return self._expression

    @expression.setter
    def expression(self, v):
        self._expression = v

    @property
    def history_expression(self):
        return self._history_expression

    @history_expression.setter
    def history_expression(self, v):
        self._history_expression = v

    @property
    def method(self):
        return self._method

    @method.setter
    def method(self, v):
        self._method = v

    @property
    def violations(self):
        return self._violations

    @violations.setter
    def violations(self, v):
        if not all(isinstance(viol, Violation) for viol in v):
            raise TypeError('Alert violations must be of type Violation')
        self._violations = v


class Violation:

    def __init__(self, expression, condition, value, history_value, history, time,
                 meta=None):
        self.expression = expression
        self.condition = condition
        self.value = value
        self.history_value = history_value
        self.history = history
        self.time = time
        self.meta = meta

    def __repr__(self):
        return json.dumps(self.as_dict())

    def as_dict(self):
        return {
            'expression': self.expression,
            'condition': self.condition,
            'value': self.value,
            'history_value': self.history_value,
            'history': self.history,
            'time': self.time,
            'meta': self.meta,
        }

    @property
    def expression(self):
        return self._expression

    @expression.setter
    def expression(self, v):
        self._expression = v

    @property
    def condition(self):
        return self._condition

    @condition.setter
    def condition(self, v):
        self._condition = v

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, v):
        self._value = v

    @property
    def time(self):
        return self._time

    @time.setter
    def time(self, v):
        self._time = v

    @property
    def history_value(self):
        return self._history_value

    @history_value.setter
    def history_value(self, v):
        self._history_value = v

    @property
    def history(self):
        return self._history

    @history.setter
    def history(self, v):
        if v is not None and not isinstance(v, list):
            raise TypeError('Violation history must be a list')
        self._history = v

    @property
    def meta(self):
        return self._meta

    @meta.setter
    def meta(self, meta):
        self._meta = meta
