# Portions of this source code are Copyright (c) 2021 Georgia Tech Research
# Corporation. All Rights Reserved. Permission to copy, modify, and distribute
# this software and its documentation for academic research and education
# purposes, without fee, and without a written agreement is hereby granted,
# provided that the above copyright notice, this paragraph and the following
# three paragraphs appear in all copies. Permission to make use of this
# software for other than academic research and education purposes may be
# obtained by contacting:
#
#  Office of Technology Licensing
#  Georgia Institute of Technology
#  926 Dalney Street, NW
#  Atlanta, GA 30318
#  404.385.8066
#  techlicensing@gtrc.gatech.edu
#
# This software program and documentation are copyrighted by Georgia Tech
# Research Corporation (GTRC). The software program and documentation are 
# supplied "as is", without any accompanying services from GTRC. GTRC does
# not warrant that the operation of the program will be uninterrupted or
# error-free. The end-user understands that the program was developed for
# research purposes and is advised not to rely exclusively on the program for
# any reason.
#
# IN NO EVENT SHALL GEORGIA TECH RESEARCH CORPORATION BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
# EVEN IF GEORGIA TECH RESEARCH CORPORATION HAS BEEN ADVISED OF THE POSSIBILITY
# OF SUCH DAMAGE. GEORGIA TECH RESEARCH CORPORATION SPECIFICALLY DISCLAIMS ANY
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED
# HEREUNDER IS ON AN "AS IS" BASIS, AND  GEORGIA TECH RESEARCH CORPORATION HAS
# NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
# MODIFICATIONS.

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
                vsplit = v.expression.split("/")
                if len(vsplit) >= 2:
                    enttype = vsplit[0]
                    entcode = vsplit[1]
                    expressions.add((enttype, entcode))

        if not len(expressions):
            # nothing to do...
            self.violations_annotated = True
            return

        metas = {}
        for exp in expressions:
            expkey = exp[0] + "/" + exp[1]
            resp = requests.get(self.IODA_ENTITY_API + "?entityType=" + exp[0] + "&entityCode=" + exp[1])
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
                metas[expkey] = {
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
