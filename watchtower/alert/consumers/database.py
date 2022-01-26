import logging
import sqlalchemy
import sqlalchemy.engine.url

from . import AbstractConsumer


class DatabaseConsumer(AbstractConsumer):

    defaults = {
        'drivername': 'sqlite',
        'username': None,
        'password': None,
        'host': './watchtower-alerts.db',
        'port': None,
        'databasename': None,
        'engine_params': {},
        'table_prefix': 'watchtower',
        'alert_table_name': 'alert',
        'error_table_name': 'error',
    }

    def __init__(self, config):
        super(DatabaseConsumer, self).__init__(self.defaults)
        if config:
            self.config.update(config)

    def start(self):
        self._init_db()

    def _init_db(self):
        meta = sqlalchemy.MetaData()

        t_alert_name = self._table_name('alert')
        self.t_alert = sqlalchemy.Table(
            t_alert_name,
            meta,

            # Alert columns
            sqlalchemy.Column('id', sqlalchemy.Integer,
                              sqlalchemy.Sequence('watchtower_alert_id_seq'),
                              primary_key=True),
            sqlalchemy.Column('fqid', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('name', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('query_time', sqlalchemy.Integer, nullable=False),
            sqlalchemy.Column('level', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('method', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('query_expression', sqlalchemy.Text),
            sqlalchemy.Column('history_query_expression', sqlalchemy.Text),

            # Violation columns, some of which could be null when no data, back to normal, etc
            sqlalchemy.Column('time', sqlalchemy.Integer),
            sqlalchemy.Column('expression', sqlalchemy.Text),
            sqlalchemy.Column('condition', sqlalchemy.String),
            sqlalchemy.Column('value', sqlalchemy.Float),
            sqlalchemy.Column('history_value', sqlalchemy.Float),

            # Metadata columns, which some violations do not have
            sqlalchemy.Column('meta_type', sqlalchemy.String),
            sqlalchemy.Column('meta_code', sqlalchemy.String),

            sqlalchemy.UniqueConstraint('fqid', 'time', 'level', 'expression'),
            sqlalchemy.Index(t_alert_name + '_type_idx', 'meta_type'),
            sqlalchemy.Index(t_alert_name + '_type_code_idx', 'meta_type', 'meta_code')
        )

        self.t_error = sqlalchemy.Table(
            self._table_name('error'),
            meta,
            sqlalchemy.Column('id', sqlalchemy.Integer,
                              sqlalchemy.Sequence('watchtower_error_id_seq'),
                              primary_key=True),
            sqlalchemy.Column('fqid', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('name', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('query_time', sqlalchemy.Integer, nullable=False),
            sqlalchemy.Column('query_expression', sqlalchemy.String),
            sqlalchemy.Column('history_query_expression', sqlalchemy.String),
            sqlalchemy.Column('type', sqlalchemy.String, nullable=False),
            sqlalchemy.Column('message', sqlalchemy.String, nullable=False),
            sqlalchemy.UniqueConstraint('fqid', 'query_time', 'type',
                                        'message')
        )

        engine_options = [self.config[n] for n in (
            'drivername', 'username', 'password', 'host', 'port',
            'databasename')]
        if 'sqlite' in engine_options[0] and engine_options[3]:
            engine_options[3] = '/' + engine_options[3] # host

        self.url = str(sqlalchemy.engine.url.URL(*engine_options))

        # Its a little unsafe to log this since it may have a password:
        # logging.debug('Database engine url: %s', self.url)

        self.engine = sqlalchemy.create_engine(self.url,
                                               **self.config['engine_params'])
        meta.create_all(self.engine)

    def _table_name(self, table):
        suffix = self.config['%s_table_name' % table]
        return "%s_%s" % (self.config['table_prefix'], suffix) \
            if self.config['table_prefix'] else suffix

    def handle_alert(self, alert):
        logging.debug("DB consumer handling alert")
        # we need violation annotations, so ensure that has been done
        alert.annotate_violations()
        with self.engine.connect() as conn:
            adict = alert.as_dict()
            vdicts = adict.pop('violations')
            for vdict in vdicts:
                mdict = vdict.pop('meta')
                if mdict is None:
                    mdict = {
                        'meta_type': None,
                        'meta_code': None,
                    }
                vdict.update({
                    'fqid': adict['fqid'],
                    'name': adict['name'],
                    'level': adict['level'],
                    'query_time': adict['time'],
                    'query_expression': adict['expression'],
                    'history_query_expression': adict['history_expression'],
                    'method': adict['method'],
                    'meta_type': mdict['meta_type'],
                    'meta_code': mdict['meta_code'] if "meta_code" in mdict else None
                })
                # get rid of the history column
                vdict.pop("history")

            # dirty hax below. should do a select first
            try:
                ins = self.t_alert.insert().values(vdicts)
                res = conn.execute(ins)
            except sqlalchemy.exc.IntegrityError as e:
                logging.warn("Alert insert failed (maybe it already exists?)")
                logging.debug(e)

    def handle_error(self, error):
        logging.debug("DB consumer handling error")
        with self.engine.connect() as conn:
            edict = error.as_dict()
            edict.update({
                'query_time': edict.pop('time'),
                'query_expression': edict.pop('expression'),
                'history_query_expression': edict.pop('history_expression')
            })

            try:
                ins = self.t_error.insert().values(**edict)
                conn.execute(ins)
            except sqlalchemy.exc.IntegrityError as e:
                logging.warn("Error insert failed (maybe it already exists?)")
                logging.debug(e)

    def handle_timer(self, now):
        pass
