# -*- coding: utf-8 -*-

# Begin of __future__ module
from __future__ import division
# End   of __future__ module

# Begin of external module
import traceback
from kuanke.user_space_api import *
import time
import tushare as ts
import copy
import datetime
import re
from enum import Enum
import lxml.html
import pandas as pd
import requests
import six
from lxml import etree
from pandas.compat import StringIO
from requests import Request
from requests.auth import HTTPBasicAuth
from six.moves.urllib.parse import urlencode
from collections import OrderedDict
import yaml
# End   of external module








class MediaType(Enum):
    DEFAULT = 'application/json'
    JOIN_QUANT = 'application/vnd.joinquant+json'


class ConnectionMethod(Enum):
    DIRECT = 'DIRECT'
    PROXY = 'PROXY'


class Client(object):
    VERSION = 'v1.0'
    KEY_REGEX = r'key=([^&]*)'

    def __init__(self, logger=None, **kwargs):
        if logger is not None:
            self._logger = logger
        else:
            import logging
            self._logger = logging.getLogger(__name__)
        self._connection_method = ConnectionMethod[kwargs.pop('connection_method', 'DIRECT')]
        if self._connection_method is ConnectionMethod.DIRECT:
            self._host = kwargs.pop('host', 'localhost')
            self._port = kwargs.pop('port', 8888)
        else:
            self._proxy_base_url = kwargs.pop('proxy_base_url')
            self._proxy_username = kwargs.pop('proxy_username')
            self._proxy_password = kwargs.pop('proxy_password')
            self._instance_id = kwargs.pop('instance_id')
        self._base_url = self.__create_base_url()
        self._key = kwargs.pop('key', '')
        self._client = kwargs.pop('client', '')
        self._timeout = kwargs.pop('timeout', (5.0, 10.0))

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, value):
        self._host = value

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, value):
        self._port = value

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = value

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        self._timeout = value

    def get_statuses(self, timeout=None):
        request = Request('GET', self.__create_url(None, 'statuses'))
        response = self.__send_request(request, timeout)
        return response.json()

    def get_account(self, client=None, timeout=None):
        request = Request('GET', self.__create_url(client, 'accounts'))
        response = self.__send_request(request, timeout)
        return response.json()

    def get_positions(self, client=None, media_type=MediaType.DEFAULT, timeout=None):
        request = Request('GET', self.__create_url(client, 'positions'))
        request.headers['Accept'] = media_type.value
        response = self.__send_request(request, timeout)
        json = response.json()
        if media_type == MediaType.DEFAULT:
            sub_accounts = pd.DataFrame(json['subAccounts']).T
            positions = pd.DataFrame(json['dataTable']['rows'], columns=json['dataTable']['columns'])
            portfolio = {'sub_accounts': sub_accounts, 'positions': positions}
            return portfolio
        return json

    def get_orders(self, client=None, status="", timeout=None):
        request = Request('GET', self.__create_url(client, 'orders', status=status))
        response = self.__send_request(request, timeout)
        json = response.json()
        df = pd.DataFrame(json['dataTable']['rows'], columns=json['dataTable']['columns'])
        return df

    def buy(self, client=None, timeout=None, **kwargs):
        kwargs['action'] = 'BUY'
        return self.__execute(client, timeout, **kwargs)

    def sell(self, client=None, timeout=None, **kwargs):
        kwargs['action'] = 'SELL'
        return self.__execute(client, timeout, **kwargs)

    def ipo(self, client=None, timeout=None, **kwargs):
        kwargs['action'] = 'IPO'
        return self.__execute(client, timeout, **kwargs)

    def execute(self, client=None, timeout=None, **kwargs):
        return self.__execute(client, timeout, **kwargs)

    def cancel(self, client=None, order_id=None, timeout=None):
        request = Request('DELETE', self.__create_order_url(client, order_id))
        self.__send_request(request, timeout)

    def cancel_all(self, client=None, timeout=None):
        request = Request('DELETE', self.__create_order_url(client))
        self.__send_request(request, timeout)

    def query(self, client=None, navigation=None, timeout=None):
        request = Request('GET', self.__create_url(client, 'reports', navigation=navigation))
        response = self.__send_request(request, timeout)
        json = response.json()
        df = pd.DataFrame(json['dataTable']['rows'], columns=json['dataTable']['columns'])
        return df

    def query_new_stocks(self):
        return self.__query_new_stocks()

    def query_convertible_bonds(self):
        return self.__query_convertible_bonds()

    def purchase_new_stocks(self, client=None, timeout=None):
        today = datetime.datetime.strftime(datetime.datetime.today(), '%Y-%m-%d')
        df = self.query_new_stocks()
        df = df[(df.ipo_date == today)]
        self._logger.info('今日有[{}]支可申购新股'.format(len(df)))
        for index, row in df.iterrows():
            try:
                order = {
                    'symbol': row['xcode'],
                    'price': row['price'],
                    'amountProportion': 'ALL'
                }
                self._logger.info('申购新股：{}'.format(order))
                self.ipo(client, timeout, **order)
            except Exception as e:
                self._logger.error(
                    '客户端[{}]申购新股[{}({})]失败\n{}'.format((client or self._client), row['name'], row['code'], e))

    def purchase_convertible_bonds(self, client=None, timeout=None):
        today = datetime.datetime.strftime(datetime.datetime.today(), '%Y-%m-%d')
        df = self.query_convertible_bonds()
        df = df[(df.ipo_date == today)]
        self._logger.info('今日有[{}]支可申购转债'.format(len(df)))
        for index, row in df.iterrows():
            try:
                order = {
                    'symbol': row['xcode'],
                    'price': 100,
                    'amountProportion': 'ALL'
                }
                self._logger.info('申购转债：{}'.format(order))
                self.buy(client, timeout, **order)
            except Exception as e:
                self._logger.error(
                    '客户端[{}]申购转债[{}({})]失败\n{}'.format((client or self._client), row['bname'], row['xcode'], e))

    def create_adjustment(self, client=None, request_json=None, timeout=None):
        request = Request('POST', self.__create_url(client, 'adjustments'), json=request_json)
        request.headers['Content-Type'] = MediaType.JOIN_QUANT.value
        response = self.__send_request(request, timeout)
        json = response.json()
        return json

    def start_clients(self, timeout=None):
        self.__change_clients_status('LOGGED')

    def shutdown_clients(self, timeout=None):
        self.__change_clients_status('STOPPED')

    def __execute(self, client=None, timeout=None, **kwargs):
        if not kwargs.get('type'):
            kwargs['type'] = 'LIMIT'
        request = Request('POST', self.__create_order_url(client), json=kwargs)
        response = self.__send_request(request)
        return response.json()

    def __change_clients_status(self, status, timeout=None):
        request = Request('PATCH', self.__create_url(None, 'clients'), json={
            'status': status
        })
        self.__send_request(request, timeout)

    def __query_new_stocks(self):
        DATA_URL = 'http://vip.stock.finance.sina.com.cn/corp/view/vRPD_NewStockIssue.php?page=1&cngem=0&orderBy=NetDate&orderType=desc'
        html = lxml.html.parse(DATA_URL)
        res = html.xpath('//table[@id=\"NewStockTable\"]/tr')
        if six.PY2:
            sarr = [etree.tostring(node) for node in res]
        else:
            sarr = [etree.tostring(node).decode('utf-8') for node in res]
        sarr = ''.join(sarr)
        sarr = sarr.replace('<font color="red">*</font>', '')
        sarr = '<table>%s</table>' % sarr
        df = pd.read_html(StringIO(sarr), skiprows=[0, 1])[0]
        df = df.select(lambda x: x in [0, 1, 2, 3, 7], axis=1)
        df.columns = ['code', 'xcode', 'name', 'ipo_date', 'price']
        df['code'] = df['code'].map(lambda x: str(x).zfill(6))
        df['xcode'] = df['xcode'].map(lambda x: str(x).zfill(6))
        return df

    def __query_convertible_bonds(self):
        df = ts.new_cbonds()
        return df

    def __create_order_url(self, client=None, order_id=None, **params):
        return self.__create_url(client, 'orders', order_id, **params)

    def __create_url(self, client, resource, resource_id=None, **params):
        all_params = copy.deepcopy(params)
        all_params.update(client=(client or self._client))
        all_params.update(key=(self._key or ''))
        if resource_id is None:
            path = '/{}'.format(resource)
        else:
            path = '/{}/{}'.format(resource, resource_id)
        url = '{}/api/{}{}?{}'.format(self._base_url, self.VERSION, path, urlencode(all_params))
        return url

    def __create_base_url(self):
        if self._connection_method is ConnectionMethod.DIRECT:
            return 'http://{}:{}'.format(self._host, self._port)
        else:
            return self._proxy_base_url

    def __send_request(self, request, timeout=None):
        if self._connection_method is ConnectionMethod.PROXY:
            request.auth = HTTPBasicAuth(self._proxy_username, self._proxy_password)
            request.headers['X-Instance-ID'] = self._instance_id
        prepared_request = request.prepare()
        self.__log_request(prepared_request)
        with requests.sessions.Session() as session:
            response = session.send(prepared_request, timeout=(timeout or self._timeout))
        self.__log_response(response)
        response.raise_for_status()
        return response

    def __log_request(self, prepared_request):
        url = self.__eliminate_privacy(prepared_request.path_url)
        if prepared_request.body is None:
            self._logger.info('Request:\n{} {}'.format(prepared_request.method, url))
        else:
            self._logger.info('Request:\n{} {}\n{}'.format(prepared_request.method, url, prepared_request.body))

    def __log_response(self, response):
        message = u'Response:\n{} {}\n{}'.format(response.status_code, response.reason, response.text)
        if response.status_code == 200:
            self._logger.info(message)
        else:
            self._logger.error(message)

    @classmethod
    def __eliminate_privacy(cls, url):
        match = re.search(cls.KEY_REGEX, url)
        if match is None:
            return url
        key = match.group(1)
        masked_key = '*' * len(key)
        url = re.sub(cls.KEY_REGEX, "key={}".format(masked_key), url)
        return url



class Adjustment(object):
    @staticmethod
    def to_json(instance):
        json = {
            'sourcePortfolio': Portfolio.to_json(instance.source_portfolio),
            'targetPortfolio': Portfolio.to_json(instance.target_portfolio),
            'schema': AdjustmentSchema.to_json(instance.schema)
        }
        return json

    @classmethod
    def from_json(cls, json):
        instance = Adjustment()
        instance.id = json.get('id', None)
        instance.status = json.get('status', None)
        batches = []
        for batch_json in json['batches']:
            batch = []
            for order_json in batch_json:
                batch.append(Order.from_json(order_json))
            batches.append(batch)
        instance.batches = batches
        instance._progress = AdjustmentProgressGroup.from_json(json['progress'])
        return instance

    def empty(self):
        return not self.batches

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def target_portfolio(self):
        return self._target_portfolio

    @target_portfolio.setter
    def target_portfolio(self, value):
        self._target_portfolio = value

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    @property
    def batches(self):
        return self._batches

    @batches.setter
    def batches(self, value):
        self._batches = value

    @property
    def progress(self):
        return self._progress

    @progress.setter
    def progress(self, value):
        self._progress = value


class AdjustmentSchema(object):
    @staticmethod
    def to_json(instance):
        json = {
            'minOrderValue': instance.min_order_value,
            'maxOrderValue': instance.max_order_value,
            'reservedSecurities': instance.reserved_securities,
        }
        return json

    def __init__(self, reserved_securities, min_order_value, max_order_value):
        self._reserved_securities = reserved_securities
        self._min_order_value = min_order_value
        self._max_order_value = max_order_value

    @property
    def reserved_securities(self):
        return self._reserved_securities

    @reserved_securities.setter
    def reserved_securities(self, value):
        self._reserved_securities = value

    @property
    def min_order_value(self):
        return self._min_order_value

    @min_order_value.setter
    def min_order_value(self, value):
        self._min_order_value = value

    @property
    def max_order_value(self):
        return self._max_order_value

    @max_order_value.setter
    def max_order_value(self, value):
        self._max_order_value = value


class AdjustmentProgressGroup(object):
    @staticmethod
    def from_json(json):
        instance = AdjustmentProgressGroup()
        instance._today = AdjustmentProgress.from_json(json['today'])
        instance._overall = AdjustmentProgress.from_json(json['overall'])
        return instance

    def __str__(self):
        str = "今日进度：{0:>.0f}% -> {1:>.0f}%；总进度：{2:>.0f}% -> {3:>.0f}%".format(
            self.today.before * 100, self.today.after * 100,
            self.overall.before * 100, self.overall.after * 100
        )
        return str

    @property
    def today(self):
        return self._today

    @today.setter
    def today(self, value):
        self._today = value

    @property
    def overall(self):
        return self._overall

    @overall.setter
    def overall(self, value):
        self._overall = value


class AdjustmentProgress(object):
    @staticmethod
    def from_json(json):
        instance = AdjustmentProgress()
        instance.before = json['before']
        instance.after = json['after']
        return instance

    @property
    def before(self):
        return self._before

    @before.setter
    def before(self, value):
        self._before = value

    @property
    def after(self):
        return self._after

    @after.setter
    def after(self, value):
        self._after = value


class Portfolio(object):
    @staticmethod
    def to_json(instance):
        positions_json = {}
        for security, position in instance.positions.items():
            positions_json[security] = Position.to_json(position)
        json = {
            'availableCash': instance.available_cash,
            'totalValue': instance.total_value,
            'otherValue': instance.other_value,
            'totalValueDeviationRate': instance.total_value_deviation_rate,
            'positionsValue': instance.positions_value,
            'positions': positions_json
        }
        return json

    def __init__(self, available_cash=None, total_value=None, other_value=None, total_value_deviation_rate=None):
        self._available_cash = available_cash
        self._total_value = total_value
        self._other_value = other_value
        self._total_value_deviation_rate = total_value_deviation_rate
        self._positions_value = 0
        self._positions = dict()

    def __getitem__(self, security):
        return self._positions[security]

    def __setitem__(self, security, position):
        self._positions[security] = position

    @property
    def fingerprint(self):
        result = dict((security, position.total_amount) for security, position in self._positions.items())
        return result

    @property
    def available_cash(self):
        return self._available_cash

    @available_cash.setter
    def available_cash(self, value):
        self._available_cash = value

    @property
    def total_value(self):
        return self._total_value

    @total_value.setter
    def total_value(self, value):
        self._total_value = value

    @property
    def other_value(self):
        return self._other_value

    @other_value.setter
    def other_value(self, value):
        self._other_value = value

    @property
    def total_value_deviation_rate(self):
        return self._total_value_deviation_rate

    @total_value_deviation_rate.setter
    def total_value_deviation_rate(self, value):
        self._total_value_deviation_rate = value

    @property
    def positions_value(self):
        return self._positions_value

    @positions_value.setter
    def positions_value(self, value):
        self._positions_value = value

    @property
    def positions(self):
        return self._positions

    @positions.setter
    def positions(self, value):
        self._positions = value

    def add_position(self, position):
        self._positions_value += position.value
        self._positions[position.security] = position

    def rebalance(self):
        if self._available_cash is None:
            self._available_cash = self._total_value - self._positions_value
        elif self._total_value is None:
            self._total_value = self._available_cash + self.positions_value
        if self._available_cash < 0:
            self._available_cash = 0
            self._total_value = self._positions_value


class Position(object):
    @staticmethod
    def to_json(instance):
        json = {
            'security': instance.security,
            'price': instance.price,
            'totalAmount': instance.total_amount,
            'closeableAmount': instance.closeable_amount,
        }
        return json

    def __init__(self, security=None, price=None, total_amount=0, closeable_amount=0):
        self._security = self._normalize_security(security)
        self._price = price
        self._total_amount = total_amount
        self._closeable_amount = total_amount if closeable_amount is None else closeable_amount
        if price is not None and total_amount is not None:
            self._value = price * total_amount

    @property
    def security(self):
        return self._security

    @security.setter
    def security(self, value):
        self._security = self._normalize_security(value)

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, value):
        self._price = value

    @property
    def total_amount(self):
        return self._total_amount

    @total_amount.setter
    def total_amount(self, value):
        self._total_amount = value

    @property
    def closeable_amount(self):
        return self._closeable_amount

    @closeable_amount.setter
    def closeable_amount(self, value):
        self._closeable_amount = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value
        if self._price != 0:
            self._total_amount = self._value / self._price

    def _normalize_security(self, security):
        return security.split('.')[0] if security else None


class OrderAction(Enum):
    OPEN = 'OPEN'
    CLOSE = 'CLOSE'


class OrderStyle(Enum):
    LIMIT = 'LIMIT'
    MARKET = 'MARKET'


class OrderStatus(Enum):
    open = 0
    filled = 1
    canceled = 2
    rejected = 3
    held = 4


class Order(object):
    @staticmethod
    def from_json(json):
        order = Order()
        order.action = OrderAction.OPEN if json['action'] == 'BUY' else OrderAction.CLOSE
        order.security = json['symbol']
        order.style = OrderStyle(json['type'])
        order.price = json['price']
        order.amount = json['amount']
        order.amountProportion = json.get('amountProportion', '')
        return order

    @staticmethod
    def from_e_order(**kwargs):
        order = Order()
        order.action = OrderAction.OPEN if kwargs['action'] == 'BUY' else OrderAction.CLOSE
        order.security = kwargs['symbol']
        order.style = OrderStyle(kwargs['type'])
        order.price = kwargs['price']
        order.amount = kwargs.get('amount', 0)
        order.amountProportion = kwargs.get('amountProportion', '')
        return order

    def __init__(self, id=None, action=None, security=None, amount=None, amountProportion=None, price=None, style=None,
                 status=OrderStatus.open, add_time=None):
        self._id = id
        self._action = action
        self._security = security
        self._amount = amount
        self._amountProportion = amountProportion
        self._price = price
        self._style = style
        self._status = status
        self._add_time = add_time

    def __str__(self):
        str = "以 {0:>7.3f}元 {1}{2} {3:>5} {4}".format(
            self.price,
            '限价' if self.style == OrderStyle.LIMIT else '市价',
            '买入' if self.action == OrderAction.OPEN else '卖出',
            self.amount,
            self.security
        )
        return str

    def to_e_order(self):
        e_order = dict(
            action=('BUY' if self._action == OrderAction.OPEN else 'SELL'),
            symbol=self._security,
            type=self._style.name,
            priceType=(0 if self._style == OrderStyle.LIMIT else 4),
            price=self._price,
            amount=self._amount,
            amountProportion=self._amountProportion or ''
        )
        return e_order

    @property
    def value(self):
        return self._amount * self._price

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def action(self):
        return self._action

    @action.setter
    def action(self, value):
        self._action = value

    @property
    def security(self):
        return self._security

    @security.setter
    def security(self, value):
        self._security = value

    @property
    def amount(self):
        return self._amount

    @amount.setter
    def amount(self, value):
        self._amount = value

    @property
    def amountProportion(self):
        return self._amountProportion

    @amountProportion.setter
    def amountProportion(self, value):
        self._amountProportion = value

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, value):
        self._price = value

    @property
    def style(self):
        return self._style

    @style.setter
    def style(self, value):
        self._style = value

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def add_time(self):
        return self._add_time

    @add_time.setter
    def add_time(self, value):
        self._add_time = value



class Struct(object):
    def __init__(self, data):
        for key, value in data.items():
            key = key.replace('-', '_')
            if isinstance(value, tuple):
                setattr(self, key, (Struct(x) if isinstance(x, dict) else x for x in value))
            if isinstance(value, list):
                setattr(self, key, [Struct(x) if isinstance(x, dict) else x for x in value])
            else:
                setattr(self, key, Struct(value) if isinstance(value, dict) else value)


class OrderedDictYAMLLoader(yaml.Loader):
    def __init__(self, *args, **kwargs):
        yaml.Loader.__init__(self, *args, **kwargs)
        self.add_constructor(u'tag:yaml.org,2002:omap', type(self).construct_odict)

    def construct_odict(self, node):
        omap = OrderedDict()
        yield omap
        if not isinstance(node, yaml.SequenceNode):
            raise yaml.constructor.ConstructorError(
                "while constructing an ordered map",
                node.start_mark,
                "expected a sequence, but found %s" % node.id, node.start_mark
            )
        for subnode in node.value:
            if not isinstance(subnode, yaml.MappingNode):
                raise yaml.constructor.ConstructorError(
                    "while constructing an ordered map", node.start_mark,
                    "expected a mapping of length 1, but found %s" % subnode.id,
                    subnode.start_mark
                )
            if len(subnode.value) != 1:
                raise yaml.constructor.ConstructorError(
                    "while constructing an ordered map", node.start_mark,
                    "expected a single mapping item, but found %d items" % len(subnode.value),
                    subnode.start_mark
                )
            key_node, value_node = subnode.value[0]
            key = self.construct_object(key_node)
            value = self.construct_object(value_node)
            omap[key] = value


class StopWatch(object):
    def __init__(self):
        pass

    def start(self):
        self._start_time = datetime.datetime.now()

    def stop(self):
        self._end_time = datetime.datetime.now()
        return self

    def short_summary(self):
        return str(self._end_time - self._start_time)


class BaseStrategyManagerFactory(object):
    def __init__(self):
        self._config = self._create_config()

    def create(self, id):
        traders = self._create_traders(id)
        return StrategyManager(id, self._create_logger(), self._config, traders, self._get_context())

    def _get_context(self):
        pass

    def _create_traders(self, id):
        traders = OrderedDict()
        for trader_id, trader_config in self._config.build_trader_configs(id).items():
            trader = self._create_trader(trader_config)
            traders[trader_id] = trader
        return traders

    def _create_trader(self, trader_config):
        return StrategyTrader(self._create_logger(), trader_config, self._get_context())

    def _create_logger(self):
        pass

    def _create_config(self):
        return StrategyConfig(self._get_context())


class BaseStrategyContext(object):
    def get_portfolio(self):
        pass

    def convert_order(self, quant_order):
        pass

    def has_open_orders(self):
        pass

    def cancel_open_orders(self):
        pass

    def cancel_order(self, quant_order):
        pass

    def read_file(self, path):
        pass

    def is_sim_trade(self):
        pass

    def is_backtest(self):
        pass

    def is_read_file_allowed(self):
        return False


class BaseLogger(object):
    def debug(self, msg, *args, **kwargs):
        pass

    def info(self, msg, *args, **kwargs):
        pass

    def warning(self, msg, *args, **kwargs):
        pass

    def error(self, msg, *args, **kwargs):
        pass

    def exception(self, msg, *args, **kwargs):
        pass


class StrategyManager(object):
    THEMATIC_BREAK = '-' * 50

    def __init__(self, id, logger, config, traders, strategy_context):
        self._id = id
        self._logger = logger
        self._config = config
        self._traders = traders
        self._strategy_context = strategy_context

    @property
    def id(self):
        return self._id

    @property
    def traders(self):
        return self._traders

    def purchase_new_stocks(self):
        for trader in self._traders.values():
            try:
                trader.purchase_new_stocks()
            except:
                self._logger.exception('[%s] 打新失败', trader.id)

    def repo(self):
        try:
            security = '131810'
            quote_df = ts.get_realtime_quotes(security)
            order = {
                'action': 'SELL',
                'symbol': security,
                'type': 'LIMIT',
                'price': float(quote_df['bid'][0]),
                'amountProportion': 'ALL'
            }
            for trader in self._traders.values():
                try:
                    trader.execute(**order)
                except:
                    self._logger.exception('[%s] 逆回购失败', trader.id)
        except:
            self._logger.exception('逆回购失败')

    def purchase_convertible_bonds(self):
        for trader in self._traders.values():
            try:
                trader.purchase_convertible_bonds()
            except:
                self._logger.exception('[%s] 申购转债失败', trader.id)

    def execute(self, order=None, **kwargs):
        if order is None and not kwargs:
            return
        for trader in self._traders.values():
            try:
                trader.execute(order, **kwargs)
            except:
                self._logger.exception('[%s] 下单失败', trader.id)

    def cancel(self, order):
        for trader in self._traders.values():
            try:
                trader.cancel(order)
            except:
                self._logger.exception('[%s] 撤单失败', trader.id)

    def work(self):
        stop_watch = StopWatch()
        stop_watch.start()
        self._logger.info("[%s] 开始工作", self._id)
        self._refresh()
        for id, trader in self._traders.items():
            trader.work()
        stop_watch.stop()
        self._logger.info("[%s] 结束工作，总耗时[%s]", self._id, stop_watch.short_summary())
        self._logger.info(self.THEMATIC_BREAK)

    def _refresh(self):
        if not self._strategy_context.is_read_file_allowed():
            return
        self._config.reload()
        trader_configs = self._config.build_trader_configs(self._id)
        for id, trader in self._traders.items():
            trader.set_config(trader_configs[id])


class StrategyTrader(object):
    def __init__(self, logger, config, strategy_context):
        self._logger = logger
        self._config = config
        self._strategy_context = strategy_context
        self._shipane_client = Client(self._logger, **config['client'])
        self._order_id_to_info_map = {}
        self._expire_before = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        self._last_sync_portfolio_fingerprint = None

    @property
    def id(self):
        return self._config['id']

    @property
    def client(self):
        return self._shipane_client

    def set_config(self, config):
        self._config = config

    def purchase_new_stocks(self):
        if not self._pre_check():
            return

        self._shipane_client.purchase_new_stocks()

    def purchase_convertible_bonds(self):
        if not self._pre_check():
            return

        self._shipane_client.purchase_convertible_bonds()

    def execute(self, order=None, **kwargs):
        if not self._pre_check():
            return

        if order is None:
            common_order = Order.from_e_order(**kwargs)
        else:
            common_order = self._normalize_order(order)

        try:
            actual_order = self._execute(common_order)
            return actual_order
        except Exception:
            self._logger.exception("[实盘易] 下单异常")

    def cancel(self, order):
        if not self._pre_check():
            return

        try:
            self._cancel(order)
        except:
            self._logger.exception("[实盘易] 撤单异常")

    def work(self):
        if not self._pre_check():
            return

        if self._config['mode'] == 'SYNC':
            self._sync()
        else:
            self._follow()

    def _sync(self):
        stop_watch = StopWatch()
        stop_watch.start()
        self._logger.info("[%s] 开始同步", self.id)
        try:
            if self._sync_config['pre-clear-for-sim']:
                self._cancel_all_for_sim()
                self._logger.info("[%s] 模拟盘撤销全部订单已完成", self.id)
            target_portfolio = self._strategy_context.get_portfolio()
            if self._should_sync(target_portfolio):
                if self._sync_config['pre-clear-for-live'] and not self._config['dry-run']:
                    self._shipane_client.cancel_all()
                    time.sleep(self._sync_config['order-interval'] / 1000.0)
                    self._logger.info("[%s] 实盘撤销全部订单已完成", self.id)

                is_sync = False
                for i in range(0, 2 + self._sync_config['extra-rounds']):
                    self._logger.info("[%s] 开始第[%d]轮同步", self.id, i + 1)
                    is_sync = self._sync_once(target_portfolio)
                    self._logger.info("[%s] 结束第[%d]轮同步", self.id, i + 1)
                    if is_sync:
                        self._last_sync_portfolio_fingerprint = target_portfolio.fingerprint
                        self._logger.info("[%s] 实盘已与模拟盘同步", self.id)
                        break
                    time.sleep(self._sync_config['round-interval'] / 1000.0)
                self._logger.info(u"[%s] 结束同步，状态：%s", self.id, "已完成" if is_sync else "未完成")
        except:
            self._logger.exception("[%s] 同步失败", self.id)
        stop_watch.stop()
        self._logger.info("[%s] 结束同步，耗时[%s]", self.id, stop_watch.short_summary())

    def _follow(self):
        stop_watch = StopWatch()
        stop_watch.start()
        self._logger.info("[%s] 开始跟单", self.id)
        try:
            common_orders = []
            all_common_orders = self._strategy_context.get_orders()
            for common_order in all_common_orders:
                if common_order.add_time >= self._strategy_context.get_current_time():
                    if common_order.status == OrderStatus.canceled:
                        origin_order = copy.deepcopy(common_order)
                        origin_order.status = OrderStatus.open
                        common_orders.append(origin_order)
                    else:
                        common_orders.append(common_order)
                if common_order.status == OrderStatus.canceled:
                    common_orders.append(common_order)

            common_orders = sorted(common_orders, key=lambda o: _PrioritizedOrder(o))
            for common_order in common_orders:
                if common_order.status != OrderStatus.canceled:
                    try:
                        self._execute(common_order)
                    except:
                        self._logger.exception("[实盘易] 下单异常")
                else:
                    try:
                        self._cancel(common_order)
                    except:
                        self._logger.exception("[实盘易] 撤单异常")
        except:
            self._logger.exception("[%s] 跟单失败", self.id)
        stop_watch.stop()
        self._logger.info("[%s] 结束跟单，耗时[%s]", self.id, stop_watch.short_summary())

    @property
    def _sync_config(self):
        return self._config['sync']

    def _execute(self, order):
        if not self._should_run():
            self._logger.info("[%s] %s", self.id, order)
            return None
        actual_order = self._do_execute(order)
        return actual_order

    def _cancel(self, order):
        if not self._should_run():
            self._logger.info("[%s] 撤单 [%s]", self.id, order)
            return
        self._do_cancel(order)

    def _do_execute(self, order):
        common_order = self._normalize_order(order)
        e_order = common_order.to_e_order()
        actual_order = self._shipane_client.execute(**e_order)
        self._order_id_to_info_map[common_order.id] = {'id': actual_order['id'], 'canceled': False}
        return actual_order

    def _do_cancel(self, order):
        if order is None:
            self._logger.info('[实盘易] 委托为空，忽略撤单请求')
            return

        if isinstance(order, int):
            quant_order_id = order
        else:
            common_order = self._normalize_order(order)
            quant_order_id = common_order.id

        try:
            order_info = self._order_id_to_info_map[quant_order_id]
            if not order_info['canceled']:
                order_info['canceled'] = True
                self._shipane_client.cancel(order_id=order_info['id'])
        except KeyError:
            self._logger.warning('[实盘易] 未找到对应的委托编号')
            self._order_id_to_info_map[quant_order_id] = {'id': None, 'canceled': True}

    def _normalize_order(self, order):
        if isinstance(order, Order):
            common_order = order
        else:
            common_order = self._strategy_context.convert_order(order)
        return common_order

    def _should_run(self):
        if self._config['dry-run']:
            self._logger.debug("[实盘易] 当前为排练模式，不执行下单、撤单请求")
            return False
        return True

    def _is_expired(self, common_order):
        return common_order.add_time < self._expire_before

    def _pre_check(self):
        if not self._config['enabled']:
            self._logger.info("[%s] 交易未启用，不执行", self.id)
            return False
        if self._strategy_context.is_backtest():
            self._logger.info("[%s] 当前为回测环境，不执行", self.id)
            return False
        return True

    def _should_sync(self, target_portfolio):
        if self._strategy_context.has_open_orders():
            self._logger.info("[%s] 有未完成订单，不进行同步", self.id)
            return False
        is_changed = target_portfolio.fingerprint != self._last_sync_portfolio_fingerprint
        if not is_changed:
            self._logger.info("[%s] 模拟持仓未改变，不进行同步", self.id)
        return is_changed

    def _cancel_all_for_sim(self):
        self._strategy_context.cancel_open_orders()

    def _sync_once(self, target_portfolio):
        adjustment = self._create_adjustment(target_portfolio)
        self._log_progress(adjustment)
        self._execute_adjustment(adjustment)
        is_sync = adjustment.empty()
        return is_sync

    def _create_adjustment(self, target_portfolio):
        request = self._create_adjustment_request(target_portfolio)
        request_json = Adjustment.to_json(request)
        response_json = self._shipane_client.create_adjustment(request_json=request_json)
        adjustment = Adjustment.from_json(response_json)
        return adjustment

    def _create_adjustment_request(self, target_portfolio):
        request = Adjustment()
        request.source_portfolio = Portfolio(
            other_value=self._sync_config['other-value'],
            total_value_deviation_rate=self._sync_config['total-value-deviation-rate'],
        )
        request.target_portfolio = target_portfolio
        request.schema = AdjustmentSchema(self._sync_config['reserved-securities'],
                                          self._sync_config['min-order-value'],
                                          self._sync_config['max-order-value'])
        return request

    def _log_progress(self, adjustment):
        self._logger.info("[%s] %s", self.id, adjustment.progress)

    def _execute_adjustment(self, adjustment):
        for batch in adjustment.batches:
            for order in batch:
                self._execute(order)
                time.sleep(self._sync_config['order-interval'] / 1000.0)
            time.sleep(self._sync_config['batch-interval'] / 1000.0)


class StrategyConfig(object):
    def __init__(self, strategy_context):
        self._strategy_context = strategy_context
        self._data = dict()
        self.reload()

    @property
    def data(self):
        return self._data

    def reload(self):
        content = self._strategy_context.read_file('shipane_sdk_config.yaml')
        stream = six.BytesIO(content)
        self._data = yaml.load(stream, Loader=OrderedDictYAMLLoader)
        self._proxies = self._create_proxy_configs()
        stream.close()

    def build_trader_configs(self, id):
        trader_configs = OrderedDict()
        for raw_manager_config in self._data['managers']:
            if raw_manager_config['id'] == id:
                for raw_trader_config in raw_manager_config['traders']:
                    trader_config = self._create_trader_config(raw_trader_config)
                    trader_configs[raw_trader_config['id']] = trader_config
                break
        return trader_configs

    def _create_proxy_configs(self):
        proxies = {}
        for raw_proxy_config in self._data['proxies']:
            id = raw_proxy_config['id']
            proxies[id] = raw_proxy_config
        return proxies

    def _create_trader_config(self, raw_trader_config):
        client_config = self._create_client_config(raw_trader_config)
        trader_config = copy.deepcopy(raw_trader_config)
        trader_config['client'] = client_config
        if 'sync' in trader_config:
            trader_config['sync']['reserved-securities'] = client_config.pop('reserved_securities', [])
            trader_config['sync']['other-value'] = client_config.pop('other_value', [])
            trader_config['sync']['total-value-deviation-rate'] = client_config.pop('total_value_deviation_rate', [])
        return trader_config

    def _create_client_config(self, raw_trader_config):
        client_config = None
        for raw_gateway_config in self._data['gateways']:
            for raw_client_config in raw_gateway_config['clients']:
                if raw_client_config['id'] == raw_trader_config['client']:
                    connection_method = raw_gateway_config['connection-method']
                    client_config = {
                        'connection_method': connection_method,
                        'key': raw_gateway_config['key'],
                        'timeout': tuple([
                            raw_gateway_config['timeout']['connect'],
                            raw_gateway_config['timeout']['read'],
                        ]),
                        'client': raw_client_config['query'],
                        'reserved_securities': raw_client_config['reserved-securities'],
                        'other_value': raw_client_config['other-value'],
                        'total_value_deviation_rate': raw_client_config['total-value-deviation-rate'],
                    }
                    if connection_method == 'DIRECT':
                        client_config.update({
                            'host': raw_gateway_config['host'],
                            'port': raw_gateway_config['port'],
                        })
                    else:
                        proxy_config = self._proxies[raw_gateway_config['proxy']]
                        client_config.update({
                            'proxy_base_url': proxy_config['base-url'],
                            'proxy_username': proxy_config['username'],
                            'proxy_password': proxy_config['password'],
                            'instance_id': raw_gateway_config['instance-id'],
                        })
                    break
                if client_config is not None:
                    break
        return client_config


class _PrioritizedOrder(object):
    def __init__(self, order):
        self.order = order

    def __lt__(self, other):
        x = self.order
        y = other.order
        if x.add_time != y.add_time:
            return x.add_time < y.add_time
        if x.status == OrderStatus.canceled:
            if y.status == OrderStatus.canceled:
                return x.id < y.id
            else:
                return False
        else:
            if y.status == OrderStatus.canceled:
                return True
            else:
                return x.id < y.id

    def __gt__(self, other):
        return other.__lt__(self)

    def __eq__(self, other):
        return (not self.__lt__(other)) and (not other.__lt__(self))

    def __le__(self, other):
        return not self.__gt__(other)

    def __ge__(self, other):
        return not self.__lt__(other)

    def __ne__(self, other):
        return not self.__eq__(other)


class JoinQuantStrategyManagerFactory(BaseStrategyManagerFactory):
    def __init__(self, context):
        self._strategy_context = JoinQuantStrategyContext(context)
        super(JoinQuantStrategyManagerFactory, self).__init__()

    def _get_context(self):
        return self._strategy_context

    def _create_logger(self):
        return JoinQuantLogger()


class JoinQuantStrategyContext(BaseStrategyContext):
    def __init__(self, context):
        self._context = context

    def get_current_time(self):
        return self._context.current_dt

    def get_portfolio(self):
        quant_portfolio = self._context.portfolio
        portfolio = Portfolio()
        portfolio.available_cash = quant_portfolio.available_cash
        portfolio.total_value = quant_portfolio.total_value
        positions = dict()
        for security, quant_position in quant_portfolio.positions.items():
            position = self._convert_position(quant_position)
            positions[position.security] = position
        portfolio.positions = positions
        return portfolio

    def convert_order(self, quant_order):
        common_order = Order(
            id=quant_order.order_id,
            action=(OrderAction.OPEN if quant_order.is_buy else OrderAction.CLOSE),
            security=quant_order.security,
            price=quant_order.limit,
            amount=quant_order.amount,
            style=(OrderStyle.LIMIT if quant_order.limit > 0 else OrderStyle.MARKET),
            status=self._convert_status(quant_order.status),
            add_time=quant_order.add_time,
        )
        return common_order

    def get_orders(self):
        orders = get_orders()
        common_orders = []
        for order in orders.values():
            common_order = self.convert_order(order)
            common_orders.append(common_order)
        return common_orders

    def has_open_orders(self):
        return bool(get_open_orders())

    def cancel_open_orders(self):
        open_orders = get_open_orders()
        for open_order in open_orders.values():
            self.cancel_order(open_order)

    def cancel_order(self, open_order):
        return cancel_order(open_order)

    def read_file(self, path):
        return read_file(path)

    def is_sim_trade(self):
        return self._context.run_params.type == 'sim_trade'

    def is_backtest(self):
        return not self.is_sim_trade()

    def is_read_file_allowed(self):
        return True

    @staticmethod
    def _convert_position(quant_position):
        position = Position()
        position.security = quant_position.security
        position.price = quant_position.price
        position.total_amount = quant_position.total_amount + quant_position.locked_amount
        position.closeable_amount = quant_position.closeable_amount
        position.value = quant_position.value
        return position

    @staticmethod
    def _convert_status(quant_order_status):
        try:
            return OrderStatus(quant_order_status.value)
        except ValueError:
            return OrderStatus.open


class JoinQuantLogger(BaseLogger):
    def debug(self, msg, *args, **kwargs):
        log.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        log.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        log.warn(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        log.error(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        msg += "\n%s"
        args += (traceback.format_exc(),)
        log.error(msg, *args, **kwargs)
