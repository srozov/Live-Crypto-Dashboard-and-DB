import os
import ccxt
import time
import datetime
import dash
from dash.dependencies import Input, Output, Event
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import threading
import dataset
import sqlite3
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import math

from collections import deque


def ensure_dir(directory):
    """"""
    if not os.path.exists(directory):
        os.makedirs(directory)



class CryptoDataGrabber(object):

    def __init__(self):
        """"""
        self.exchange = ccxt.binance()
        self.exchange.load_markets()
        self.exchange.enableRateLimit = True
        self.delay_seconds = self.exchange.rateLimit / 1000
        self.symbols = self.exchange.symbols


        # self.symbols = [s for s in self.symbols if "BTC" in s]


        # self.symbols = ['BCH/BTC', 'EOS/BTC', 'ADA/BTC', 'XRP/BTC', 'LTC/BTC', 'TRX/BTC', 'ICX/BTC', 'GVT/BTC', 'XMR/BTC', 'XLM/BTC']


        self.symbols =  ['BTC/USDT', 'ETH/BTC', 'XRP/BTC', 'XEM/BTC', 'STRAT/BTC', 'XMR/BTC', 'LTC/BTC', 'BCH/BTC', 'ETC/BTC']


        self.symbols_sel = []

        self.since_datetime = datetime.datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

        self.timeframe = '2h'

        if self.timeframe == '1m':
            self.timeframe_s = 60
        elif self.timeframe == '5m':
            self.timeframe_s = 5 * 60
        elif self.timeframe == '15m':
            self.timeframe_s = 15 * 60
        elif self.timeframe == '30m':
            self.timeframe_s = 30 * 60
        elif self.timeframe == '1h':
            self.timeframe_s = 60 *60
        elif self.timeframe == '2h':
            self.timeframe_s = 2 * 60 * 60
        elif self.timeframe == '4h':
            self.timeframe_s = 4 * 60 * 60
        elif self.timeframe == '6h':
            self.timeframe_s = 6 * 60 * 60
        elif self.timeframe == '12h':
            self.timeframe_s = 12 * 60 * 60
        elif self.timeframe == '1d':
            self.timeframe_s = 24 * 60 * 60
        elif self.timeframe == '1w':
            self.timeframe_s = 7 * 24 * 60 * 60


        self.graph_w = 1000



        self.db_file = 'databases/market_prices_' + self.timeframe + '.db'

        self.db_file_sel = 'databases/market_prices_sel_' + self.timeframe + '.db'


        # name of the sqlite database file
        self.deques = dict()
        self.ohlcv = dict()
        self.last_ohlcv = []
        # self.database = dataset.connect(self.db_url)
        ensure_dir('databases')
        # db = sqlite3.connect(self.db_file)
        # db_sel = sqlite3.connect(self.db_file_sel)

        self.fill_tables(self.symbols, self.db_file)


        if self.start_datetime != self.since_datetime :


            self.fetch_ohlcv_seq(self.symbols, self.db_file)

        # for vol-selected symbols (first run)

        # self.symbols_sel = self.get_symbols_by_vol()
        #
        # self.fill_tables(self.symbols_sel, self.db_file_sel)
        #
        # self.fetch_ohlcv_seq(self.symbols_sel, self.db_file_sel)


        # db = sqlite3.connect(self.db_file_sel)
        # c = db.cursor()
        # c.execute("SELECT * FROM" + "'{}'".format('BTC/USDT') + "ORDER BY time ASC limit 1")
        # first_row = c.fetchall()
        # self.start_datetime_sel = datetime.datetime.strptime(first_row[0][0], '%Y-%m-%d %H:%M:%S')
        # if self.start_datetime_sel != self.since_datetime :

        # print('pass fill')


        #     self.deques[symbol] = deque()
        #
        #     for e in self.database[symbol]:
        #         entry = (e['bid'], e['ask'], e['spread'], e['time'])
        #         self.deques[symbol].append(entry)
        # del self.database
        self.thread = threading.Thread(target=self.__update)
        self.thread.daemon = True
        self.thread.start()

    def fill_tables(self, symbols, db_file):
        # create table if not exist for each pair

        db = sqlite3.connect(db_file)
        c = db.cursor()

        for symbol in symbols:
            c.execute("CREATE TABLE IF NOT EXISTS " + "'{}'".format(
                symbol) + "(time REAL, open REAL, high REAL, low REAL, close REAL, b_volume REAL, q_volume REAL)")
            db.commit()

        # check if table not empty, get start and end dates

        c.execute("SELECT * FROM" + "'{}'".format(symbols[0]) + "ORDER BY time DESC limit 1")

        # TODO change to last_row = c.execute("SELECT MAX(time) FROM" + "'{}'".format(symbol)).fetchall()[0][0]


        last_row = c.fetchall()

        c.execute("SELECT * FROM" + "'{}'".format(symbols[0]) + "ORDER BY time ASC limit 1")

        # TODO change to first_row = c.execute("SELECT MIN(time) FROM" + "'{}'".format(symbol)).fetchall()[0][0]


        first_row = c.fetchall()

        # calculate number of candles to fetch to present time

        if len(first_row) > 0:

            self.start_datetime = datetime.datetime.strptime(first_row[0][0], '%Y-%m-%d %H:%M:%S')

            n_fetch = math.floor((datetime.datetime.now() - datetime.datetime.strptime(last_row[0][0],
                                                                                       '%Y-%m-%d %H:%M:%S')).total_seconds() / self.timeframe_s)

            print('fetching', n_fetch, ' candles (from end to now)')

        # if empty fetch 500 chandles

        else:

            self.start_datetime = datetime.datetime.now()
            self.start_datetime = self.start_datetime - datetime.timedelta(
                minutes=self.start_datetime.minute % self.timeframe_s,
                seconds=self.start_datetime.second,
                microseconds=self.start_datetime.microsecond)

            print('start_dt %s' % str(self.start_datetime))
            print('since_dt %s' % str(self.since_datetime))


            #  TODO this is wrong -> timedelta.total_seconds()

            n_fetch = math.floor((self.start_datetime - self.since_datetime).total_seconds() / self.timeframe_s)

            # print((self.start_datetime - self.since_datetime).total_seconds())

            if n_fetch > 500:
                n_fetch = 500

            print('fetching ', n_fetch, ' candles (from now to end)')

        if n_fetch > 0:
            for symbol in symbols:
                if self.exchange.has['fetchOHLCV']:
                    # print('Obtaining OHLCV data')
                    data = self.exchange.fetch_ohlcv(symbol, timeframe=self.timeframe, limit=n_fetch)
                    data = list(zip(*data))
                    data[0] = [datetime.datetime.fromtimestamp(ms / 1000)
                               for ms in data[0]]

                    # add quote volume
                    data.append(tuple([a * b for a, b in zip(data[4], data[5])]))

                    self.ohlcv[symbol] = data

                    c.execute("CREATE TABLE IF NOT EXISTS " + "'{}'".format(
                        symbol) + "(time REAL, open REAL, high REAL, low REAL, close REAL, b_volume REAL, q_volume REAL)")
                    c.executemany("INSERT INTO " + "'{}'".format(symbol) + "VALUES(?,?,?,?,?,?,?)",
                                  np.array(self.ohlcv[symbol]).transpose())
                    db.commit()

        db.close()

    def get_symbols_by_vol(self):

        total_volume = {}

        db = sqlite3.connect(self.db_file)
        c = db.cursor()

        for symbol in self.symbols:
            total_volume[symbol] = c.execute('SELECT SUM(q_volume) FROM (SELECT q_volume FROM ' + "'{}'".format(
                symbol) + 'ORDER BY time DESC limit 24 * 7 )').fetchall()[0][0]

        symbols_by_vol = sorted(total_volume, key=total_volume.get)[-9:]

        # symbols_by_vol.remove('BCN/BTC')
        # symbols_by_vol.append('VEN/BTC')

        return symbols_by_vol

    def get_symbols(self):
        """"""
        return self.symbols

    def get_prices(self, symbol):
        """"""
        return self.deques[symbol]

    def get_ohlcv(self, symbol):
        """"""
        data = self.ohlcv[symbol]
        return data[0], data[1], data[2], data[3], data[4], data[5]

    def get_window_ohlcv(self, symbol):
        """"""
        con = create_engine('sqlite:///' + self.db_file)
        df = pd.read_sql(
            "SELECT * FROM (SELECT * FROM" + "'{}'".format(symbol) + "ORDER BY time DESC limit " + "'{}'".format(self.graph_w) + ") ORDER BY time ASC",
            con)
        return df

    def fetch_ohlcv_seq(self, symbols, db_file):

        hold = 30
        db = sqlite3.connect(db_file)

        c = db.cursor()
        ohlcv = dict()

        for symbol in symbols:

            c.execute("SELECT * FROM" + "'{}'".format(
                symbol) + "ORDER BY time ASC limit 1")
            first_row = c.fetchall()

            start_datetime = first_row[0][0]
            start_timestamp = int(time.mktime(datetime.datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S').timetuple())) * 1000

            since_timestamp = int(time.mktime(self.since_datetime.timetuple())) * 1000

            data = []

            if self.exchange.has['fetchOHLCV']:

                while since_timestamp + 500 * self.timeframe_s * 1000 < start_timestamp:
                    try:
                        ohlcvs = self.exchange.fetch_ohlcv(symbol, self.timeframe, since_timestamp)
                        since_timestamp += len(ohlcvs) * self.timeframe_s * 1000
                        data += ohlcvs



                    except (
                    ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:

                        print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
                        time.sleep(hold)

                n_fetch = math.floor((start_timestamp - since_timestamp) / (self.timeframe_s * 1000)) - 1

                # TODO: correct:  won't write to db if fetched exactly 500 candles the first time?

                if n_fetch > 0:

                    print('fetching last', n_fetch, 'candles')
                    print(symbol)

                    ohlcvs = self.exchange.fetch_ohlcv(symbol, self.timeframe, since_timestamp, limit=n_fetch)

                    data += ohlcvs

                    data = list(zip(*data))

                    data[0] = [datetime.datetime.fromtimestamp(ms / 1000)
                           for ms in data[0]]

                    # add quote volume
                    data.append(tuple([a * b for a, b in zip(data[4], data[5])]))


                    ohlcv[symbol] = data

                    c.execute("CREATE TABLE IF NOT EXISTS " + "'{}'".format(
                        symbol) + "(time REAL, open REAL, high REAL, low REAL, close REAL, b_volume REAL, q_volume REAL)")
                    c.executemany("INSERT INTO " + "'{}'".format(symbol) + "VALUES(?,?,?,?,?,?,?)",
                              np.array(ohlcv[symbol]).transpose())
                    db.commit()

        db.close()



    def __update(self):
        """
        https://github.com/ccxt-dev/ccxt/wiki/Manual#market-price
        """

        db = sqlite3.connect(self.db_file)
        c = db.cursor()
        c.execute("SELECT * FROM" + "'{}'".format(
            'BTC/USDT') + "ORDER BY time DESC limit 1")
        last_row = c.fetchall()

        db_current_time = True

        while db_current_time :

            if datetime.datetime.fromtimestamp(self.exchange.fetch_ohlcv('BTC/USDT', self.timeframe, limit=1)[0][0] / 1000) == datetime.datetime.strptime(last_row[0][0], '%Y-%m-%d %H:%M:%S') :
                time.sleep(15)
            else :

                db_current_time = False


        while True:

            start_time = datetime.datetime.now()
            for symbol in self.symbols:

                self.last_ohlcv = self.exchange.fetch_ohlcv(symbol, self.timeframe, limit=1)
                self.last_ohlcv[0][0] = datetime.datetime.fromtimestamp(self.last_ohlcv[0][0] / 1000)
                self.last_ohlcv.append(tuple([a * b for a, b in zip(self.last_ohlcv[4], self.last_ohlcv[5])]))

                c.execute("CREATE TABLE IF NOT EXISTS " + "'{}'".format(
                    symbol) + "(time REAL, open REAL, high REAL, low REAL, close REAL, b_volume REAL, q_volume REAL)")
                c.executemany("INSERT INTO " + "'{}'".format(symbol) + "VALUES(?,?,?,?,?,?,?)",
                              self.last_ohlcv)
                db.commit()

            wait_time = self.timeframe_s - (datetime.datetime.now() - start_time).total_seconds()
            time.sleep(wait_time)


data = CryptoDataGrabber()

selected_dropdown_value = 'BTC/USDT'

app = dash.Dash('market_data')
app.layout = html.Div([
    html.Div([
        html.H1('Binance Nerd', id='h1_title'),
        dcc.Dropdown(
            id='symbol-dropdown',
            options=[{'label': key, 'value': key}
                     for key in data.get_symbols()],
            value=selected_dropdown_value
        ),
        html.Div([
            dcc.Graph(
                id='ohlc',
                config={
                    'displayModeBar': False
                }
            ),
        ], className="row"),

        # change update interval later

        dcc.Interval(id='graph-update', interval=60 * 1000),
    ], className="row")
])
app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})


@app.callback(Output('h1_title', 'children'),
              [Input('symbol-dropdown', 'value')])
def change_plot(value):
    global selected_dropdown_value
    selected_dropdown_value = value
    return 'Market Prices ' + str(value)


@app.callback(Output('ohlc', 'figure'),
              [Input('symbol-dropdown', 'value')],
                events = [Event('graph-update', 'interval')])
def plot_olhc(value):
    global data
    df = data.get_window_ohlcv(value)
    return {
        'data': [go.Ohlc(x=df.time,
                         open=df.open,
                         high=df.high,
                         low=df.low,
                         close=df.close)],
        'layout': dict(title="OHLC")
    }


if __name__ == '__main__':
    app.run_server()
    print(self.server)


