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
        self.symbols_USDT = [s for s in self.symbols if "USDT" in s]

        self.since_datetime = datetime.datetime.strptime('2018-05-04 00:39:00', '%Y-%m-%d %H:%M:%S')

        self.timeframe = '1m'
        self.db_file = 'databases/market_prices.db'  # name of the sqlite database file
        self.deques = dict()
        self.ohlcv = dict()
        self.last_ohlcv = []
        # self.database = dataset.connect(self.db_url)
        ensure_dir('databases')
        db = sqlite3.connect(self.db_file)
        c = db.cursor()


        for symbol in self.symbols_USDT:

            c.execute("CREATE TABLE IF NOT EXISTS " + "'{}'".format(
                symbol) + "(time REAL, open REAL, high REAL, low REAL, close REAL, volume REAL)")
            db.commit()


        c.execute("SELECT * FROM" + "'{}'".format(self.symbols_USDT[0]) + "ORDER BY time DESC limit 1")
        last_row = c.fetchall()

        c.execute("SELECT * FROM" + "'{}'".format(self.symbols_USDT[0]) + "ORDER BY time ASC limit 1")
        first_row = c.fetchall()

        if len(first_row) > 0 :

            self.start_datetime = datetime.datetime.strptime(first_row[0][0], '%Y-%m-%d %H:%M:%S')

            n_fetch = math.floor((datetime.datetime.now() - datetime.datetime.strptime(last_row[0][0], '%Y-%m-%d %H:%M:%S')).seconds / 60)

        else :

            self.start_datetime = datetime.datetime.now()
            self.start_datetime = self.start_datetime - datetime.timedelta(minutes=self.start_datetime.minute % 1,
                                         seconds=self.start_datetime.second,
                                         microseconds=self.start_datetime.microsecond)

            n_fetch = 500


        for symbol in self.symbols_USDT:
            if self.exchange.has['fetchOHLCV']:
                # print('Obtaining OHLCV data')
                data = self.exchange.fetch_ohlcv(symbol, timeframe= self.timeframe , limit= n_fetch)
                data = list(zip(*data))
                data[0] = [datetime.datetime.fromtimestamp(ms / 1000)
                           for ms in data[0]]
                self.ohlcv[symbol] = data

                c.execute("CREATE TABLE IF NOT EXISTS " + "'{}'".format(
                    symbol) + "(time REAL, open REAL, high REAL, low REAL, close REAL, volume REAL)")
                c.executemany("INSERT INTO " + "'{}'".format(symbol) + "VALUES(?,?,?,?,?,?)",
                              np.array(self.ohlcv[symbol]).transpose())
                db.commit()

        db.close()

        if self.start_datetime != self.since_datetime :

            self.fetch_ohlcv_seq()

        #     self.deques[symbol] = deque()
        #
        #     for e in self.database[symbol]:
        #         entry = (e['bid'], e['ask'], e['spread'], e['time'])
        #         self.deques[symbol].append(entry)
        # del self.database
        self.thread = threading.Thread(target=self.__update)
        self.thread.daemon = True
        self.thread.start()

    def get_symbols(self):
        """"""
        return self.symbols_USDT

    def get_prices(self, symbol):
        """"""
        return self.deques[symbol]

    def get_ohlcv(self, symbol):
        """"""
        data = self.ohlcv[symbol]
        return data[0], data[1], data[2], data[3], data[4], data[5]

    def get_window_ohlcv(self, symbol):
        """"""
        con = create_engine('sqlite:///databases/market_prices.db')
        df = pd.read_sql(
            "SELECT * FROM (SELECT * FROM" + "'{}'".format(symbol) + "ORDER BY time DESC limit 100) ORDER BY time ASC",
            con)
        return df

    def fetch_ohlcv_seq(self):

        msec = 1000
        minute = 60 * msec
        hold = 30

        db = sqlite3.connect(self.db_file)
        c = db.cursor()
        ohlcv = dict()

        for symbol in self.symbols_USDT:

            c.execute("SELECT * FROM" + "'{}'".format(
                symbol) + "ORDER BY time ASC limit 1")
            first_row = c.fetchall()

            start_datetime = first_row[0][0]
            start_timestamp = int(time.mktime(datetime.datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S').timetuple())) * 1000

            since_timestamp = int(time.mktime(self.since_datetime.timetuple())) * 1000

            data = []

            if self.exchange.has['fetchOHLCV']:

                while since_timestamp + 500 * minute < start_timestamp:
                    try:
                        ohlcvs = self.exchange.fetch_ohlcv(symbol, '1m', since_timestamp)
                        since_timestamp += len(ohlcvs) * minute
                        data += ohlcvs



                    except (
                    ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:

                        print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
                        time.sleep(hold)

                n_fetch = math.floor((start_timestamp - data[-1][0]) / 60000) - 1
                print('fetching last')
                print(symbol)

                ohlcvs = self.exchange.fetch_ohlcv(symbol, '1m', since_timestamp, limit=n_fetch)

                data += ohlcvs

                data = list(zip(*data))

                data[0] = [datetime.datetime.fromtimestamp(ms / 1000)
                           for ms in data[0]]

                ohlcv[symbol] = data

                c.execute("CREATE TABLE IF NOT EXISTS " + "'{}'".format(
                    symbol) + "(time REAL, open REAL, high REAL, low REAL, close REAL, volume REAL)")
                c.executemany("INSERT INTO " + "'{}'".format(symbol) + "VALUES(?,?,?,?,?,?)",
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
            self.symbols_USDT[0]) + "ORDER BY time DESC limit 1")
        last_row = c.fetchall()

        db_current_time = True

        while db_current_time :

            if datetime.datetime.fromtimestamp(self.exchange.fetch_ohlcv(self.symbols_USDT[0], timeframe='1m', limit=1)[0][0] / 1000) == datetime.datetime.strptime(last_row[0][0], '%Y-%m-%d %H:%M:%S') :
                time.sleep(15)
            else :

                db_current_time = False


        while True:

            start_time = datetime.datetime.now()
            for symbol in self.symbols_USDT:

                self.last_ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe='1m', limit=1)
                self.last_ohlcv[0][0] = datetime.datetime.fromtimestamp(self.last_ohlcv[0][0] / 1000)

                c.execute("CREATE TABLE IF NOT EXISTS " + "'{}'".format(
                    symbol) + "(time REAL, open REAL, high REAL, low REAL, close REAL, volume REAL)")
                c.executemany("INSERT INTO " + "'{}'".format(symbol) + "VALUES(?,?,?,?,?,?)",
                              self.last_ohlcv)
                db.commit()

            wait_time = 60 - (datetime.datetime.now() - start_time).seconds
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


