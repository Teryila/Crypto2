import csv
import http.server
import json
import operator
import os.path
import random
import threading
from datetime import timedelta, datetime
from random import normalvariate
import dateutil.parser

################################################################################
# Configuration

# Simulation parameters
REALTIME = True
SIM_LENGTH = timedelta(days=365 * 5)
MARKET_OPEN = datetime.today().replace(hour=0, minute=30, second=0)

# Market parameters for cryptocurrencies (DOGE and BTC)
SPD = (2.0, 6.0, 0.1)   # Spread parameters
PX = (60.0, 150.0, 1)   # Price parameters
FREQ = (12, 36, 50)     # Frequency parameters
OVERLAP = 4

################################################################################
# Test Data

def bwalk(min_val, max_val, std_dev):
    """Generates a bounded random walk."""
    rng = max_val - min_val
    while True:
        max_val += normalvariate(0, std_dev)
        yield abs((max_val % (rng * 2)) - rng) + min_val

def market(t0=MARKET_OPEN):
    """Generates a random series of market conditions (time, price, spread)."""
    for hours, px, spd in zip(bwalk(*FREQ), bwalk(*PX), bwalk(*SPD)):
        yield t0, px, spd
        t0 += timedelta(hours=abs(hours))

def orders(hist):
    """Generates a random set of limit orders (time, stock, side, price, size)."""
    for t, px, spd in hist:
        stock = 'DOGE' if random.random() > 0.5 else 'BTC'
        side, d = ('sell', 2) if random.random() > 0.5 else ('buy', -2)
        order = round(normalvariate(px + (spd / d), spd / OVERLAP), 2)
        size = int(abs(normalvariate(0, 100)))
        yield t, stock, side, order, size

################################################################################
# Order Book

def add_book(book, order, size, _age=10):
    """Add a new order and size to a book, and age the rest of the book."""
    yield order, size, _age
    for o, s, age in book:
        if age > 0:
            yield o, s, age - 1

def clear_order(order, size, book, op=operator.ge, _notional=0):
    """Try to clear a sized order against a book, returning a tuple of (notional, new_book) if successful."""
    (top_order, top_size, age), tail = book[0], book[1:]
    if op(order, top_order):
        _notional += min(size, top_size) * top_order
        sdiff = top_size - size
        if sdiff > 0:
            return _notional, list(add_book(tail, top_order, sdiff, age))
        elif len(tail) > 0:
            return clear_order(order, -sdiff, tail, op, _notional)

def clear_book(buy=None, sell=None):
    """Clears all crossed orders from a buy and sell book, returning the new books uncrossed."""
    while buy and sell:
        order, size, _ = buy[0]
        new_book = clear_order(order, size, sell)
        if new_book:
            sell = new_book[1]
            buy = buy[1:]
        else:
            break
    return buy, sell

def order_book(orders, book, stock_name):
    """Generates a series of order books from a series of orders."""
    for t, stock, side, order, size in orders:
        if stock_name == stock:
            new = add_book(book.get(side, []), order, size)
            book[side] = sorted(new, reverse=side == 'buy', key=lambda x: x[0])
        bids, asks = clear_book(**book)
        yield t, bids, asks

################################################################################
# Test Data Persistence

def generate_csv():
    """Generate a CSV of order history."""
    with open('test.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Time', 'Stock', 'Side', 'Price', 'Size'])
        for t, stock, side, order, size in orders(market()):
            if t > MARKET_OPEN + SIM_LENGTH:
                break
            writer.writerow([t.isoformat(), stock, side, order, size])

def read_csv(filename='test.csv'):
    """Read data from a CSV file and return as a list of dictionaries."""
    data = []
    try:
        with open(filename, 'r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                print(f"Read row: {row}")  # Debug print
                data.append(row)
    except FileNotFoundError:
        print(f"Error: The file {filename} does not exist.")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
    return data

################################################################################
# Server

class ThreadedHTTPServer(threading.Thread):
    """Threaded HTTP Server class for handling HTTP requests concurrently."""

    def __init__(self, host='localhost', port=8081):
        super().__init__()
        self.host = host
        self.port = port
        self.server = None

    def run(self):
        """Start the HTTP server."""
        server_address = (self.host, self.port)
        handler = RequestHandler
        self.server = http.server.HTTPServer(server_address, handler)
        print(f"HTTP server started on {self.host}:{self.port}")
        self.server.serve_forever()

    def shutdown(self):
        """Shutdown the HTTP server."""
        if self.server:
            self.server.shutdown()
            self.join()

class RequestHandler(http.server.BaseHTTPRequestHandler):
    """Custom request handler for HTTP server."""

    def do_GET(self):
        """Handle GET requests."""
        if self.path.startswith('/query'):
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            params = self.read_params(self.path)
            data = self.handle_query(params)
            print(f"Response Data: {data}")  # Debug print
            self.wfile.write(bytes(json.dumps(data) + '\n', encoding='utf-8'))

    def read_params(self, path):
        """Read query parameters into a dictionary."""
        query = path.split('?')
        if len(query) > 1:
            query = query[1].split('&')
            return dict(map(lambda x: x.split('='), query))
        return {}

    def handle_query(self, params):
        """Handle query requests and return current top of the order book."""
        app = App()
        return app.handle_query(params)

################################################################################
# App

class App:
    """Application class for managing order books and handling queries."""

    def __init__(self):
        self._book_doge = {'buy': [], 'sell': []}
        self._book_btc = {'buy': [], 'sell': []}
        self._data_doge = order_book(read_csv(), self._book_doge, 'DOGE')
        self._data_btc = order_book(read_csv(), self._book_btc, 'BTC')
        self._rt_start = datetime.now()
        self._sim_start, _, _ = next(self._data_doge)

    @property
    def _current_book_doge(self):
        """Generator property for fetching current order book data for DOGE."""
        for t, bids, asks in self._data_doge:
            if REALTIME:
                while t > self._sim_start + (datetime.now() - self._rt_start):
                    yield t, bids, asks
            else:
                yield t, bids, asks

    @property
    def _current_book_btc(self):
        """Generator property for fetching current order book data for BTC."""
        for t, bids, asks in self._data_btc:
            if REALTIME:
                while t > self._sim_start + (datetime.now() - self._rt_start):
                    yield t, bids, asks
            else:
                yield t, bids, asks

    def handle_query(self, params):
        """Handle query parameters and return current top of the order book."""
        print(f"Handling query with params: {params}")  # Debug print
        if 'stock' in params:
            stock = params['stock'].upper()
            if stock == 'DOGE':
                return {'bids': self.current_bids(self._current_book_doge),
                        'asks': self.current_asks(self._current_book_doge)}
            elif stock == 'BTC':
                return {'bids': self.current_bids(self._current_book_btc),
                        'asks': self.current_asks(self._current_book_btc)}
        return {}

    def current_bids(self, book):
        """Return the current top bids from the order book."""
        try:
            return next(book)[1]
        except StopIteration:
            return []

    def current_asks(self, book):
        """Return the current top asks from the order book."""
        try:
            return next(book)[2]
        except StopIteration:
            return []

################################################################################
# Main

if __name__ == '__main__':
    generate_csv()
    server = ThreadedHTTPServer()
    server.start()
    try:
        while True:
            pass
    except KeyboardInterrupt:
        server.shutdown()
        print("Server stopped.")
