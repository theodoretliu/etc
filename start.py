#!/usr/bin/python3
import sys
import socket
import json
import time
import datetime

ID_MAX = 2 ** 32 - 1

class Exchange:
    def __init__(self, hostname):
        self.hostname = hostname
        self.sock = None
        self.orders_dict = {}  # order_id -> (date, SYM, price, amt)
        self.sells = {}  # SYM -> (mean, low, num, high, num)
        self.buys = {}  # SYM -> (mean, low, num, high, num)
        self.positions = {}  # SYM -> integer
        self.cash = 0
        self.ID = 0
        self.orders = []

    def connect(self):
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.hostname, 25000))
                break
            except:
                time.sleep(1)

        self.sock = s.makefile('rw', 1)
        self.write({"type": "hello", "team": "BIGBOARDTRIO"})

    def read(self):
        r = self.sock.readline()
        if not r:
            return None
        else:
            return json.loads(r)

    def write(self, obj):
        try:
            json.dump(obj, self.sock)
            self.sock.write("\n")
            return True
        except:
            return None

    def buy(self, sym, price, size):
        self.add("buy", sym, price, size)

    def sell(self, sym, price, size):
        self.add("sell", sym, price, size)

    def add(self, dir, sym, price, size):
        id_ = self.ID
        self.write({
            "order_id": id_,
            "symbol": sym,
            "dir": dir,
            "price": price,
            "size": size
        })
        self.ID = (id_ + 1) % ID_MAX
        pass

    def run(self):
        self.connect()
        while True:
            dat = self.read()
            if dat is None:
                self.connect()
            else:
                self.trade(dat)

            msg_type = dat["type"]
            if msg_type == "hello":
                for sym_o in dat["symbols"]:
                    self.positions[sym_o.symbol] = sym_o.position
            elif msg_type == "book":
                sym = dat["symbol"]
                for kind in ("buy", "sell"):
                    min_o = min(dat[kind], lambda d: d.price)
                    max_o = max(dat[kind], lambda d: d.price)
                    mean_o = sum(map(lambda d: d.price, dat[kind])) // len(dat[kind])
                    getattr(self, kind + "s")()[sym] = (mean_o,) + min_o + max_o


    def trade(self, obj):
        for symb in self.buys:
            if symb == "XLF":
                continue

            high = self.buys.get(symb)
            low = self.sells.get(symb)

            if high is None or low is None:
                continue

            high = high[3]
            low = low[1]

            if high - 1 <= low + 1:
                continue

            self.sell(symb, high - 1, 1)
            self.buy(symb, low + 1, 1)

        # if obj["type"] != "book":
        #     return

        # if obj["symbol"] == "XLF":
        #     return

        # buys = obj["buy"]

        # MAX = -float("inf")
        # for buy in buys:
        #     if buy[0] > MAX:
        #         MAX = buy[0]

        # sells = obj["sell"]

        # MIN = float("inf")
        # for sell in sells:
        #     if sell[0] < MIN:
        #         MIN = sell[0]

        # if MAX - 1 <= MIN + 1:
        #     return

        # self.write({"type": "add", "order_id": self.ID, "symbol": obj["symbol"], "dir": "SELL", "price": MAX - 1, "size": 1})
        # self.orders.append(self.ID)

        # if len(self.orders) > 100:
        #     self.write({"type": "cancel", "order_id": self.orders.pop(0)})

        # self.ID += 1
        
        # self.write({"type": "add", "order_id": self.ID, "symbol": obj["symbol"], "dir": "BUY", "price": MIN + 1, "size": 1})
        # self.orders.append(self.ID)

        # if len(self.orders) > 100:
        #     self.write({"type": "cancel", "order_id": self.orders.pop(0)})

        # self.ID += 1

def main():
    # e = Exchange("localhost")
    e = Exchange("test-exch-BIGBOARDTRIO")
    e.run()


if __name__ == "__main__":
    main()
