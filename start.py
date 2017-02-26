#!/usr/bin/python3
import sys
import socket
import json
import time


class Exchange:
    def __init__(self, hostname):
        self.hostname = hostname
        self.handler = []
        self.sock = None
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

    def run(self):
        self.connect()
        while True:
            dat = self.read()
            if dat is None:
                self.connect()
            else:
                self.trade(dat)

            time.sleep(0.001)

    def trade(self, obj):
        if obj["type"] != "book":
            return

        if obj["symbol"] == "XLF":
            return

        buys = obj["buy"]

        MAX = -float("inf")
        for buy in buys:
            if buy[0] > MAX:
                MAX = buy[0]

        sells = obj["sell"]

        MIN = float("inf")
        for sell in sells:
            if sell[0] < MIN:
                MIN = sell[0]

        if MAX - 1 <= MIN + 1:
            return

        self.write({"type": "add", "order_id": self.ID, "symbol": obj["symbol"], "dir": "SELL", "price": MAX - 1, "size": 1})
        self.orders.append(self.ID)

        if len(self.orders) > 100:
            self.write({"type": "cancel", "order_id": self.orders.pop(0)})

        self.ID += 1
        
        self.write({"type": "add", "order_id": self.ID, "symbol": obj["symbol"], "dir": "BUY", "price": MIN + 1, "size": 1})
        self.orders.append(self.ID)

        if len(self.orders) > 100:
            self.write({"type": "cancel", "order_id": self.orders.pop(0)})

        self.ID += 1

def main():
    # e = Exchange("localhost")
    e = Exchange("production")
    e.run()


if __name__ == "__main__":
    main()
