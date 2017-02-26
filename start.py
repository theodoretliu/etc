#!/usr/bin/python3
import sys
import socket
import json
import time
import datetime

ID_MAX = 2 ** 32 - 1


# milliseconds from epoch
def now():
    return int(round(time.time() * 1000))


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
        self.add("BUY", sym, price, size)

    def sell(self, sym, price, size):
        self.add("SELL", sym, price, size)

    def add(self, dir, sym, price, size):
        id_ = self.ID
        self.orders_dict[id_] = (now(), sym, price, size)
        self.write({
            "type": "add",
            "order_id": id_,
            "symbol": sym,
            "dir": dir.upper(),
            "price": price,
            "size": size
        })
        self.ID = (id_ + 1) % ID_MAX

    def cancel(self, order_id):
        self.orders_dict.pop(order_id, None)
        self.write({
            "type": "cancel",
            "order_id": order_id
        })

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
                    self.positions[sym_o["symbol"]] = sym_o["position"]
            elif msg_type == "book":
                sym = dat["symbol"]
                for kind in ("buy", "sell"):
                    min_o = min(dat[kind], lambda d: d.price)
                    max_o = max(dat[kind], lambda d: d.price)
                    mean_o = sum(map(lambda d: d.price, dat[kind])) // len(dat[kind])
                    getattr(self, kind + "s")()[sym] = (mean_o,) + min_o + max_o
            elif msg_type == "reject":
                print("REJECTED: ", dat["error"], file=sys.stderr)
                self.orders_dict.pop(dat["order_id"], None)
            elif msg_type == "error":
                print("ERROR: ", dat["error"], file=sys.stderr)
            elif msg_type == "out":
                self.orders_dict.pop(dat["order_id"], None)
            elif msg_type == "fill":
                sym = dat["symbol"]
                cur = self.positions.get(sym, 0)
                cash = self.positions.get("USD", 0)
                amt = dat["price"] * dat["size"]
                if dat["dir"] == "BUY":
                    cur += amt
                    cash -= amt
                elif dat["dir"] == "SELL":
                    cur -= amt
                    cash += amt
                else:
                    print("WTF: not BUY or SELL", file=sys.stderr)
                self.positions[sym] = cur
                self.positions[sym] = cash
            elif msg_type == "ack":
                pass
            elif msg_type == "trade":
                pass


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
