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
            print("!!! WRITE FAILED !!!", file=sys.stderr)
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
        print("ADD", id_)
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
                continue

            msg_type = dat["type"]
            if msg_type == "hello":
                for sym_o in dat["symbols"]:
                    self.positions[sym_o["symbol"]] = sym_o["position"]
            elif msg_type == "book":
                sym = dat["symbol"]
                for kind in ("buy", "sell"):
                    if len(dat[kind]) == 0:
                        min_o = (None, 0)
                        max_o = (None, 0)
                        mean_o = None
                    else:
                        min_o = tuple(min(dat[kind], key=lambda d: d[0]))
                        max_o = tuple(max(dat[kind], key=lambda d: d[0]))
                        mean_o = sum(map(lambda d: d[0], dat[kind])) // len(dat[kind])
                    getattr(self, kind + "s")[sym] = (mean_o,) + min_o + max_o
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
                self.positions["USD"] = cash
            elif msg_type == "ack":
                print("ACK", dat["order_id"])
            elif msg_type == "trade":
                pass


def bond_trade(exchange):
    state = exchange.positions["BOND"]
    if state == 0:
        exchange.buy("BOND", 999, 1)
    elif state > 0:
        exchange.sell("BOND", 1001, 1)
"""
    mode = "BUY"
    ordered = False
    last = None
    while True:
        if mode == "BUY":
            pos = exchange.positions["BOND"]
            if pos != last:
                if not ordered:
                    exchange.buy("BOND", 999, 10)
                    ordered = True
                else:
                    ordered = False
                    mode = "SELL"
            last = pos
        else:
            pos = exchange.positions["BOND"]
            if pos != last:
                if not ordered:
                    exchange.sell("BOND", 999, 10)
                    ordered = True
                else:
                    ordered = False
                    mode = "SELL"
            last = pos
"""

def trade(exchange):
    MIN = float('inf')
    MAX = -float('inf')
    buy_symb = ""
    sell_symb = ""

    for symb in exchange.buys:
        if symb == "XLF":
            continue

        high = exchange.buys.get(symb)
        low = exchange.sells.get(symb)

        print("H", high)
        print("L", low)

        if high is None or low is None:
            continue

        h_mean, h_low, h_low_num, h_high, h_high_num = high
        l_mean, l_low, l_low_num, l_high, l_high_num = low

        if h_mean is None or l_mean is None:
            continue

        if h_high > MAX:
            MAX = h_high
            sell_symb = symb

        if l_low < MIN:
            MIN = l_low
            buy_symb = symb

        print("Yo")

    if MAX - 1 <= MIN + 1 or sell_symb == "" or buy_symb == "":
        return

    exchange.sell(sell_symb, MAX - 1, 1)
    exchange.buy(buy_symb, MIN + 1, 1)


def main():
    # e = Exchange("localhost")
    from threading import Thread, Timer
    e = Exchange("test-exch-BIGBOARDTRIO")

    def strategies_runner():
        while True:
            trade(e)
            bond_trade(e)
            time.sleep(0.05)
    Thread(target=strategies_runner).start()

    e.run()


if __name__ == "__main__":
    main()
