#!/usr/bin/python3
from threading import Thread
from collections import deque
import sys
import socket
import json
import time

ID_MAX = 2 ** 32 - 1


# milliseconds from epoch
def now():
    return int(round(time.time() * 1000))


class Exchange:
    def __init__(self, hostname):
        self.hostname = hostname
        self.sock = None

        self.reset()

    def reset(self):
        print("---- RESET ----")
        self.orders_dict = {}  # order_id -> (date, SYM, price, amt)

        # the state of the book
        self.sells = {}  # SYM -> (mean, low, num, high, num)
        self.buys = {}  # SYM -> (mean, low, num, high, num)

        self.fullbook_sells = {}
        self.fullbook_buys = {}

        self.our_sell_avg = {}  # SYM -> (sum, denom)
        self.our_buy_avg = {}  # SYM -> (sum, denom)

        # our current positions
        self.positions = {}  # SYM -> integer

        self.ID = 0
        self.orders = []

        # valbz and vale state
        self.valbz_rolling = deque(maxlen=100)

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
    def convert(self, sym, direction, size):
        self.ID += 1
        self.write({
            "type": "convert",
            "order_id": self.ID,
            "symbol": sym,
            "dir": direction,
            "size": size
        })

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
                self.reset()
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
                    getattr(self, "fullbook_" + kind + "s")[sym] = dat[kind]
            elif msg_type == "reject":
                print("REJECTED: ", dat["error"], file=sys.stderr)
                self.orders_dict.pop(dat["order_id"], None)
                if dat["error"] == "TRADING_CLOSED":
                    self.reset()
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
                if dat["symbol"] == "VALBZ":
                    self.valbz_rolling.append(dat["price"])



def order_pruning(exchange):
    d = {}
    for o_id, (_, sym, _, _) in exchange.orders_dict.items():
        if sym not in d:
            d[sym] = []
        d[sym].append(o_id)

    for sym, l in d.items():
        if len(l) > 90:
            for o_id in l[:80]:
                exchange.cancel(o_id)

def confirm(exchange, sym, direction):
    last = -float("inf") if direction == "UP" else float("inf")
    while True:
        pos = exchange.positions.get(sym)
        if pos == None:
            break
        if direction == "UP" and pos > last:
            break
        elif direction == "DOWN" and pos < last:
            break
        time.sleep(0.001)

def vale_valbz(exchange):
    vale_buy = exchange.buys.get("VALE")
    valbz_buy = exchange.buys.get("VALBZ")
    vale_sell = exchange.sells.get("VALE")
    valbz_sell = exchange.sells.get("VALBZ")

    if valbz_buy == None or valbz_sell == None or vale_buy == None or vale_sell == None:
        return
    
    # mean, low, num, high, num (self, order_id, sym, direction, size)

    state_vale = exchange.positions.get("VALE")
    state_valbz = exchange.positions.get("VALBZ")

    if state_vale is None or state_valbz is None:
        return

    if vale_sell[1] is not None and valbz_buy[3] is not None and vale_sell[1] + 10 < valbz_buy[3]:
        if state_vale < 10:
            exchange.buy("VALE", vale_sell[1], 10)
        if state_vale > 0:
            exchange.convert("VALE", "SELL", 10)
        if state_valbz > 0:
            exchange.sell("VALBZ", valbz_buy[3], 10)

    elif valbz_sell[1] is not None and vale_buy[3] is not None and valbz_sell[1] + 10 < vale_buy[3]:
        if state_valbz < 10:
            exchange.buy("VALBZ", vale_sell[1], 10)
        if state_valbz > 0:
            exchange.convert("VALBZ", "SELL", 10)
        if state_vale > 0:
            exchange.sell("VALE", vale_buy[3], 10)

def bond_trade(exchange):
    state = exchange.positions.get("BOND")
    if state is None:
        return

    if state == 0:
        exchange.buy("BOND", 999, 1)
    elif state > 0:
        exchange.sell("BOND", 1001, 1)
    pos = exchange.positions.get("BOND")
    if pos is None:
        return

    cur_orders = list(filter(
        lambda x: x[1][1] == "BOND",
        exchange.orders_dict.items()
    ))

    if len(cur_orders) < 50:
        if pos < -70:
            exchange.buy("BOND", 999, 2)
        elif pos > 70:
            exchange.sell("BOND", 1001, 2)
        else:
            exchange.buy("BOND", 999, 1)
            exchange.sell("BOND", 1001, 1)


def trade(exchange):
    MIN = float('inf')
    MAX = -float('inf')
    buy_symb = ""
    sell_symb = ""

    for symb in exchange.buys:
        if symb == "XLF" or symb == "BOND":
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

    # for sym in exchange.positions:
    #     quantity = exchange.positions.get(sym)

    #     if quantity is None:
    #         continue

    #     if quantity >= 50:
    #         temp = exchange.buys.get(sym)

    #         if temp is not None:
    #             exchange.sell(sym, temp[0], 50)

    #     if quantity <= -50:
    #         temp = exchange.sells.get(sym)

    #         if temp is not None:
    #             exchange.buy(sym, temp[0], 50)
                


def threading_wrapper(func, exchange, interval):
    def runner():
        while True:
            func(exchange)
            time.sleep(interval)
    return Thread(target=runner)


def main():
    if len(sys.argv) != 2:
        print("Please specify prod or test")
        exit(1)

    if sys.argv[1].lower() in ("prod", "production"):
        e = Exchange("production")
        print("--- PRODUCTION PRODUCTION PRODUCTION ---")
    else:
        e = Exchange("test-exch-BIGBOARDTRIO")
        print("--- TEST ---")

    threading_wrapper(bond_trade, e, 0.03).start()
    threading_wrapper(vale_valbz, e, 0.03).start()
    threading_wrapper(order_pruning, e, 5).start()
    e.run()


if __name__ == "__main__":
    main()
