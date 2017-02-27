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
    def __init__(self):
        self.reset(None)

    def reset(self, sock):
        print("*** RESET ***")
        self.sock = sock
        self.fail = False

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
        self.vale_ordered_buys = {}  # order_id -> fair_val
        self.vale_ordered_sells = {}  # order_id -> fair_val


        # xlf stuff
        self.gs_rolling = deque(maxlen=100)
        self.ms_rolling = deque(maxlen=100)
        self.wfc_rolling = deque(maxlen=100)
        self.xlf_ordered_buys = {}  # order_id -> fair_val
        self.xlf_ordered_sells = {}  # order_id -> fair_val

        self.write({"type": "hello", "team": "BIGBOARDTRIO"})

    def read(self):
        if self.sock is not None:
            r = self.sock.readline()
            if not r:
                self.fail = True
                return None
            else:
                return json.loads(r)

    def write(self, obj):
        if self.sock is not None:
            try:
                json.dump(obj, self.sock)
                self.sock.write("\n")
            except:
                print("!!! WRITE FAILED !!!", file=sys.stderr)
                self.fail = True

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
        return self.add("BUY", sym, price, size)

    def sell(self, sym, price, size):
        return self.add("SELL", sym, price, size)

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
        return id_

    def cancel(self, order_id):
        self.orders_dict.pop(order_id, None)
        self.write({
            "type": "cancel",
            "order_id": order_id
        })

    def run(self):
        while True:
            dat = self.read()
            if self.fail:
                return

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
                    getattr(self, "fullbook_" + kind + "s")[sym] = dat[kind]
            elif msg_type == "reject":
                print("REJECTED: ", dat["error"], file=sys.stderr)
                self.orders_dict.pop(dat["order_id"], None)
                if dat["error"] == "TRADING_CLOSED":
                    return
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
                    cur += dat["size"]
                    cash -= amt
                elif dat["dir"] == "SELL":
                    cur -= dat["size"]
                    cash += amt
                else:
                    print("WTF: not BUY or SELL", file=sys.stderr)
                self.positions[sym] = cur
                self.positions["USD"] = cash
            elif msg_type == "ack":
                print("ACK", dat["order_id"])
            elif msg_type == "trade":
                sym = dat["symbol"]
                if sym == "VALBZ":
                    self.valbz_rolling.append(dat["price"])
                elif sym == "GS":
                    self.gs_rolling.append(dat["price"])
                elif sym == "MS":
                    self.ms_rolling.append(dat["price"])
                elif sym == "WFC":
                    self.wfc_rolling.append(dat["price"])


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


def xlf_stuff(e):
    if len(e.gs_rolling) < 5 or len(e.ms_rolling) < 5 or len(e.wfc_rolling) < 5:
        return

    m_gs = sum(e.gs_rolling) / len(e.gs_rolling)
    m_ms = sum(e.ms_rolling) / len(e.ms_rolling)
    m_wfc = sum(e.wfc_rolling) / len(e.wfc_rolling)

    fair = (3000 + 2 * m_gs + 3 * m_ms + 2 * m_wfc) / 10

    to_remove = (e.xlf_ordered_buys.keys() | e.xlf_ordered_sells.keys()) - e.orders_dict.keys()
    for r in to_remove:
        e.xlf_ordered_buys.pop(r, None)
        e.xlf_ordered_sells.pop(r, None)

    for id_, old_fair in e.xlf_ordered_buys.items():
        diff = fair - old_fair
        if diff > 16 or diff < -1:
            e.cancel(id_)

    for id_, old_fair in e.xlf_ordered_sells.items():
        diff = fair - old_fair
        if diff < -16 or diff > 1:
            e.cancel(id_)

    owned_shares = e.positions.get("XLF", 0)
    if owned_shares > 50:
        id_ = e.sell("XLF", fair, 20)
        e.xlf_ordered_sells[id_] = fair
    elif owned_shares < -50:
        id_ = e.buy("XLF", fair, 20)
        e.xlf_ordered_buys[id_] = fair

    buy_offers = sorted(e.fullbook_buys.get("XLF"), key=lambda x: x[0], reverse=True)
    sell_offers = sorted(e.fullbook_sells.get("XLF"), key=lambda x: x[0])

    if owned_shares < 95:
        for o in buy_offers:
            if o[0] < (fair - 1):
                id_ = e.buy("XLF", o[0] + 1, min((o[1] + 1) // 2, 10))
                e.xlf_ordered_buys[id_] = fair
                break

    if owned_shares > -95:
        for o in sell_offers:
            if o[0] > (fair + 1):
                id_ = e.sell("XLF", o[0] - 1, min((o[1] + 1) // 2, 10))
                e.xlf_ordered_sells[id_] = fair
                break


def fair_vale(e):
    if len(e.valbz_rolling) == 0:
        return

    fair = sum(e.valbz_rolling) / len(e.valbz_rolling)

    to_remove = (e.vale_ordered_buys.keys() | e.vale_ordered_sells.keys()) - e.orders_dict.keys()
    for r in to_remove:
        e.vale_ordered_buys.pop(r, None)
        e.vale_ordered_sells.pop(r, None)

    for id_, old_fair in e.vale_ordered_buys.items():
        diff = fair - old_fair
        if diff > 16 or diff < -1:
            e.cancel(id_)

    for id_, old_fair in e.vale_ordered_sells.items():
        diff = fair - old_fair
        if diff < -16 or diff > 1:
            e.cancel(id_)

    owned_shares = e.positions.get("VALE", 0)
    if owned_shares > 9:
        e.sell("VALE", fair, 9)
    elif owned_shares < -9:
        e.buy("VALE", fair, 9)

    buy_offers = sorted(e.fullbook_buys.get("VALE"), key=lambda x: x[0], reverse=True)
    sell_offers = sorted(e.fullbook_sells.get("VALE"), key=lambda x: x[0])

    for o in buy_offers:
        if o[0] < (fair - 1):
            id_ = e.buy("VALE", o[0] + 1, min((o[1] + 1) // 2, 5))
            e.vale_ordered_buys[id_] = fair
            break

    for o in sell_offers:
        if o[0] > (fair + 1):
            id_ = e.sell("VALE", o[0] - 1, min((o[1] + 1) // 2, 5))
            e.vale_ordered_sells[id_] = fair
            break


def vale_valbz(exchange):
    vale_buy = exchange.buys.get("VALE")
    valbz_buy = exchange.buys.get("VALBZ")
    vale_sell = exchange.sells.get("VALE")
    valbz_sell = exchange.sells.get("VALBZ")
    
    if any(i == None for i in (vale_sell, vale_buy, valbz_buy, valbz_sell)):
        return
    if any(i == None for i in vale_buy + vale_sell + valbz_buy + valbz_sell):
        return

    state_vale = exchange.positions.get("VALE")
    state_valbz = exchange.positions.get("VALBZ")

    if state_vale is None or state_valbz is None:
        return

    if vale_sell[1] + 10 < valbz_buy[3] - 1:
        exchange.buy("VALE", vale_sell[1] + 1, 10)
        exchange.convert("VALE", "SELL", 10)
        exchange.sell("VALBZ", valbz_buy[3] - 1, 10)

    elif valbz_sell[1] + 10 < vale_buy[3] - 1:
        exchange.buy("VALBZ", vale_sell[1] + 1, 10)
        exchange.convert("VALBZ", "SELL", 10)
        exchange.sell("VALE", vale_buy[3] - 1, 1)

def convert_xlf(exchange):
    gs_buy = exchange.buys.get("GS")
    gs_sell = exchange.sells.get("GS")
    ms_buy = exchange.buys.get("MS")
    ms_sell = exchange.sells.get("MS")
    wfc_buy = exchange.buys.get("WFC")
    wfc_sell = exchange.sells.get("WFC")
    xlf_buy = exchange.buys.get("XLF")
    xlf_sell = exchange.sells.get("XLF")
    if any(i == None for i in (xlf_buy, xlf_sell, gs_buy, gs_sell, ms_buy, ms_sell, wfc_buy, wfc_sell)):
        return

    if not all(all(x) for x in (gs_buy, ms_buy, wfc_buy, xlf_sell)):
        return

    price_to_buy = xlf_sell[1]

    mult = 2
    cost_for_others = 3*1000 + 2*gs_buy[3] + 3*ms_buy[3] + 2*wfc_buy[3]
    print("price to buy: ", price_to_buy * 10)
    print("cost_for_others: ", cost_for_others)
    if price_to_buy*10 + 100 < cost_for_others:
        if exchange.positions["XLF"] >= 100 - 10*mult or exchange.positions["BOND"] >= 100 - 3 * mult \
            or exchange.positions["GS"] >= 100 - 2 * mult or exchange.positions["MS"] >= 100 - 3 * mult \
            or exchange.positions["WFC"] >= 100 - 2 * mult:
            print("????")
            return

        print("LOL")
        exchange.buy("XLF", price_to_buy + 1, 10*mult)
        exchange.convert("XLF", "SELL", 10*mult)
        exchange.sell("BOND", 1001, 3*mult)
        exchange.sell("GS", gs_buy[3] - 1, 2*mult)
        exchange.sell("MS", ms_buy[3] - 1, 3*mult)
        exchange.sell("WFC", wfc_buy[3] - 1, 2*mult)


def bond_trade(exchange):
    pos = exchange.positions.get("BOND")
    if pos is None:
        return

    cur_orders = list(filter(
        lambda x: x[1][1] == "BOND",
        exchange.orders_dict.items()
    ))

    if len(cur_orders) < 50:
        if pos < -70:
            exchange.buy("BOND", 999, 20)
        elif pos > 70:
            exchange.sell("BOND", 1001, 20)
        else:
            exchange.buy("BOND", 999, 1)
            exchange.sell("BOND", 1001, 1)


def cancel_all(exchange, sym_cancel):
    cancel_ids = []
    for o_id, (_, sym, _, _) in exchange.orders_dict.items():
        if sym == sym_cancel:
            cancel_ids.append(o_id)

    for i in cancel_ids:
        exchange.cancel(i)


def trade(exchange):
    # MIN = float('inf')
    # MAX = -float('inf')
    # buy_symb = ""
    # sell_symb = ""

    for symb in exchange.buys:
        if symb != "VALE":
            continue
        cancel_all(exchange, symb)
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

        if l_low - h_high < 3:
            # cancel_all(exchange, symb)
            continue

        exchange.buy(symb, h_high + 1, 1)
        exchange.sell(symb, l_low - 1, 1)

    if len(exchange.orders_dict) > 100:
        exchange.cancel(min(exchange.order_dict))
        #     MAX = h_high
        #     sell_symb = symb

        # if l_low < MIN:
        #     MIN = l_low
        #     buy_symb = symb

        # print("Yo")

    # if MAX - 1 <= MIN + 1 or sell_symb == "" or buy_symb == "":
    #     return

    # exchange.sell(sell_symb, MAX - 1, 1)
    # exchange.buy(buy_symb, MIN + 1, 1)

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
        hostname = "production"
        print("--- PRODUCTION PRODUCTION PRODUCTION ---")
    else:
        hostname = "test-exch-BIGBOARDTRIO"
        print("--- TEST ---")

    e = Exchange()

    # threading_wrapper(bond_trade, e, 0.03).start()
    # threading_wrapper(vale_valbz, e, 0.03).start()
    # threading_wrapper(fair_vale, e, 0.06).start()
    # threading_wrapper(xlf_stuff, e, 0.07).start()
    threading_wrapper(convert_xlf, e, 0.04).start()
    threading_wrapper(order_pruning, e, 5).start()

    s = None

    while True:
        if s is not None:
            try:
                s.close()
            except:
                print("FAILED TO CLOSE SOCKET", file=sys.stderr)

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((hostname, 25000))
            sock = s.makefile('rw', 1)
        except:
            time.sleep(1)
            continue

        e.reset(sock)
        e.run()
        e.reset(None)

        print("WAITING FOR MARKET")
        sys.stdout.flush()
        time.sleep(6)

if __name__ == "__main__":
    main()
