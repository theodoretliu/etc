#!/usr/bin/python3
import sys
import socket
import json
import time


class Exchange:
    hostname = ""
    handlers = []
    sock = None

    def __init__(self, hostname):
        self.hostname = hostname

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


def main():
    e = Exchange("localhost")
    # e = Exchange("test-exch-BIGBOARDTRIO")
    e.run()


if __name__ == "__main__":
    main()
