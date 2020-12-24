#!/usr/bin/env python3

import logging
from rdt import RDTSocket, server_logger
import time, threading

SERVER_ADDR = '127.0.0.1'
SERVER_PORT = 18888

BUFFER_SIZE = 10240

DATA_END = b'@'


class Echo(threading.Thread):
    def __init__(self, conn, address):
        threading.Thread.__init__(self)
        self.conn = conn
        self.address = address

    def run(self):
        data = bytearray()
        res_len = 152138

        while not self.conn.local_closed:
            while len(data) < res_len and not self.conn.local_closed:
                data.extend(self.conn.recv(BUFFER_SIZE))
                if len(data) > 152138/2 + 10:
                    break
            if len(data) != 0:
                self.conn.send(data)  # echo
                print('note send~')
                res_len = res_len - len(data)
                data = bytearray()
            else:
                time.sleep(0.1)
        print('closed')


if __name__ == '__main__':
    server = RDTSocket()
    server.bind((SERVER_ADDR, SERVER_PORT))
    try:
        while True:
            conn, client_addr = server.accept()
            Echo(conn, client_addr).start()

            # data = bytearray()
            # while True:
            #     time.sleep(0.1)
            #     data = conn.recv(BUFFER_SIZE)
            #     if len(data) != 0:
            #         conn.send(data)  # echo
            #     else:
            #         time.sleep(0.1)
                # while data[-1] != DATA_END:
                #     time.sleep(0.1)
                #     data.extend(conn.recv(BUFFER_SIZE))
                # print(f'server recv OK, data size: {len(data)}')
                # if data:
                #     conn.send(bytes(data))  # echo
                #     print(f'server send OK, data size: {len(data)}')
                # time.sleep(200)
            # conn.close()
    except KeyboardInterrupt as k:
        print(k)
