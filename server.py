#!/usr/bin/env python3

import logging
from rdt import RDTSocket, server_logger
import time

SERVER_ADDR = '127.0.0.1'
SERVER_PORT = 18888

BUFFER_SIZE = 6144

DATA_END = b'@'

if __name__ == '__main__':
    server = RDTSocket()
    server.bind((SERVER_ADDR, SERVER_PORT))
    try:
        while True:
            conn, client = server.accept()
            data = bytearray()
            while True:
                time.sleep(0.1)
                data = conn.recv(BUFFER_SIZE)
                if len(data) != 0:
                    conn.send(data)  # echo
                    print('\n\n\n\n\n\n\n\n\n\n\n')
                else:
                    time.sleep(0.1)
                # while data[-1] != DATA_END:
                #     time.sleep(0.1)
                #     data.extend(conn.recv(BUFFER_SIZE))
                # print(f'server recv OK, data size: {len(data)}')
                # if data:
                #     conn.send(bytes(data))  # echo
                #     print(f'server send OK, data size: {len(data)}')
                # time.sleep(200)
            conn.close()
    except KeyboardInterrupt as k:
        print(k)
