#!/usr/bin/env python3

import logging
from rdt import RDTSocket, server_logger

SERVER_ADDR = '127.0.0.1'
SERVER_PORT = 18888

BUFFER_SIZE = 2048

if __name__ == '__main__':
    server = RDTSocket()
    server.bind((SERVER_ADDR, SERVER_PORT))
    try:
        while True:
            conn, client = server.accept()
            while True:
                data = conn.recv(BUFFER_SIZE)
                if not data:
                    break
                conn.send(data)  # echo
            conn.close()
    except KeyboardInterrupt as k:
        print(k)
