#!/usr/bin/env python3


import logging
from time import time
from rdt import RDTSocket,RDTSegment

SERVER_ADDR = '127.0.0.1'
SERVER_PORT = 18888
BUFFER_SIZE = 2048
MESSAGE = 'rdtrdtrdtrdt'
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[CLIENT %(levelname)s] %(asctime)s: %(message)s')
    client = RDTSocket()
    client.connect((SERVER_ADDR, SERVER_PORT))
    # client.send(MESSAGE.encode())
    # data = client.recv(BUFFER_SIZE)
    # assert data == MESSAGE
    # client.close()
