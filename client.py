#!/usr/bin/env python3


import logging
import time
from rdt import RDTSocket,RDTSegment

SERVER_ADDR = '127.0.0.1'
SERVER_PORT = 18888
BUFFER_SIZE = 2048

def unit_convert(value):
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = 1024.0
    for i in range(len(units)):
        if (value / size) < 1:
            return "%.2f%s" % (value, units[i])
        value = value / size

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[CLIENT %(levelname)s] %(asctime)s: %(message)s')
    client = RDTSocket()
    client.connect((SERVER_ADDR, SERVER_PORT))
    MESSAGE = '0'*54600
    start = time.time()
    client.send(MESSAGE.encode())
    print(f'client send OK, data size: {len(MESSAGE)}')
    data = bytearray()
    while len(data) < 54600:
        data.extend(client.recv(BUFFER_SIZE))
        print(len(data))
    print(f'client recv OK, data size: {len(data)}')
    print('==========================')
    print(f'time cost: {time.time() - start} s,  data len: {unit_convert(len(MESSAGE.encode()))}')
    print('==========================')
    assert bytes(data).decode() == MESSAGE
    # client.close()
