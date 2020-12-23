#!/usr/bin/env python3


import logging
import time
from rdt import RDTSocket, RDTSegment

SERVER_ADDR = '127.0.0.1'
SERVER_PORT = 18888
BUFFER_SIZE = 6144


def unit_convert(value):
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = 1024.0
    for i in range(len(units)):
        if (value / size) < 1:
            return "%.2f%s" % (value, units[i])
        value = value / size


DATA_END = b'@'

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[CLIENT %(levelname)s] %(asctime)s: %(message)s')
    client = RDTSocket()
    client.connect((SERVER_ADDR, SERVER_PORT))
    alice = open('alice.txt', 'rb')
    MESSAGE = alice.read()
    meg_len = len(MESSAGE)
    start = time.time()
    client.send(MESSAGE)
    mid = time.time()
    print('-------------------------')
    print(f'client send OK, data size: {unit_convert(len(MESSAGE))}, send time cost: {mid - start} s')
    print('-------------------------')
    data = bytearray()
    time.sleep(1)
    data.extend(client.recv(BUFFER_SIZE))
    while len(data) != meg_len:
        time.sleep(0.1)
        data.extend(client.recv(BUFFER_SIZE))
    end = time.time()
    assert bytes(data) == MESSAGE
    print(f'client recv OK, data size: {len(data)}')
    print('==========================')
    print(f'client recv OK, data size: {unit_convert(len(MESSAGE))} bytes, recv time cost: {end - mid} s')
    print(f'Total time cost: {end - start}')
    print('==========================')

    # client.close()
