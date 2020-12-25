#!/usr/bin/env python3

import time
from rdt import RDTSocket, RDTSegment
from difflib import Differ

SERVER_ADDR = '127.0.0.1'
SERVER_PORT = 18888
BUFFER_SIZE = 10240

def unit_convert(value):
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = 1024.0
    for i in range(len(units)):
        if (value / size) < 1:
            return "%.2f%s" % (value, units[i])
        value = value / size

def test00():
    client = RDTSocket(debug=False)
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
        time.sleep(0.08)
        data.extend(client.recv(BUFFER_SIZE))
    end = time.time()
    assert bytes(data) == MESSAGE
    print(f'client recv OK, data size: {len(data)}')
    print('==========================')
    print(f'client recv OK, data size: {unit_convert(len(MESSAGE))} bytes, recv time cost: {end - mid} s')
    print(f'Total time cost: {end - start}')
    print('==========================')

    client.close()


def test01():
    client = RDTSocket()
    client.connect(('127.0.0.1', 9999))

    data_count = 0
    echo = b''
    count = 3

    with open('alice.txt', 'r') as f:
        data = f.read()
        encoded = data.encode()
        assert len(data) == len(encoded)

    start = time.perf_counter()
    for i in range(count):  # send 'alice.txt' for count times
        data_count += len(data)
        client.send(encoded)

    '''
    blocking send works but takes more time 
    '''

    while True:
        reply = client.recv(2048)
        echo += reply
        print(reply)
        if len(echo) == len(encoded) * count:
            break
    client.close()

    '''
    make sure the following is reachable
    '''

    print(f'transmitted {data_count}bytes in {time.perf_counter() - start}s')
    diff = Differ().compare((data*count).splitlines(keepends=True), echo.decode().splitlines(keepends=True))

    for line in diff:
        assert line.startswith('  ')  # check if data is correctly echoed


if __name__ == '__main__':
    test01()