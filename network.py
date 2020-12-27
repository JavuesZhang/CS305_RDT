from socket import socket, AF_INET, SOCK_DGRAM, inet_aton, inet_ntoa
import random, time
import threading, queue
import rdt
from socketserver import ThreadingUDPServer

lock = threading.Lock()

loss_rate = 0.1
corrupt_rate = 0.00001
corrupt_rate_byte = (1-corrupt_rate)**8
buffer_size = 100000


def bytes_to_addr(bytes):
    return inet_ntoa(bytes[:4]), int.from_bytes(bytes[4:8], 'big')


def addr_to_bytes(addr):
    return inet_aton(addr[0]) + addr[1].to_bytes(4, 'big')


class Server(ThreadingUDPServer):
    def __init__(self, addr, rate=None, delay=None):
        super().__init__(addr, None)
        self.rate = rate
        self.buffer = 0
        self.delay = delay

    def verify_request(self, request, client_address):
        """
        request is a tuple (data, socket)
        data is the received bytes object
        socket is new socket created automatically to handle the request

        if this function returns False， the request will not be processed, i.e. is discarded.
        details: https://docs.python.org/3/library/socketserver.html
        """
        if self.buffer + len(request[0]) < buffer_size:  # some finite buffer size (in bytes)
            self.buffer += len(request[0])
            return True
        else:
            return False

    def finish_request(self, request, client_address):
        data, socket = request

        with lock:
            if self.rate: time.sleep(len(data) / self.rate)
            self.buffer -= len(data)
            """
            blockingly process each request
            you can add random loss/corrupt here

            for example:
            if random.random() < loss_rate:
                return 
            for i in range(len(data)-1):
                if random.random() < corrupt_rate:
                    data = data[:i] + (data[i]+1).to_bytes(1,'big) + data[i+1:]
            """

            if random.random() < loss_rate:
                drop = rdt.RDTSegment.parse(data[8:])
                print(client_address, bytes_to_addr(data[:8]), f" packet loss, {drop.log_raw_info()}")
                # print(client_address, bytes_to_addr(data[:8]), f" packet loss")
                return

            # if random.random() < corrupt_rate:
            #     data = bytearray(data)
            #     for i in range(0, random.randint(0, 3)):
            #         pos = random.randint(0, len(data) - 1)
            #         data[pos] = random.randint(0, 255)
            #     data = bytes(data)
            #     corrupt = rdt.RDTSegment.parse(data[8:])
            #     print(client_address, bytes_to_addr(data[:8]), f" packet corrupt, {corrupt.log_info()}")

            corrupt = rdt.RDTSegment.parse(data[8:])
            data = bytearray(data)
            is_corrupted = False
            for i in range(8, len(data)):
                if random.random() > corrupt_rate_byte:
                    is_corrupted = True
                    data[i] = data[i] ^ random.randint(0, 255)
            if is_corrupted and corrupt:
                print(client_address, bytes_to_addr(data[:8]), f" corrupt pkt, {corrupt.log_raw_info()}")
            data = bytes(data)
        """
        this part is not blocking and is executed by multiple threads in parallel
        you can add random delay here, this would change the order of arrival at the receiver side.

        for example:
        time.sleep(random.random())
        """

        to = bytes_to_addr(data[:8])
        print(client_address, to)  # observe tht traffic
        socket.sendto(addr_to_bytes(client_address) + data[8:], to)


server_address = ('127.0.0.1', 11223)
network2 = ('127.0.0.1', 11224)

if __name__ == '__main__':
    with Server(server_address) as server:
        server.serve_forever()
