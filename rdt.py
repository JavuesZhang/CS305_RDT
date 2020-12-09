from typing import Union, Text, Optional, Callable, Any, Iterable, Mapping

from USocket import UnreliableSocket

import struct, queue, threading, time


#############################################################################
# TODO:
# 1. Determine the buff size of each socket, that is, the size of the queue
#############################################################################


class RDTSocket(UnreliableSocket):
    """
    The functions with which you are to build your RDT.
    -   recvfrom(bufsize)->bytes, addr
    -   sendto(bytes, address)
    -   bind(address)

    You can set the mode of the socket.
    -   settimeout(timeout)
    -   setblocking(flag)
    By default, a socket is created in the blocking mode.
    https://docs.python.org/3/library/socket.html#socket-timeouts

    """

    def __init__(self, rate=None, debug=True):
        super().__init__(rate=rate)
        self._rate = rate
        self._send_to = None
        self._recv_from = None
        self.debug = debug
        #############################################################################
        # TODO: ADD YOUR NECESSARY ATTRIBUTES HERE
        #############################################################################

        self.windows = [0] * 120  # win size

        self.segment_buff = queue.Queue()
        self.data_buff = bytes()
        self.allow_accept = False
        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################

    def accept(self) -> ('RDTSocket', (str, int)):
        """
        Accept a connection. The socket must be bound to an address and listening for
        connections. The return value is a pair (conn, address) where conn is a new
        socket object usable to send and receive data on the connection, and address
        is the address bound to the socket on the other end of the connection.

        This function should be blocking.
        """
        conn, addr = RDTSocket(self._rate), None
        #############################################################################
        # TODO: YOUR CODE HERE                                                      #
        #############################################################################
        self.allow_accept = True

        # while True:
        #     seg = super().recv(1024)
        #     if seg is syn:
        #
        #         ack_num = syn_num + 1
        #         syn_num = random()
        #         syn = 1, ack = 1
        #
        #         super().send(encode(...))
        #         while True:
        #             seg = super().recv(1024)
        #             if conn time out:
        #                 break
        #             if seg is ack:
        #                 return new
        #                 socket(), addr
        #             else
        #                 continue
        #
        #     pass

        self.allow_accept = False
        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################
        return conn, addr

    def connect(self, address: (str, int)):
        """
        Connect to a remote socket at address.
        Corresponds to the process of establishing a connection on the client side.
        """
        #############################################################################
        # TODO: YOUR CODE HERE                                                      #
        #############################################################################
        try:
            self.getsockname()
        except Exception:
            self.bind(("", 0))
        syn_seg = RDTSegment(self.getsockname()[1],address[1],1,0,8,syn=True)

        # send syn pkt
        # while True:
        #     seg = recv()
        #     if seg is syn,ack and ack_num == syn_num + 1
        #         send ack pkt
        #         break

        raise NotImplementedError()
        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################

    def recv(self, bufsize: int) -> bytes:
        """
        Receive data from the socket.
        The return value is a bytes object representing the data received.
        The maximum amount of data to be received at once is specified by bufsize.

        Note that ONLY data send by the peer should be accepted.
        In other words, if someone else sends data to you from another address,
        it MUST NOT affect the data returned by this function.
        """
        data = None
        assert self._recv_from, "Connection not established yet. Use recvfrom instead."
        #############################################################################
        # TODO: YOUR CODE HERE                                                      #
        #############################################################################
        recv_data = bytearray()
        while True:
            while self.segment_buff.empty():
                # time.sleep(0.01)
                pass
            seg = self.segment_buff.get()

            pass
            break

        data = recv_data[:bufsize]
        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################
        return data

    def send(self, data: bytes):
        """
        Send data to the socket.
        The socket must be connected to a remote socket, i.e. self._send_to must not be none.
        """
        assert self._send_to, "Connection not established yet. Use sendto instead."
        #############################################################################
        # TODO: YOUR CODE HERE                                                      #
        #############################################################################
        raise NotImplementedError()
        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################

    def close(self):
        """
        Finish the connection and release resources. For simplicity, assume that
        after a socket is closed, neither futher sends nor receives are allowed.
        """
        #############################################################################
        # TODO: YOUR CODE HERE                                                      #
        #############################################################################

        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################
        super().close()

    def set_send_to(self, send_to):
        self._send_to = send_to

    def set_recv_from(self, recv_from):
        self._recv_from = recv_from

    def bind(self, address) -> None:
        super().bind(address)

    def _recvfrom(self, bufsize) -> (bytes, tuple):
        """
        Provide to DataCenter (socket which bind listen port) to call, receive and classify segment
        """
        return super().recvfrom(bufsize)


"""
You can define additional functions and classes to do thing such as packing/unpacking packets, or threading.

"""


class RDTSegment:
    """
    Reliable Data Transfer Segment

    Segment Format:

      0   1   2   3   4   5   6   7   8   9   a   b   c   d   e   f
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                          Source port #                        |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                            Dest port #                        |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                        Sequence number                        |
    |                                                               |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                     Acknowledgment number                     |
    |                                                               |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |ACK|RST|SYN|FIN| Unused flags  |         Unused                |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                         Receive window                        |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                           Checksum                            |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                                                               |
    /                            Payload                            /
    /                                                               /
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+

    Flags:
     - SYN                      Synchronize
     - FIN                      Finish
     - ACK                      Acknowledge

    Ranges:(bit)
     - head length              128
     - Sequence Number          0 - 4294967296
     - Acknowledgement Number   0 - 4294967296

    Checksum Algorithm:         16 bit one's complement of the one's complement sum

    Size of sender's window     16

    # struct 格式字符 https://docs.python.org/zh-cn/3.10/library/struct.html?highlight=struct#struct-format-strings
    """

    _head_len_byte = 18
    _seq_num_bound = 2 ** (4 * 8)
    _win_size_bound = 2 ** 12
    _default_win_size = 16

    def __init__(self, src_port: int, dest_port: int, seq_num: int, ack_num: int, recv_win: int, payload: bytes = None,
                 ack: bool = False, rst: bool = False, syn: bool = False, fin: bool = False):
        self.src_port = src_port
        self.dest_port = dest_port
        self.seq_num = seq_num % self._seq_num_bound
        self.ack_num = ack_num % self._seq_num_bound

        self.ack = ack
        self.rst = rst
        self.syn = syn
        self.fin = fin
        # self.flags = flags & 0xFF

        self.recv_win = recv_win
        self.payload = payload

    def _set_flag(self, flags: int):
        self.ack = (flags & 0x80) != 0
        self.rst = (flags & 0x40) != 0
        self.syn = (flags & 0x20) != 0
        self.fin = (flags & 0x10) != 0

    def _get_flag(self) -> int:
        flags = 0
        if self.ack:
            flags |= 0x80
        if self.rst:
            flags |= 0x40
        if self.syn:
            flags |= 0x20
        if self.fin:
            flags |= 0x10
        return flags & 0xFF

    def encode(self) -> bytes:
        """Encode RDTSegment object to segment (bytes)"""
        flags = self._get_flag()
        # src_port   dest_port  seq_num  ack_num  flags  unused  recv_win  checksum
        head = struct.pack('!HHLLBBHH', self.src_port, self.dest_port, self.seq_num, self.ack_num, flags, 0,
                           self.recv_win, 0)
        segment = bytearray(head)
        if self.payload:
            segment.extend(self.payload)
        checksum = RDTSegment.calc_checksum(segment)
        segment[16] = checksum >> 8
        segment[17] = checksum & 0xFF

        return bytes(segment)

    @staticmethod
    def parse(segment: bytes):
        """Parse raw segment into an RDTSegment object"""
        try:
            assert RDTSegment.calc_checksum(segment) == 0
            head = segment[0:RDTSegment._head_len_byte]

            payload = segment[18:]
            src_port, dest_port, seq_num, ack_num = struct.unpack('!HHLL', head[0:12])
            flags, unused, recv_win, checksum = struct.unpack('!BBHH', head[12:18])
            rdt_seg = RDTSegment(src_port, dest_port, seq_num, ack_num, recv_win, payload)
            rdt_seg._set_flag(flags)
            return rdt_seg
        except AssertionError as e:
            raise ValueError from e

    @staticmethod
    def calc_checksum(segment: bytes) -> int:
        """
        :param segment: raw bytes of a segment, with its checksum set to 0
        :return: 16-bit unsigned checksum
        """
        it = iter(segment)
        bytes_sum = sum(((a << 8) + b for a, b in zip(it, it)))  # (seg[0], seg[1]) (seg[2], seg[3]) ...
        # pad the data with zero to a multiple of length 16
        if len(segment) % 2 == 1:
            bytes_sum += segment[-1] << 8  # (seg[-1], 0)
        # wraparound
        while bytes_sum > 2 ** 16:
            bytes_sum = (bytes_sum & 0xFFFF) + (bytes_sum >> 16)
        return ~bytes_sum & 0xFFFF


class DataCenter(threading.Thread):
    def __init__(self, data_entrance: RDTSocket):
        threading.Thread.__init__(self)
        self.data_entrance = data_entrance

        self.__flag = threading.Event()  # The identity used to pause the thread
        self.__flag.set()  # Initialization does not block threads
        self.__running = threading.Event()  # The identity used to stop the thread
        self.__running.set()  # Initialization thread running

        self.segment_buff_table = {}

    def start(self) -> None:
        super().start()

    def getName(self) -> str:
        return super().getName()

    def setName(self, name: Text) -> None:
        super().setName(name)

    def run(self):
        # listen_addr = self.data_entrance.getsockname()

        while self.__running.isSet():
            self.__flag.wait()  # 为True时立即返回, 为False时阻塞直到self.__flag为True后返回
            data, addr = self.data_entrance._recvfrom(2048)
            if data:
                if addr in self.segment_buff_table:
                    self.segment_buff_table[addr].put(data)
                elif self.data_entrance.allow_accept:
                    self.data_entrance.segment_buff.put(data)

    def pause(self) -> None:
        """
        Block thread, pause receiving message
        :return: None
        """
        self.__flag.clear()

    def resume(self) -> None:
        """
        Stop blocking thread, continue to receive message
        :return: None
        """
        self.__flag.set()

    def stop(self) -> None:
        """
        Stop thread
        :return:
        """
        self.__flag.set()
        self.__running.clear()

    def add_buff(self, key: tuple, value: queue.Queue) -> bool:
        """
        Add buff to buff table
        :param key: address, (ip,port), type tuple (str, int)
        :param value: buff queue
        :return:True if not exist this buff and it added to buff table, else False
        """
        if key in self.segment_buff_table:
            self.segment_buff_table[key] = value
            return True
        return False

    def rvm_buf(self, key: tuple) -> bool:
        """
        Remove buff from buff table
        :param key: address, (ip,port), type tuple (str, int)
        :return: True if exist this buff and it deleted from buff table, else False
        """
        if key in self.segment_buff_table:
            del self.segment_buff_table[key]
            return True
        return False
