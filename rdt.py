from typing import Union, Text, Optional, Callable, Any, Iterable, Mapping

from USocket import UnreliableSocket

import struct, queue, threading, time, random

from queue import PriorityQueue
#############################################################################
# TODO:
# 1. Determine the buff size of each socket, that is, the size of the queue
#############################################################################

DEFAULT_BUFF_SIZE = 2048


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

        self.is_master_server = False
        self.master_server_addr = ('127.0.0.1', 0)
        self.conn_cnt = -1
        self.max_conn = 10
        self.data_center = None
        self.is_server = False
        self.windows = 0  # win size
        self.local_seq_num = 0
        self.local_ack_num = 0

        self.segment_buff = queue.Queue()
        self.data_buff = bytearray()
        self.allow_accept = False
        self.peer_addr = None

        self.busy = False

        # sack variable
        self.main_pq = PriorityQueue()
        self.sub_pq = PriorityQueue()
        self.ack_base = -1
        self.ack_set = set()

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
        if self.conn_cnt == -1:
            self.listen(10)

        if self.conn_cnt > self.max_conn:
            # raise exception
            return None, None

        self.allow_accept = True
        peer_set = set()

        while True:
            seg, addr = self._recv_from(DEFAULT_BUFF_SIZE)
            rdt_seg = RDTSegment.parse(seg)

            if rdt_seg is None:
                continue
            # if time_out
            #     break

            if rdt_seg.syn:
                seq_num = random.randint(0, RDTSegment.SEQ_NUM_BOUND - 1)
                ack_num = rdt_seg.seq_num + 1
                syn_ack_seg = RDTSegment(self.getsockname()[1], rdt_seg.src_port, seq_num, ack_num,
                                         recv_win=8, syn=True)
                self._send_to(syn_ack_seg, addr)
                peer_set.add(addr)
            elif rdt_seg.ack and addr in peer_set:
                self.data_center.add_socket(addr, conn)
                conn.is_server = True
                conn._recv_from = self._get_recvfrom()
                conn.master_server_addr = self.master_server_addr
                conn.data_center = self.data_center
                break

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

        try:  # If there is no binding address, randomly bind one
            self.getsockname()
        except OSError:
            self.bind(("", 0))
        self._send_to = self._get_sendto(self)
        self._recv_from = self._get_recvfrom()

        # send syn pkt
        self.local_seq_num = random.randint(0, RDTSegment.SEQ_NUM_BOUND - 1)
        self.local_ack_num = 0
        syn_seg = RDTSegment(self.getsockname()[1], address[1], self.local_seq_num, self.local_ack_num, recv_win=8,
                             syn=True)
        self._send_to(syn_seg.encode(), address)

        while True:
            seg, addr = self._recv_from(DEFAULT_BUFF_SIZE)
            rdt_seg = RDTSegment.parse(seg)
            if rdt_seg is None:
                continue
            # if syn & ack, send ack
            if address == addr and rdt_seg.syn and rdt_seg.ack and self.local_seq_num + 1 == rdt_seg.ack_num:
                self.local_ack_num = rdt_seg.seq_num + 1
                ack_seg = RDTSegment(self.getsockname()[1], address[1], self.local_seq_num, self.local_ack_num,
                                     recv_win=8, ack=True)
                self._send_to(ack_seg.encode(), address)
                break

        self.peer_addr = address

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
        data = bytes(self.data_buff[:bufsize])
        self.data_buff = self.data_buff[bufsize:]
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
        self._send_to = None
        if self.is_server:
            self.data_center.rvm_socket(self.peer_addr)

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

    def listen(self, max_conn: int = 10):
        if self.conn_cnt == -1:
            self.max_conn = max_conn
            self.conn_cnt = 0
            self.data_center = DataCenter(self)
            self.is_master_server = True
            self._recv_from = self._get_recvfrom()
            self._send_to = self._get_sendto(self)
            try:
                self.master_server_addr = super().getsockname()
            except OSError:
                self.bind(('127.0.0.1', 0))
                self.master_server_addr = super().getsockname()
        else:
            self.max_conn = max_conn

    def getsockname(self):
        try:
            addr = super().getsockname()
            return addr
        except OSError:
            return

    def _get_recvfrom(self):
        """

        """
        if self.is_server:
            def recvfrom_func(bufsize):
                while self.segment_buff.empty():
                    # time.sleep(0.01)
                    pass
                return self.segment_buff.get()

            return recvfrom_func
        else:
            return super().recvfrom

    def _get_sendto(self, sock: 'RDTSocket'):
        if self.is_server:
            return sock._send_to
        else:
            return super().sendto
        pass

    def _recv_segs_to_data_buff(self):
        """
        Receive and process the segment and add the processed data to the buff
        """
        # recv_data = bytearray()
        ack_seg = None
        while True:
            seg, addr = self._recv_from(DEFAULT_BUFF_SIZE)
            rdt_seg = RDTSegment.parse(seg)
            if rdt_seg is None:
                continue

            # ack_num = rdt_seg.seq_num + 1
            # TODO: selective ack
            check_sum = RDTSegment.calc_checksum(rdt_seg.encode())
            check_sum += rdt_seg.check_sum
            if ~check_sum:
                continue

            data_len = len(rdt_seg.payload)
            ack_range_tuple = (rdt_seg.seq_num, (rdt_seg.seq_num + data_len) % rdt_seg.SEQ_NUM_BOUND)
            if self.ack_base < rdt_seg.seq_num:
                if ack_range_tuple not in self.ack_set:
                    self.main_pq.put(AckRange(ack_range_tuple, rdt_seg.payload))
                    self.ack_set.add(ack_range_tuple)
                if ack_seg is None:
                    ack_seg = RDTSegment(rdt_seg.dest_port, rdt_seg.src_port, 0, self.ack_base, ack=True)
            elif self.ack_base == rdt_seg.seq_num or self.ack_base == -1:
                self.ack_base += data_len
                self.data_buff.extend(rdt_seg.payload)
                while True:
                    if self.main_pq.empty():
                        if self.sub_pq.empty():
                            break
                        self.main_pq = self.sub_pq
                        self.sub_pq = PriorityQueue()
                        continue
                    min_ack_range = self.main_pq.get()
                    self.ack_set.remove(min_ack_range)
                    if self.ack_base == min_ack_range.value[0]:
                        self.ack_base = min_ack_range.value[1]
                        self.data_buff.extend(min_ack_range.data)
                        continue
                    else:
                        # construct an option sack
                        ack_seg = RDTSegment(rdt_seg.dest_port, rdt_seg.src_port, 0, self.ack_base, ack=True)
                        ack_seg.options[5] = [(self.ack_base, min_ack_range.value[0])]
                        break
            else:
                self.sub_pq.put(AckRange(ack_range_tuple, rdt_seg.payload))

            self._send_to(ack_seg.encode(), addr)

            break



"""
You can define additional functions and classes to do thing such as packing/unpacking packets, or threading.

"""


class ProcessingSegment(threading.Thread):
    def __init__(self, rdt_socket: RDTSocket):
        threading.Thread.__init__(self)
        self.rdt_socket = rdt_socket

    def run(self):
        self.rdt_socket.busy = True
        self.rdt_socket._recv_segs_to_data_buff()
        self.rdt_socket.busy = False

class AckRange:
    def __init__(self, value: tuple, data: bytes):
        self.value = value
        self.data = data

    def __lt__(self, other: 'AckRange'):
        return self.value[0] < other.value[0]

class DataCenter(threading.Thread):
    def __init__(self, data_entrance: RDTSocket):
        threading.Thread.__init__(self)
        self.data_entrance = data_entrance

        self.__flag = threading.Event()  # The identity used to pause the thread
        self.__flag.set()  # Initialization does not block threads
        self.__running = threading.Event()  # The identity used to stop the thread
        self.__running.set()  # Initialization thread running

        self.socket_table = {}

    def set_data_entrance(self, data_entrance: RDTSocket):
        self.data_entrance = data_entrance

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
            data, addr = self.data_entrance._recv_from(DEFAULT_BUFF_SIZE)
            if data:
                if addr in self.socket_table:
                    sock = self.socket_table[addr]
                    sock.segment_buff.put((data, addr))
                    if not sock.busy:
                        ProcessingSegment(sock).start()
                elif self.data_entrance.allow_accept:
                    self.data_entrance.segment_buff.put(data)

    def pause(self) -> None:
        """
        Block thread, pause receiving segment
        :return: None
        """
        self.__flag.clear()

    def resume(self) -> None:
        """
        Stop blocking thread, continue to receive segment
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

    def add_socket(self, key: tuple, value: RDTSocket) -> bool:
        """
        Add socket to buff table
        :param key: address, (ip,port), type tuple (str, int)
        :param value: socket
        :return:True if not exist this socket and it added to socket table, else False
        """
        if key in self.socket_table:
            self.socket_table[key] = value
            self.data_entrance.conn_cnt += 1
            return True
        return False

    def rvm_socket(self, key: tuple) -> bool:
        """
        Remove socket from buff table
        :param key: address, (ip,port), type tuple (str, int)
        :return: True if exist this socket and it deleted from socket table, else False
        """
        if key in self.socket_table:
            del self.socket_table[key]
            self.data_entrance.conn_cnt -= 1
            return True
        return False


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
    | Header length |ACK|RST|SYN|FIN|         Unused                |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                         Receive window                        |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                           Checksum                            |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                                                               |
    /                            Options                            /
    /                                                               /
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
    SEQ_NUM_BOUND = 2 ** (4 * 8)
    _win_size_bound = 2 ** 12
    _default_win_size = 16

    def __init__(self, src_port: int, dest_port: int, seq_num: int, ack_num: int, recv_win: int = 8,
                 check_sum: int = 0, payload: bytes = None, options: dict = None,
                 ack: bool = False, rst: bool = False, syn: bool = False, fin: bool = False):
        if options is None:
            options = {}
        self.src_port = src_port
        self.dest_port = dest_port
        self.seq_num = seq_num % self.SEQ_NUM_BOUND
        self.ack_num = ack_num % self.SEQ_NUM_BOUND
        self.check_sum = check_sum
        self.options = options

        self.ack = ack
        self.rst = rst
        self.syn = syn
        self.fin = fin
        # self.flags = flags & 0xFF

        self.recv_win = recv_win
        self.payload = payload

    def _decode_flags(self, flags: int) -> None:
        self.ack = (flags & 0x08) != 0
        self.rst = (flags & 0x04) != 0
        self.syn = (flags & 0x02) != 0
        self.fin = (flags & 0x01) != 0
        #   [header length]        unit           size
        #     in segment           Word   (flags & 0xFF) >> 4
        #  in RDTSegment class     Byte   (flags & 0xFF) >> 2
        self.head_len = (flags & 0xFF) >> 2

    def _encode_flags(self) -> int:
        flags = 0
        if self.ack:
            flags |= 0x08
        if self.rst:
            flags |= 0x04
        if self.syn:
            flags |= 0x02
        if self.fin:
            flags |= 0x01
        flags |= ((self.head_len & 0x3c) << 2)
        return flags & 0xFF

    def _decode_options(self, options: bytes) -> None:
        i = 0
        op_len = len(options)
        while i < op_len:
            kind = int(options[i])
            if kind == 5:
                length = int(options[i + 1])
                edges_cnt = (length - 2) // 4
                # edges: (leftEdge1, rightEdge1, leftEdge2, rightEdge2 ...)
                edges = struct.unpack('!' + 'L' * edges_cnt, options[i + 2:i + length])
                # sack_edge: ((leftEdge1, rightEdge1), (leftEdge2, rightEdge2) ...)
                it = iter(edges)
                sack_edge = tuple(b for b in zip(it, it))
                self.options[kind] = sack_edge
                i += length
            elif kind == 0:
                break
            elif kind == 1:
                i += 1
            else:
                break

    def _encode_options(self) -> bytes:
        options_byte = bytearray()
        if 5 in self.options:
            sack_edge = self.options.get(5)
            sack_cnt = len(sack_edge)
            options_byte.extend(struct.pack('!BBBB', 1, 1, 5, sack_cnt + 2))
            for i, j in sack_edge:
                options_byte.extend(struct.pack('!LL', i, j))
        return bytes(options_byte)

    def encode(self) -> bytes:
        """Encode RDTSegment object to segment (bytes)"""
        flags = self._encode_flags()
        # B 1 H 2 L 4
        # src_port   dest_port  seq_num  ack_num  flags  unused  recv_win  checksum
        head = struct.pack('!HHLLBBHH', self.src_port, self.dest_port, self.seq_num, self.ack_num, flags, 0,
                           self.recv_win, 0)
        segment = bytearray(head)

        if self.options:
            segment.extend(self._encode_options())

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
            if RDTSegment.calc_checksum(segment) != 0:
                return None
            head = segment[0:18]

            src_port, dest_port, seq_num, ack_num = struct.unpack('!HHLL', head[0:12])
            flags, unused, recv_win, checksum = struct.unpack('!BBHH', head[12:18])
            head_length = (flags & 0xFF) >> 2
            payload = segment[head_length:]
            rdt_seg = RDTSegment(src_port, dest_port, seq_num, ack_num, recv_win, checksum, payload)
            rdt_seg._decode_flags(flags)
            if rdt_seg.head_len > 18:
                rdt_seg._decode_options(segment[18:head_length])
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
