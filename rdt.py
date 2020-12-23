from typing import Text

from USocket import UnreliableSocket

import struct, queue, threading, time, random, logging

from queue import PriorityQueue

#############################################################################
# TODO:
# 1. timeout reset
# 2. See you
# 3.
#############################################################################

LOG_FORMAT = "[%(name)s %(levelname)s] %(filename)s[func: %(filename)s  line:%(lineno)d] %(asctime)s: %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
client_logger = logging.getLogger("CLIENT")
server_logger = logging.getLogger("SERVER")

DEFAULT_BUFF_SIZE = 2048
DEFAULT_WIN_SIZE = 65535
CONSTANT2P32 = 4294967296  # 2^32

alpha = 0.125
beta = 0.25


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

        # master server
        self.is_master_server = False
        self.allow_accept = False
        self.data_center = None
        self.super_recvfrom = super().recvfrom  # Provided to data_center for use
        self.conn_cnt = -1
        self.max_conn = 10

        # server
        self.is_server = False
        self.segment_buff = queue.Queue()
        self.data_buff = bytearray()

        # Retransmission mechanism
        self.recv_win_size = DEFAULT_WIN_SIZE  # recv window size
        self.local_seq_num = 0  # the position of next byte send in send window
        self.local_ack_num = 0  # the position of the next byte of the expected ack in recv window
        self.mss = 1460
        self.rtt_orign = 0.005

        # socket
        self.busy = False
        self.local_addr = ('0.0.0.0', 0)
        self.peer_addr = ('255.255.255.255', 0)
        self.ack_num_option_buff = queue.Queue()
        self.seq_num_payload_buff = queue.Queue()
        self.recv_thread = ProcessingSegment(self)
        self.local_closing = False
        self.local_closed = False
        self.peer_closing = False
        self.peer_closed = False

        # sack variable
        self.main_pq = PriorityQueue()
        self.sub_pq = PriorityQueue()
        self.ack_set = set()
        self.sub_ack_set = set()

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
            self.listen(30)

        if self.conn_cnt > self.max_conn:
            # raise exception
            server_logger.info(f"The number of server [{self.local_addr}] connections has reached the maximum")
            return None, None

        self.allow_accept = True
        peer_info = {}

        server_logger.info(f"Server [{self.local_addr}] is waiting for the client to connect")
        while True:
            rdt_seg, addr = self._recv_from()

            if rdt_seg is None:
                continue

            if rdt_seg.syn:
                seq_num = 4294950000  # TODO: random.randint(0, RDTSegment.SEQ_NUM_BOUND - 1)
                ack_num = rdt_seg.seq_num + 1
                syn_ack_seg = RDTSegment(rdt_seg.dest_port, rdt_seg.src_port, seq_num, ack_num,
                                         ack=True, syn=True)
                self._send_to(syn_ack_seg, addr)
                peer_info[addr] = (seq_num + 1)
                server_logger.info(f"Connection establishment request received from client [{addr}]")
                server_logger.info(f"Client request connection pool: {peer_info}")
            elif addr in peer_info and rdt_seg.ack_num == peer_info[addr]:
                # master server
                conn.data_center = self.data_center
                # server
                conn.is_server = True
                # Retransmission mechanism
                conn.recv_win_size = DEFAULT_WIN_SIZE
                conn.local_seq_num = rdt_seg.ack_num
                conn.local_ack_num = rdt_seg.seq_num
                # socket
                conn.local_addr = self.local_addr
                conn.peer_addr = addr
                conn._recv_from = conn._server_recvfrom
                conn._send_to = conn._server_sendto

                if len(rdt_seg.payload) != 0:
                    self.segment_buff.put((rdt_seg.encode(), addr), block=False)
                server_logger.info(
                    f"Server [{self.local_addr}] accepts the connection from client [{addr}], start processing segment")
                if len(rdt_seg.payload) != 0:
                    self.seq_num_payload_buff.put((rdt_seg.seq_num, rdt_seg.payload), block=False)
                self.data_center.add_sock(addr, conn)
                conn.recv_thread.start()
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
            self.local_addr = super(RDTSocket, self).getsockname()
        except OSError:
            self.bind(("127.0.0.1", 0))
            self.local_addr = super(RDTSocket, self).getsockname()
            client_logger.info(f"Unbinding address, randomly bind one, at {self.local_addr}")
        self._send_to = self._client_sendto
        self._recv_from = self._client_recvfrom
        self.recv_win_size = DEFAULT_WIN_SIZE
        self.peer_addr = address

        client_logger.info(f"client [{self.local_addr}] initiate connection request to server [{address}].")

        # send syn pkt
        self.local_seq_num = 4294950000  # TODO: random.randint(0, RDTSegment.SEQ_NUM_BOUND - 1)
        self.local_ack_num = 0
        syn_seg = RDTSegment(self.local_addr[1], address[1], self.local_seq_num, self.local_ack_num, syn=True)
        self._send_to(syn_seg, address)

        start_time = time.time()
        try_conn_cnt = 1
        while True:
            rdt_seg, addr = self._recv_from(timeout=0.1)
            if rdt_seg:
                try_conn_cnt = 1
            else:
                if try_conn_cnt > 20:
                    client_logger.info("Request no response, exit connection request.")
                    return
                self._send_to(syn_seg, address)
                try_conn_cnt += 1
                client_logger.info("request timeout, try again.")
                continue

            if address == addr and rdt_seg.syn and rdt_seg.ack and self.local_seq_num + 1 == rdt_seg.ack_num:
                self.rtt_orign = time.time() - start_time
                self.local_ack_num = rdt_seg.seq_num + 1
                self.local_seq_num = rdt_seg.ack_num
                ack_seg = RDTSegment(self.local_addr[1], rdt_seg.src_port, self.local_seq_num, self.local_ack_num,
                                     ack=True)
                self._send_to(ack_seg, address)
                break

        client_logger.info(f"Client [{self.local_addr}] is connected to server [{address}], start processing segment.")
        self.recv_thread.start()
        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################

    def recv(self, bufsize: int) -> bytes:
        """
        Receive data from the socket.
        The return value is a bytes object representing the data received.
        The maximum amount of data to be received at once is specified by buff size.

        Note that ONLY data send by the peer should be accepted.
        In other words, if someone else sends data to you from another address,
        it MUST NOT affect the data returned by this function.
        """
        assert self._recv_from, "Connection not established yet. Use recvfrom instead."
        #############################################################################
        # TODO: YOUR CODE HERE                                                      #
        #############################################################################
        timeout_sec = self.gettimeout()
        if timeout_sec is None:
            while len(self.data_buff) == 0:
                pass
        else:
            tout = time.time() + timeout_sec
            while len(self.data_buff) == 0 and time.time() < tout:
                pass
            if time.time() > tout:
                # raise
                return bytes()
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
        # 0--slow start;
        # 1--congestion avoidance
        # 2--fast recovery
        congestionCtrl_state = 0
        threash = 1 << 32

        # first sequence num
        seq_base = self.local_seq_num
        # location at data
        send_base = 0
        # location at data
        next_seq = 0
        send_window_len = self.mss * 10

        # parameter for timeout_judge
        time_out = self.rtt_orign
        sample_rtt = 0
        estimate_rtt = self.rtt_orign
        dev_rtt = 0
        #
        fr_cnt = 0
        unACK_range = [0, 0]
        seq_size = 1 << 32

        def congestion_control_newAck(ack_size):
            nonlocal congestionCtrl_state, send_window_len, threash
            if congestionCtrl_state == 0:
                send_window_len += ack_size
                if send_window_len >= threash:
                    congestionCtrl_state = 1
                    send_window_len = threash
            if congestionCtrl_state == 1:
                send_window_len += int((ack_size / send_window_len) * self.mss)
            if congestionCtrl_state == 2:
                threash = send_window_len / 2
                send_window_len = threash + 3 * self.mss
                congestionCtrl_state = 1

        def timeout_rst(sample_new):
            nonlocal sample_rtt, estimate_rtt, dev_rtt, time_out
            sample_rtt = sample_new
            estimate_rtt = (1 - alpha) * estimate_rtt + alpha * sample_rtt
            dev_rtt = (1 - beta) * dev_rtt + beta * abs(estimate_rtt - sample_rtt)
            time_out = estimate_rtt + 4 * dev_rtt

        while True:
            # divide segments in send_window and send them
            timer_start = time.time()
            while True:
                if send_base + send_window_len - next_seq >= self.mss and next_seq + self.mss <= len(data):
                    self.seq_num_payload_buff.put(
                        ((seq_base + next_seq) % seq_size, data[next_seq: next_seq + self.mss]), block=False)
                    print(f'Please send segment, seq_num={seq_base + next_seq}')
                    next_seq += self.mss
                elif send_base + send_window_len > len(data) > next_seq and next_seq + self.mss > len(data):
                    self.seq_num_payload_buff.put(((seq_base + next_seq) % seq_size, data[next_seq:]), block=False)
                    print(
                        f'Please send segment, seq_num={seq_base + next_seq}, it is the last one, len={len(data) - next_seq}')
                    next_seq = len(data)
                else:
                    break
            # receive ack
            while True:
                send_base_seq = (seq_base + send_base) % seq_size
                if not self.ack_num_option_buff.empty():
                    ack_tuple = self.ack_num_option_buff.get(block=False)
                    print(ack_tuple)
                    ack_num = ack_tuple[0]
                    options = ack_tuple[1]
                    if ack_num == (seq_base + len(data)) % RDTSegment.SEQ_NUM_BOUND or send_base == len(data):
                        self.local_seq_num = (self.local_seq_num + len(data)) % RDTSegment.SEQ_NUM_BOUND
                        return
                    # move window
                    if ack_num > send_base_seq:
                        print(
                            f'move window!, send_base_from={send_base} send_base_to={send_base + (ack_num - send_base_seq)}')
                        send_base += (ack_num - send_base_seq)
                        if ack_num - send_base_seq <= self.mss:
                            timeout_rst(time.time() - timer_start)
                        timer_start = time.time()
                        congestion_control_newAck(ack_num - send_base_seq)
                        fr_cnt = 0
                        break
                    elif ack_num < send_base_seq and send_base_seq + send_window_len >= seq_size + ack_num:
                        print(
                            f'move window rollback!, send_base_from={send_base} send_base_to={send_base + (seq_size + ack_num - send_base_seq)}')
                        send_base += (seq_size + ack_num - send_base_seq)
                        if seq_size + ack_num - send_base_seq <= self.mss:
                            timeout_rst(time.time() - timer_start)
                        congestion_control_newAck(seq_size + ack_num - send_base_seq)
                        fr_cnt = 0
                        break
                    # acculate for fast retransmission
                    elif 5 in options:

                        if fr_cnt < 3:
                            fr_cnt += 1
                        else:
                            for unACK_range in options[5]:
                                fr_base = send_base + unACK_range[0] - send_base_seq
                                fr_len = unACK_range[1] - unACK_range[0]
                                if fr_len < 0:
                                    fr_len += RDTSegment.SEQ_NUM_BOUND

                                fr_next = fr_base
                                while True:
                                    print(
                                        f'retransimission, seq_num={seq_base + fr_next} fr_base={fr_base} fr_len={fr_len} fr_next={fr_next} unack={unACK_range}')
                                    if fr_base + fr_len - fr_next >= self.mss:
                                        self.seq_num_payload_buff.put(
                                            ((seq_base + fr_next) % seq_size, data[fr_next: fr_next + self.mss]),
                                            block=False)
                                        fr_next += self.mss
                                    elif fr_base + fr_len - len(data) == 0:
                                        self.seq_num_payload_buff.put(((seq_base + fr_next) % seq_size, data[fr_next:]),
                                                                      block=False)
                                        fr_next = len(data)
                                    else:
                                        break
                            fr_cnt = 0
                            congestionCtrl_state = 2
                            congestion_control_newAck(self.mss)
                            timer_start = time.time()

                    # without operation
                    else:
                        continue
                # judge time out
                if time.time() - timer_start > time_out:
                    print(f'timeout, time_from_start:{time.time() - timer_start}, time_out:{time_out}')
                    buff = ((seq_base + send_base) % seq_size, data[send_base: min(send_base + self.mss, len(data))])
                    if buff not in self.seq_num_payload_buff.queue:
                        self.seq_num_payload_buff.put(buff, block=False)
                    timer_start = time.time()
                    # send_window_len = 0
                    congestion_control_newAck(self.mss)

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
        if self.local_closed or self._send_to is None:
            return

        self.local_closing = True
        if self.is_master_server:
            pass
        elif self.is_server:

            self.data_center.rvm_sock(self.peer_addr)

            master_sock = self.data_center.data_entrance

            if master_sock.local_closed and master_sock.conn_cnt == 0:
                self.data_center.stop()
        else:
            client_logger.info(
                f"client [{self.local_addr[0]}] attempts to close the connection with server [{self.peer_addr}]")

            fin_seg = RDTSegment(self.local_addr[1], self.peer_addr[1], self.local_seq_num, self.local_ack_num,
                                 fin=True)
            ack_seg = RDTSegment(self.local_addr[1], self.peer_addr[1], self.local_seq_num,
                                 0, ack=True)
            self.settimeout(1)
            while True:
                if not self.peer_closing and not self.peer_closed:
                    self._send_to(fin_seg, self.peer_addr)
                elif self.peer_closed:
                    try:
                        peer_seg, addr = self._recv_from()
                        if peer_seg.fin:
                            ack_seg = RDTSegment(self.local_addr[1], self.peer_addr[1], self.local_seq_num,
                                                 peer_seg.seq_num + 1, ack=True)
                            self._send_to(ack_seg, self.peer_addr)
                        else:
                            break
                    except OSError:
                        break
                    continue
                else:
                    self._send_to(ack_seg, self.peer_addr)

                try:
                    peer_seg, addr = self._recv_from()
                except OSError:
                    continue

                if peer_seg.ack and peer_seg.ack_num == self.local_seq_num + 1:
                    self.peer_closing = True
                elif peer_seg.fin:
                    ack_seg = RDTSegment(self.local_addr[1], self.peer_addr[1], self.local_seq_num,
                                         peer_seg.seq_num + 1, ack=True)
                    self._send_to(ack_seg, self.peer_addr)
                    self.peer_closed = True
                    # break

            self._send_to = None
            self.local_closed = True
            client_logger.info(
                f"client [{self.local_addr}] successfully closed the connection with server [{self.peer_addr}]")

        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################
        super().close()

    def bind(self, address) -> None:
        super(RDTSocket, self).bind(address)

    def listen(self, max_conn: int = 10):
        if self.conn_cnt == -1:
            self.max_conn = max_conn
            self.conn_cnt = 0
            self.is_master_server = True
            self._recv_from = self._server_recvfrom
            self._send_to = self._server_sendto
            try:
                self.local_addr = super(RDTSocket, self).getsockname()
            except OSError:
                self.bind(('127.0.0.1', 0))
                self.local_addr = super(RDTSocket, self).getsockname()
            self.data_center = DataCenter(self)
            self.data_center.start()
        else:
            self.max_conn = max_conn

    def getsockname(self) -> (str, int):
        return self.local_addr

    def _server_recvfrom(self, timeout: float = None) -> ('RDTSegment', tuple):
        """
        Get RDTSegment from segment buffer, Provided to the server or master server for use
        :param timeout: unit seconds
        :return: None if timeout, else not corrupt RDTSegment
        """
        if timeout is None:
            timeout = 1000
        tout = time.time() + timeout
        while time.time() < tout:
            if self.segment_buff.qsize():
                seg, addr = self.segment_buff.get(block=False)
                server_logger.info(
                    f'Take out segment from  ({self.local_addr[0]}, {self.local_addr[1]}, {addr[0]}, {addr[1]})')
                rdt_seg = RDTSegment.parse(seg)
                if rdt_seg:
                    server_logger.info(
                        f"Received: {self.peer_addr[0]} -> {self.local_addr[0]}  {rdt_seg.log_info()}")
                    return rdt_seg, addr
                server_logger.info(f"Server [{self.local_addr}] Received corrupted segment")
        return None, None

    def _client_recvfrom(self, timeout: float = None) -> ('RDTSegment', tuple):
        """
        Get RDTSegment from segment buffer, Provided to the client for use
        :param timeout: unit seconds
        :return: None if timeout, else not corrupt RDTSegment
        """
        timeout_sec = super().gettimeout()
        super().settimeout(timeout)
        try:
            while True:
                data, frm = super().recvfrom(DEFAULT_BUFF_SIZE)
                rdt_seg = RDTSegment.parse(data)
                if rdt_seg:
                    client_logger.info(
                        f"Received: {self.peer_addr[0]} -> {self.local_addr[0]}  {rdt_seg.log_info()}")
                    super().settimeout(timeout_sec)
                    return rdt_seg, frm
                client_logger.info(f"Client [{self.local_addr}] Received corrupted segment")
        except OSError:
            return None, None

    def _server_sendto(self, rdt_seg: 'RDTSegment', addr):
        """
        Send RDTSegment, Provided to the server or master server for use
        :param rdt_seg: RDTSegment needed to send
        :param addr: Destination address
        """
        server_logger.info(
            f"Sent    : {self.local_addr[0]} -> {self.peer_addr[0]}  {rdt_seg.log_info()}")
        # super(RDTSocket, self).sendto(rdt_seg.encode(), addr)
        self.data_center.data_entrance.sendto(rdt_seg.encode(), addr)

    def _client_sendto(self, rdt_seg: 'RDTSegment', addr):
        """
        Send RDTSegment, Provided to the client for use
        :param rdt_seg: RDTSegment needed to send
        :param addr: Destination address
        """
        client_logger.info(
            f"Sent    : {self.local_addr[0]} -> {self.peer_addr[0]}  {rdt_seg.log_info()}")
        # super(RDTSocket, self).sendto(rdt_seg.encode(), addr)
        self.sendto(rdt_seg.encode(), addr)

    def _recv_segs_to_data_buff(self):
        """
        Receive and process the segment and add the processed data to the buff
        """
        tmp_counter = 0
        need_option = False
        ack_seg = RDTSegment(self.local_addr[1], self.peer_addr[1], 0, self.local_ack_num)
        while True:
            rdt_seg, addr = self._recv_from(0.1)
            if rdt_seg is None:
                if not self.seq_num_payload_buff.empty():
                    # need send data which come from func send()
                    seg_cnt = self.seq_num_payload_buff.qsize()
                    print(f'No segment can be received, but need to send segment, send segments count: {seg_cnt}')
                    for i in range(seg_cnt):
                        seq_payload = self.seq_num_payload_buff.get(block=False)
                        seq_seg = RDTSegment(self.local_addr[1], self.peer_addr[1], seq_payload[0], self.local_ack_num,
                                             payload=seq_payload[1])
                        self._send_to(seq_seg, self.peer_addr)
                else:
                    if tmp_counter < 5:
                        tmp_counter += 1
                    else:
                        print('No segment can be received, also not need to send segment')
                        tmp_counter = 0
                continue

            if rdt_seg.ack:
                if rdt_seg.options:
                    print(f'sender need retransmission op={rdt_seg.options}')
                self.ack_num_option_buff.put((rdt_seg.ack_num, rdt_seg.options), block=False)

            data_len = len(rdt_seg.payload)
            if data_len != 0:
                # seq num is valid
                ack_seg.ack = True

                ack_range_tuple = (rdt_seg.seq_num, (rdt_seg.seq_num + data_len) % rdt_seg.SEQ_NUM_BOUND)
                if self.local_ack_num < rdt_seg.seq_num < self.local_ack_num + self.recv_win_size:
                    # seg_num inside recv window
                    print(f'recv seg inside recv-win, seq_num={rdt_seg.seq_num}, payload len={len(rdt_seg.payload)}')
                    if ack_range_tuple not in self.ack_set:
                        self.main_pq.put(AckRange(ack_range_tuple, rdt_seg.payload))
                        self.ack_set.add(ack_range_tuple)

                    need_option = True
                elif self.local_ack_num == rdt_seg.seq_num:
                    # seq_num equal recv window base
                    print(
                        f'recv seg, is the expected seq_num, seq_num={rdt_seg.seq_num}, payload len={len(rdt_seg.payload)}, next expected={rdt_seg.seq_num + len(rdt_seg.payload)}')
                    self.local_ack_num = (self.local_ack_num + data_len) % RDTSegment.SEQ_NUM_BOUND
                    self.data_buff.extend(rdt_seg.payload)
                    ack_seg.ack_num = self.local_ack_num
                    while True:
                        # Cumulative confirmation
                        if self.main_pq.empty():
                            if self.sub_pq.empty():
                                break
                            print('window rollback successfully')
                            self.main_pq.queue = self.sub_pq.queue
                            self.sub_pq.queue = []
                            self.ack_set = self.sub_ack_set
                            self.sub_ack_set = set()
                            continue
                        min_ack_range = self.main_pq.queue[0]
                        if self.local_ack_num == min_ack_range.value[0]:
                            print(f'Cumulative confirmation, local_ack_num={self.local_ack_num}')
                            self.main_pq.get()
                            self.ack_set.remove(min_ack_range.value)
                            self.local_ack_num = min_ack_range.value[1]
                            self.data_buff.extend(min_ack_range.data)
                            continue
                        else:
                            # construct an option sack
                            need_option = True
                            break
                elif self.local_ack_num - self.recv_win_size < rdt_seg.seq_num < self.local_ack_num:
                    # seq_num in left of window within one win_size
                    print('recv seg, seq_num in left of window within one win_size')
                    ack_seg.ack_num = self.local_ack_num
                elif rdt_seg.seq_num < self.local_ack_num + self.recv_win_size - RDTSegment.SEQ_NUM_BOUND:
                    # window rollback
                    print('recv seg, window rollback')
                    if not ack_range_tuple in self.sub_ack_set:
                        self.sub_pq.put(AckRange(ack_range_tuple, rdt_seg.payload))
                        self.sub_ack_set.add(ack_range_tuple)
                else:
                    print('recv seg, Alien segment')
                    ack_seg.ack = False
                    ack_seg.options = {}

                if need_option:
                    range_back_list = []
                    cnt = 1
                    new_op_5 = [(self.local_ack_num, self.main_pq.queue[0].value[0])]

                    while cnt < 5 and self.main_pq.qsize() > 1:
                        tmp = self.main_pq.get()
                        tmp_range = tmp.value
                        new_range = list(tmp_range)
                        new_data = bytearray(tmp.data)

                        while self.main_pq.qsize() > 0:
                            top_range = self.main_pq.queue[0].value
                            if top_range[0] == tmp_range[1]:
                                new_range[1] = top_range[1]
                                tmp = self.main_pq.get()
                                new_data.extend(tmp.data)
                            else:
                                break
                        range_back_list.append(AckRange(tuple(new_range), bytes(new_data)))

                        if self.main_pq.qsize():
                            new_op_5.append((new_range[1], self.main_pq.queue[0].value[0]))
                            cnt += 1

                    for i in range_back_list:
                        self.main_pq.put(i)
                        self.ack_set.add(i.value)

                    ack_seg.ack_num = self.local_ack_num
                    ack_seg.options[5] = new_op_5
                    need_option = False

                    print(
                        f'receiver call sender need retransmission, local_ack_num={self.local_ack_num} op={ack_seg.options}')
                else:
                    ack_seg.options = {}
            else:
                ack_seg.ack = False
                ack_seg.options = {}

            if rdt_seg.fin:
                pass
            if not self.seq_num_payload_buff.empty():
                # need send data which come from func send()
                seq_payload = self.seq_num_payload_buff.get(block=False)
                ack_seg.seq_num = seq_payload[0]
                ack_seg.payload = seq_payload[1]
            else:
                ack_seg.seq_num = self.local_seq_num
                ack_seg.payload = b''

            if ack_seg.ack or len(ack_seg.payload) != 0:
                self._send_to(ack_seg, addr)


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
        server_logger.info("DataCenter start work...")
        while self.__running.isSet():
            self.__flag.wait()  # 为True时立即返回, 为False时阻塞直到self.__flag为True后返回
            data, addr = self.data_entrance.super_recvfrom(DEFAULT_BUFF_SIZE)
            if data:
                if addr in self.socket_table:
                    sock = self.socket_table.get(addr)
                    sock.segment_buff.put((data, addr), block=False)
                    server_logger.info(
                        f'Data distribution to ({sock.local_addr[0]}, {sock.local_addr[1]}, {addr[0]}, {addr[1]})')
                elif self.data_entrance.allow_accept:
                    self.data_entrance.segment_buff.put((data, addr), block=False)

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

    def add_sock(self, key: tuple, value: RDTSocket) -> bool:
        """
        Add socket to buff table
        :param key: address, (ip,port), type tuple (str, int)
        :param value: socket
        :return:True if not exist this socket and it added to socket table, else False
        """
        if key not in self.socket_table:
            self.socket_table[key] = value
            self.data_entrance.conn_cnt += 1
            server_logger.info(f"new socket add to data center, accept data from {value.peer_addr}")
            return True
        return False

    def rvm_sock(self, key: tuple) -> bool:
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
    SEQ_NUM_BOUND = 4294967296  # 2 ** (4 * 8)
    WIN_SIZE_BOUND = 65535  # 2 ** 16
    DEFAULT_WIN_SIZE = 65535

    def __init__(self, src_port: int, dest_port: int, seq_num: int, ack_num: int, recv_win: int = 65535,
                 check_sum: int = 0, payload: bytes = b'', options: dict = None,
                 ack: bool = False, rst: bool = False, syn: bool = False, fin: bool = False):
        if options is None:
            options = {}
        self.src_port = src_port
        self.dest_port = dest_port
        self.seq_num = seq_num % self.SEQ_NUM_BOUND
        self.ack_num = ack_num % self.SEQ_NUM_BOUND
        self.check_sum = check_sum
        self.options = options
        self.header_len = 0

        self.ack = ack
        self.rst = rst
        self.syn = syn
        self.fin = fin
        # self.flags = flags & 0xFF

        self.recv_win = recv_win
        self.payload = payload

    def __str__(self) -> str:
        return f'RDTSegment[' \
               f'src_port: {self.src_port}  dest_port: {self.dest_port}  seq_num: {self.seq_num}  ack_num: {self.ack_num}  ' \
               f'recv_win: {self.recv_win}  check_sum: {self.check_sum}  ' \
               f'ack: {self.ack}  rst: {self.rst}  syn: {self.syn}  fin: {self.fin} payload len = {len(self.payload)}]'

    def _decode_flags(self, flags: int) -> None:
        self.ack = (flags & 0x08) != 0
        self.rst = (flags & 0x04) != 0
        self.syn = (flags & 0x02) != 0
        self.fin = (flags & 0x01) != 0
        #   [header length]        unit           size
        #     in segment           Word   (flags & 0xFF) >> 4
        #  in RDTSegment class     Byte   (flags & 0xFF) >> 2
        self.header_len = (flags & 0xF0) >> 2

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
        flags |= ((self.header_len & 0x3c) << 2)
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
            options_byte.extend(struct.pack('!BBBB', 1, 1, 5, ((sack_cnt << 3) + 2)))
            for i, j in sack_edge:
                options_byte.extend(struct.pack('!LL', i, j))
        return bytes(options_byte)

    def encode(self) -> bytes:
        """Encode RDTSegment object to segment (bytes)"""
        options = self._encode_options()
        self.header_len = (len(options) + 16)
        flags = self._encode_flags()
        # B 1 H 2 L 4
        # src_port   dest_port  seq_num  ack_num  flags  unused   checksum
        head = struct.pack('!HHLLBBH', self.src_port, self.dest_port, self.seq_num, self.ack_num, flags, 0, 0)
        segment = bytearray(head)

        if self.options:
            segment.extend(options)

        segment.extend(self.payload)
        checksum = RDTSegment.calc_checksum(segment)
        segment[14] = checksum >> 8
        segment[15] = checksum & 0xFF
        return bytes(segment)

    @staticmethod
    def parse(segment: bytes):
        """Parse raw segment into an RDTSegment object"""
        if RDTSegment.calc_checksum(segment) != 0:
            return None
        head = segment[0:16]

        src_port, dest_port, seq_num, ack_num = struct.unpack('!HHLL', head[0:12])
        flags, unused, checksum = struct.unpack('!BBH', head[12:16])
        head_length = (flags & 0xF0) >> 2
        payload = segment[head_length:]
        rdt_seg = RDTSegment(src_port, dest_port, seq_num, ack_num, DEFAULT_WIN_SIZE, checksum, payload)
        rdt_seg._decode_flags(flags)
        if rdt_seg.header_len > 16:
            rdt_seg._decode_options(segment[16:head_length])
        return rdt_seg

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
        while bytes_sum > 65535:
            bytes_sum = (bytes_sum & 0xFFFF) + (bytes_sum >> 16)
        return ~bytes_sum & 0xFFFF

    def flag_str(self) -> str:
        if self.ack:
            if self.syn:
                return '[SYN, ACK] '
            elif self.fin:
                return '[FIN, ACK] '
            else:
                return '[ACK] '
        if self.syn:
            return '[SYN] '
        if self.fin:
            return '[FIN] '
        return ''

    def log_info(self) -> str:
        op = None
        if 5 in self.options:
            op = self.options[5]
        return f'{self.src_port} -> {self.dest_port} {self.flag_str()}Seq={self.seq_num} Ack={self.ack_num} ' \
               f'Win={self.recv_win} Len={len(self.payload)} Op={op}'
