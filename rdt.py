from typing import Text

from USocket import UnreliableSocket

import struct, queue, threading, time, random, logging

from queue import PriorityQueue

#############################################################################
# TODO:
# 1. Determine the buff size of each socket, that is, the size of the queue
#############################################################################

LOG_FORMAT = "[%(name)s %(levelname)s] %(filename)s[func: %(filename)s  line:%(lineno)d] %(asctime)s: %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
client_logger = logging.getLogger("CLIENT")
server_logger = logging.getLogger("SERVER")

DEFAULT_BUFF_SIZE = 2048
DEFAULT_WIN_SIZE = 65535
CONSTANT2P32 = 4294967296  # 2^32


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
        self.master_server_addr = ('127.0.0.1', 0)
        self.allow_accept = False
        self.data_center = None
        self.super_recvfrom = super().recvfrom
        self.conn_cnt = -1
        self.max_conn = 10

        # server
        self.is_server = False
        self.busy = False
        self.segment_buff = queue.Queue()
        self.data_buff = bytearray()

        # Retransmission mechanism
        self.recv_win_size = 0  # recv window size
        self.local_seq_num = 0  # the position of next byte send in send window
        self.local_ack_num = 0  # the position of the next byte of the expected ack in recv window
        self.mss = 1460
        self.rtt_orign = 10

        # socket
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
            server_logger.info(f"The number of server [{self.master_server_addr}] connections has reached the maximum")
            return None, None

        self.allow_accept = True
        peer_set = set()

        server_logger.info(f"Server [{self.master_server_addr}] is waiting for the client to connect")
        while True:
            rdt_seg, addr = self._recv_from(DEFAULT_BUFF_SIZE)

            # if time_out
            #     break

            if rdt_seg.syn:
                seq_num = 0  # random.randint(0, RDTSegment.SEQ_NUM_BOUND - 1)
                ack_num = rdt_seg.seq_num + 1
                syn_ack_seg = RDTSegment(rdt_seg.dest_port, rdt_seg.src_port, seq_num, ack_num,
                                         ack=True, syn=True)
                self._send_to(syn_ack_seg, addr)
                peer_set.add(addr)
                server_logger.info(f"Connection establishment request received from client [{addr}]")
                server_logger.info(f"Client request connection pool: {peer_set}")
            elif rdt_seg.ack and addr in peer_set:
                self.data_center.add_sock(addr, conn)
                # master server
                conn.master_server_addr = self.master_server_addr
                conn.data_center = self.data_center
                # server
                conn.is_server = True
                # Retransmission mechanism
                self.recv_win_size = DEFAULT_WIN_SIZE
                conn.local_seq_num = rdt_seg.ack_num
                conn.local_ack_num = rdt_seg.seq_num
                # socket
                conn.local_addr = self.local_addr
                conn.peer_addr = addr
                conn._recv_from = conn._get_recvfrom(conn)
                conn._send_to = conn._get_sendto(self)

                server_logger.info(f"Server [{self.master_server_addr}] accepts the connection from client [{addr}]")
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
        self._send_to = self._get_sendto(self)
        self._recv_from = self._get_recvfrom(self)
        self.recv_win_size = DEFAULT_WIN_SIZE
        self.peer_addr = address

        client_logger.info(f"client [{self.getsockname()}] initiate connection request to server [{address}].")

        # send syn pkt
        self.local_seq_num = 0  # random.randint(0, RDTSegment.SEQ_NUM_BOUND - 1)
        self.local_ack_num = 0
        syn_seg = RDTSegment(self.local_addr[1], address[1], self.local_seq_num, self.local_ack_num, syn=True)
        self._send_to(syn_seg, address)

        self.settimeout(2)
        try_conn_cnt = 1
        while True:
            try:
                rdt_seg, addr = self._recv_from(DEFAULT_BUFF_SIZE)
                try_conn_cnt = 1
            except OSError:
                if try_conn_cnt > 5:
                    client_logger.info("Request no response, exit connection request.")
                    return
                self._send_to(syn_seg, address)
                try_conn_cnt += 1
                client_logger.info("request timeout, try again.")
                continue

            # if time_out
            #     break
            # if syn & ack, send ack
            if address == addr and rdt_seg.syn and rdt_seg.ack and self.local_seq_num + 1 == rdt_seg.ack_num:
                self.local_ack_num = rdt_seg.seq_num + 1
                self.local_seq_num = rdt_seg.ack_num
                ack_seg = RDTSegment(self.local_addr[1], rdt_seg.src_port, self.local_seq_num, self.local_ack_num,
                                     ack=True)
                self._send_to(ack_seg, address)
                break

        self.settimeout(None)
        client_logger.info(f"client [{self.local_addr}] is connected to server [{address}].")
        self.recv_thread.start()
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
        timeout_sec = self.gettimeout()
        timeout_sec = None
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
        send_window_len = self.mss

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
            alpha = 0.125
            beta = 0.25
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
                        ((seq_base + next_seq) % seq_size, data[next_seq: next_seq + self.mss]))
                    next_seq += self.mss
                elif send_base + send_window_len > len(data) > next_seq and next_seq + self.mss > len(data):
                    self.seq_num_payload_buff.put(((seq_base + next_seq) % seq_size, data[next_seq:]))
                    next_seq = len(data)
                else:
                    break
            # receive ack
            send_base_seq = (seq_base + send_base) % seq_size
            while not self.ack_num_option_buff.empty():
                ack_tuple = self.ack_num_option_buff.get()
                ack_num = ack_tuple[0]
                options = ack_tuple[1]
                if ack_num == seq_base + len(data):
                    return
                # move window
                if ack_num > send_base_seq:
                    send_base += (ack_num - send_base_seq)
                    if ack_num - send_base_seq <= self.mss:
                        timeout_rst(time.time() - timer_start)
                    congestion_control_newAck(ack_num - send_base_seq)
                    fr_cnt = 0
                    continue
                elif ack_num < send_base_seq and send_base_seq + send_window_len >= seq_size + ack_num + 1:
                    send_base += (seq_size + ack_num + 1 - send_base_seq)
                    if seq_size + ack_num + 1 - send_base_seq <= self.mss:
                        timeout_rst(time.time() - timer_start)
                    congestion_control_newAck(seq_size + ack_num - send_base_seq)
                    fr_cnt = 0
                    continue
                # acculate for fast retransmission
                elif 5 in options:
                    if fr_cnt == 0:
                        unACK_range[0] = send_base_seq
                        unACK_range[1] = options[5][1]
                        fr_cnt += 1
                    else:
                        if (options[5][1] < unACK_range[1] and unACK_range + seq_size + 1 >= options[5][
                            1] + send_window_len) or (
                                options[5][1] > unACK_range[1] and unACK_range + seq_size + 1 <= options[5][
                            1] + send_window_len):
                            unACK_range[1] = options[5][1]
                        fr_cnt += 1
                        # judge fast retransimission
                        if fr_cnt == 3:
                            fr_base = send_base
                            if unACK_range[0] < unACK_range[1]:
                                fr_len = unACK_range[1] - unACK_range[0]
                            else:
                                fr_len = unACK_range[1] - unACK_range[0] + seq_size
                            fr_next = fr_base
                            while True:
                                if fr_base + fr_len - fr_next >= self.mss:
                                    self.seq_num_payload_buff.put(
                                        ((seq_base + fr_next) % seq_size, data[fr_next: fr_next + self.mss]))
                                    fr_next += self.mss
                                elif fr_next < len(data):
                                    self.seq_num_payload_buff.put(((seq_base + fr_next) % seq_size, data[fr_next:]))
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
                    self.seq_num_payload_buff.put(
                        ((seq_base + send_base) % seq_size, data[send_base: min(send_base + self.mss, len(data))]))
                    timer_start = time.time()
                    send_window_len = 0
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
            self.recv_thread.stop()
            fin_seg = RDTSegment(self.getsockname()[1], self.peer_addr[1], self.local_seq_num, self.local_ack_num,
                                 fin=True)
            ack_seg = RDTSegment(self.getsockname()[1], self.peer_addr[1], self.local_seq_num,
                                 0, ack=True)
            self.settimeout(1)
            while True:
                if not self.peer_closing and not self.peer_closed:
                    self._send_to(fin_seg, self.peer_addr)
                elif self.peer_closed:
                    try:
                        peer_seg, addr = self._recv_from(DEFAULT_BUFF_SIZE)
                        if peer_seg.fin:
                            ack_seg = RDTSegment(self.getsockname()[1], self.peer_addr[1], self.local_seq_num,
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
                    peer_seg, addr = self._recv_from(DEFAULT_BUFF_SIZE)
                except OSError:
                    continue

                if peer_seg.ack and peer_seg.ack_num == self.local_seq_num + 1:
                    self.peer_closing = True
                elif peer_seg.fin:
                    ack_seg = RDTSegment(self.getsockname()[1], self.peer_addr[1], self.local_seq_num,
                                         peer_seg.seq_num + 1, ack=True)
                    self._send_to(ack_seg, self.peer_addr)
                    self.peer_closed = True
                    # break

            self._send_to = None
            self.local_closed = True
            client_logger.info(
                f"client [{self.getsockname()}] successfully closed the connection with server [{self.peer_addr}]")

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
            self._recv_from = self._get_recvfrom(self)
            self._send_to = self._get_sendto(self)
            try:
                self.master_server_addr = super(RDTSocket, self).getsockname()
            except OSError:
                self.bind(('127.0.0.1', 0))
                self.master_server_addr = super(RDTSocket, self).getsockname()
            self.local_addr = self.master_server_addr
            self.data_center = DataCenter(self)
            self.data_center.start()
        else:
            self.max_conn = max_conn

    def getsockname(self) -> (str, int):
        # try:
        #     addr = super(RDTSocket, self).getsockname()
        #     return addr
        # except OSError:
        #     return self.master_server_addr
        return self.local_addr

    def _get_recvfrom(self, sock: 'RDTSocket'):
        """

        """
        if self.is_server or self.is_master_server:
            def recvfrom_func(bufsize) -> ('RDTSegment', tuple):
                timeout_sec = self.gettimeout()
                if timeout_sec is None:
                    timeout_sec = 1000
                tout = time.time() + timeout_sec
                while time.time() < tout:
                    if not self.segment_buff.empty():
                        seg, addr = self.segment_buff.get()
                        rdt_seg = RDTSegment.parse(seg)
                        if rdt_seg:
                            server_logger.info(
                                f"Received: {self.peer_addr[0]} -> {self.local_addr[0]}  {rdt_seg.log_info()}")
                            return rdt_seg, addr
                        server_logger.info(f"Server [{self.local_addr}] Received corrupted segment")

                raise OSError

            return recvfrom_func
        else:
            def new_recv(bufsize) -> ('RDTSegment', tuple):
                while True:
                    data, frm = super(RDTSocket, self).recvfrom(bufsize)
                    rdt_seg = RDTSegment.parse(data)
                    if rdt_seg:
                        break
                    client_logger.info(f"Client [{self.local_addr}] Received corrupted segment")
                client_logger.info(
                    f"Received: {self.peer_addr[0]} -> {self.local_addr[0]}  {rdt_seg.log_info()}")
                return rdt_seg, frm

            return new_recv

    def _get_sendto(self, sock: 'RDTSocket'):
        if self.is_server or self.is_master_server:

            #     def new_send_func(rdt_seg: 'RDTSegment', addr):
            #         server_logger.info(
            #             f"Sent    : {self.local_addr[0]} -> {self.peer_addr[0]}  {rdt_seg.log_info()}")
            #         sock._send_to(rdt_seg.encode(), addr)
            #
            #     return new_send_func
            # elif self.is_master_server:
            def new_send_func(rdt_seg: 'RDTSegment', addr):
                server_logger.info(
                    f"Sent    : {self.local_addr[0]} -> {self.peer_addr[0]}  {rdt_seg.log_info()}")
                # super(RDTSocket, self).sendto(rdt_seg.encode(), addr)
                sock.sendto(rdt_seg.encode(), addr)

            return new_send_func
        else:
            def new_send_func(rdt_seg: 'RDTSegment', addr):
                client_logger.info(
                    f"Sent    : {self.local_addr[0]} -> {self.peer_addr[0]}  {rdt_seg.log_info()}")
                # super(RDTSocket, self).sendto(rdt_seg.encode(), addr)
                self.sendto(rdt_seg.encode(), addr)

            return new_send_func

    def _recv_segs_to_data_buff(self):
        """
        Receive and process the segment and add the processed data to the buff
        """
        # recv_data = bytearray()
        ack_seg = RDTSegment(self.local_addr[1], self.peer_addr[1], 0, self.local_ack_num)
        self.settimeout(1)
        while True:
            try:
                rdt_seg, addr = self._recv_from(DEFAULT_BUFF_SIZE)
            except OSError:
                print('recv time out')
                if not self.seq_num_payload_buff.empty():
                    # need send data which come from func send()
                    print('send1~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                    seq_payload = self.seq_num_payload_buff.get()
                    seq_seg = RDTSegment(self.local_addr[1], self.peer_addr[1], seq_payload[0], self.local_ack_num,
                                         payload=seq_payload[1])
                    self._send_to(seq_seg, self.peer_addr)
                continue

            if rdt_seg.ack:
                self.ack_num_option_buff.put((rdt_seg.ack_num, rdt_seg.options))

            data_len = len(rdt_seg.payload)
            if data_len != 0:
                print(' why???????????????????????????????????')
                # seq num is valid
                ack_seg.ack = True

                ack_range_tuple = (rdt_seg.seq_num, (rdt_seg.seq_num + data_len) % rdt_seg.SEQ_NUM_BOUND)
                if self.local_ack_num < rdt_seg.seq_num < self.local_ack_num + self.recv_win_size:
                    # seg_num inside recv window
                    if ack_range_tuple not in self.ack_set:
                        self.main_pq.put(AckRange(ack_range_tuple, rdt_seg.payload))
                        self.ack_set.add(ack_range_tuple)
                    ack_seg.ack_num = self.local_ack_num
                elif self.local_ack_num == rdt_seg.seq_num:
                    # seq_num equal recv window base
                    self.local_ack_num += data_len
                    self.data_buff.extend(rdt_seg.payload)
                    ack_seg.ack_num = self.local_ack_num
                    while True:
                        # Cumulative confirmation
                        if self.main_pq.empty():
                            if self.sub_pq.empty():
                                break
                            self.main_pq = self.sub_pq
                            self.sub_pq = PriorityQueue()
                            continue
                        min_ack_range = self.main_pq.queue[0]
                        if self.local_ack_num == min_ack_range.value[0]:
                            self.main_pq.get()
                            self.ack_set.remove(min_ack_range.value)
                            self.local_ack_num = min_ack_range.value[1]
                            self.data_buff.extend(min_ack_range.data)
                            continue
                        else:
                            # construct an option sack
                            ack_seg.ack_num = self.local_ack_num
                            ack_seg.options[5] = [(self.local_ack_num, min_ack_range.value[0])]
                            break
                elif self.local_ack_num - self.recv_win_size < rdt_seg.seq_num < self.local_ack_num:
                    # seq_num in left of window within one win_size
                    ack_seg.ack_num = self.local_ack_num
                elif rdt_seg.seq_num < self.local_ack_num + self.recv_win_size - RDTSegment.SEQ_NUM_BOUND:
                    # window rollback
                    self.sub_pq.put(AckRange(ack_range_tuple, rdt_seg.payload))
            else:
                ack_seg.ack = False
                ack_seg.options = {}

            if not self.seq_num_payload_buff.empty():
                # need send data which come from func send()
                print('send data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  ??????????????????????????????????????')
                seq_payload = self.seq_num_payload_buff.get()
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
        self.__running = threading.Event()  # The identity used to stop the thread
        self.__running.set()  # Initialization thread running
        self.rdt_socket = rdt_socket

    def run(self):
        self.rdt_socket.busy = True
        if self.rdt_socket.is_server:
            server_logger.info(f"Server [{self.rdt_socket.local_addr}] start processing segment")
            self.rdt_socket._recv_segs_to_data_buff()
        else:
            client_logger.info(f"Client [{self.rdt_socket.local_addr}] start processing segment")
            while self.__running.isSet():
                self.rdt_socket._recv_segs_to_data_buff()
        self.rdt_socket.busy = False

    def stop(self) -> None:
        """
        Stop thread
        :return:
        """
        self.__running.clear()


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
        server_logger.info("DataCenter start work...")
        while self.__running.isSet():
            self.__flag.wait()  # 为True时立即返回, 为False时阻塞直到self.__flag为True后返回
            # data, addr = self.data_entrance._recv_from(DEFAULT_BUFF_SIZE)
            data, addr = self.data_entrance.super_recvfrom(DEFAULT_BUFF_SIZE)
            if data:
                if addr in self.socket_table:
                    sock = self.socket_table.get(addr)
                    sock.segment_buff.put((data, addr))
                    print(f'Data distribution to {addr}')
                    if not sock.busy:
                        sock.recv_thread.start()
                        # ProcessingSegment(sock).start()
                elif self.data_entrance.allow_accept:
                    self.data_entrance.segment_buff.put((data, addr))

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
        self.header_len = (flags & 0xFF) >> 2

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
            options_byte.extend(struct.pack('!BBBB', 1, 1, 5, sack_cnt + 2))
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
        head_length = (flags & 0xFF) >> 2
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
        return f'{self.src_port} -> {self.dest_port} {self.flag_str()}Seq={self.seq_num} Ack={self.ack_num} ' \
               f'Win={self.recv_win} Len={len(self.payload)}'
