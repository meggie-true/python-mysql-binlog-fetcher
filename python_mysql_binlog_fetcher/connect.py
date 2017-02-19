# -*- coding: utf-8 -*-

"""
---------------------------------------------------------------------------------
"THE MODIFIED BEER-WARE LICENSE" (Revision 42):
<adam.bambuch2@gmail.com> wrote this file. As long as you retain this notice you
can do whatever you want with this stuff. If we meet some day, and you think
this stuff is worth it, you can buy me a beer in return Adam Bambuch

THIS SOFTWARE IS PROVIDED BY Adam Bambuch ''AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Adam Bambuch BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
---------------------------------------------------------------------------------
"""

import threading
import socket
import errno
import time
import Queue

from python_mysql_binlog_fetcher.bytebuff import bytebuff
from python_mysql_binlog_fetcher import exceptions

_NETWORK_LOOP_SLEEP = 1.0 / 1000.0
_NETWORK_TIMEOUT = 60.0


class Connect(threading.Thread):
    def __init__(self,host, port=3306):
        super(Connect, self).__init__()

        self._host = host
        self._port = port

        self._recv_buff = ""
        self._parsed_header = None
        self._connection = None
        self._err = None
        self._die = False
        self._connection = None
        self._send_queue = Queue.Queue()
        self._recv_queue = Queue.Queue()

    def _connect(self):
        self._connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print self._host
        self._connection.connect((self._host, self._port))
        self._connection.setblocking(False)

    def _worker(self):
        self._connect()

        while True:
            time.sleep(_NETWORK_LOOP_SLEEP)
            while True:
                sequence_id, buff = self._recv_packet()
                if sequence_id is None:
                    break
                self._recv_queue.put((sequence_id, buff))
            while True:
                try:
                    sequence_id, buff = self._send_queue.get_nowait()
                    self._send_packet(sequence_id, buff.data)
                except Queue.Empty:
                    break

    def run(self):
        try:
            self._worker()
        except socket.error as e:
            if not self._die:
                self._err = e
                raise
        except Exception as e:
            self._err = e
            raise

    def _send_packet(self, sequence_id, data):
        size = len(data)
        size1 = size >> 8
        size2 = size & 0xFF

        buff = bytebuff()
        buff.add("B", size2)
        buff.add("H", size1)
        buff.add("B", sequence_id)
        buff.add_raw(data)

        self._send(buff.data)

    def _recv_packet(self):
        """
        https://dev.mysql.com/doc/internals/en/mysql-packet.html
        """

        self._recv_to_buff()

        if len(self._recv_buff) < 4 and not self._parsed_header:
            return None, None
        if not self._parsed_header:
            first4 = self._recv_buff[:4]
            headerbuff = bytebuff(first4)
            size = headerbuff.get("H") << 8 + headerbuff.get("B")
            sequence_id = headerbuff.get("B")
            self._recv_buff = self._recv_buff[4:]
            self._parsed_header = (size, sequence_id)
        else:
            size = self._parsed_header[0]
            sequence_id = self._parsed_header[1]

        if self._recv_buff < size:
            return None, None

        data = self._recv_buff[:size]
        buff = bytebuff(data)
        self._recv_buff = self._recv_buff[size:]
        self._parsed_header = None

        return sequence_id, buff

    def _recv_to_buff(self):
        buffsize = 4096
        data = ""
        try:
            while 1:
                data += self._connection.recv(buffsize)
                if len(data) % buffsize or len(data) == 0:
                    break
        except socket.error as e:
            if e.errno not in (errno.EWOULDBLOCK, errno.EAGAIN):
                raise

        self._recv_buff += data
        return len(data)

    def _send(self, data):
        self._connection.sendall(data)

    def send_packet(self, sequence_id, buff):
        self._send_queue.put((sequence_id, buff))

    def recv_packet(self, blocking=False):
        sequence_id, buff = None, None
        for i in xrange(int(_NETWORK_TIMEOUT / _NETWORK_LOOP_SLEEP)):
            try:
                sequence_id, buff = self._recv_queue.get_nowait()
                break
            except Queue.Empty:
                pass
            if not blocking:
                break
            time.sleep(_NETWORK_LOOP_SLEEP)
        if blocking and sequence_id is None:
            raise exceptions.FetcherStreamBrokenError("Connection timedout")
        return sequence_id, buff
