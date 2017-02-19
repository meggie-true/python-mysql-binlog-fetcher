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
import hashlib

from python_mysql_binlog_fetcher.connect import Connect
from python_mysql_binlog_fetcher import const
from python_mysql_binlog_fetcher import exceptions
from python_mysql_binlog_fetcher.bytebuff import bytebuff
from python_mysql_binlog_fetcher import secret


class Client(threading.Thread):
    def __init__(self, user, password, server_id, host, port=3306):
        super(Client, self).__init__()

        self._user = user
        self._password = password
        self._server_id = server_id

        self._connect = Connect(host, port)
        self._server_version = None
        self._connection_id = None
        self._auth_plugin_data = None

    def _hash_password(self):
        sha1_password = hashlib.sha1(self._password).digest()
        sha1_sha1_password = hashlib.sha1(sha1_password).digest()
        sha1_combined = hashlib.sha1(self._auth_plugin_data[:20] + sha1_sha1_password).digest()

        hashed = ""
        for i in range(20):
            hashed += chr(ord(sha1_password[i]) ^ ord(sha1_combined[i]))
        return hashed


    def _handle_initial_handshake(self, sequence_number, buff):
        protocol_version = buff.get("B")
        if protocol_version != const.PROTOCOL_VERSION:
            raise exceptions.FetcherUnsupportedProtocolVersionError()
        self._server_version = buff.get("S")
        self._connection_id = buff.get("I")
        self._auth_plugin_data = buff.get("8s")
        buff.skip(19)
        self._auth_plugin_data += buff.get("13s")

        # we don't care about rest of the packet

    def _send_handshake_response(self):
        client_flags = const.CLIENT_LONG_PASSWORD | const.CLIENT_PROTOCOL_41 | const.CLIENT_LONG_FLAG | const.CLIENT_SECURE_CONNECTION
        hashed_password = self._hash_password()
        max_packet_size = 0xFFFFFF
        character_set = const.CHARACTER_SET_UTF8_GENERAL_CI

        buff = bytebuff()
        buff.add("I", client_flags)
        buff.add("I", max_packet_size)
        buff.add("B", character_set)
        buff.add_zeros(23)
        buff.add("S", self._user)
        buff.add("B", len(hashed_password))
        buff.add_raw(hashed_password)

        self._connect.send_packet(1, buff)

    def _auth(self):
        sequence_number, buff = self._connect.recv_packet(True)
        self._handle_initial_handshake(sequence_number, buff)
        self._send_handshake_response()
        sequence_number, buff = self._connect.recv_packet(True)
        print buff.data

    def _worker(self):
        self._connect.start()
        self._auth()

    def run(self):
        self._worker()

x = Client("test", secret.password, 33, "localhost")
x.start()
x.join()
print x._server_version
