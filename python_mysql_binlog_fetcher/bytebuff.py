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

import struct
import binascii

from python_mysql_binlog_fetcher import common


class bytebuff(object):
    def __init__(self, data = "", position = 0, bit_position = 0, default_order = "<"):
        """
        Parameters
        ----------
        data : str
        position: int
        default_order : str
        """

        self.data = data # TODO: use something mutable
        self.default_order = default_order

        self._position = position

    def __add__(self, buff):
        """
        Join bytebuff or string, left position unchanged.
        Parameters
        ----------
        buff : str, bytebuff
        """

        if isinstance(buff, bytebuff):
            self.data += buff.data
            return self

        if isinstance(buff, str):
            self.data += buff
            return self

        raise TypeError("cannot concatenate %s and %s objects" % (self.__name__, buff.__name__))

    def copy(self):
        buff = bytebuff()
        buff.data = self.data
        buff.default_order = self.default_order
        buff._position = self._position
        return buff

    def __str__(self):
        """
        Returns
        -------
        str
            hex(buff)
        """

        return "0x" + binascii.hexlify(self.data)

    def __len__(self):
        return len(self.data)

    def cut(self):
        """
        Remove already read data
        """

        self.data = self.data[self._position:]
        self._position = 0

    def add(self, fmt, item):
        """
        Parameters
        ----------
        fmt : str
        *args : items specified in given format

        Raises
        ------
        struct.error
        """

        fmt = self._fmt_order_add(fmt)
        assert(len(fmt) == 2 or
               (len(fmt) > 2 and fmt[-1] == "s" and common.is_number(fmt[1:-1])))

        if fmt[1] == "S":
            self.data += item
            self.data += "\0"
            return

        self.data += struct.pack(fmt, item)

    def get_raw(self, length):
        return self.get("%ds" % length)

    def add_raw(self, data):
        self.add("%ds" % len(data), data)

    def add_bytes(self, bytes):
        """
        Parameters
        ----------
        bytes : str
        """

        self.data += str(bytes)

    def add_zeros(self, size_or_fmt):
        """
        Parameters
        ----------
        size_or_fmt : int or str
        """

        size = self._get_size_fmt(size_or_fmt)
        self.add(str(size) + "s", size * "\0")

    def skip(self, size_or_fmt):
        """
        Parameters
        ----------
        size_or_fmt : int or str
        """

        size = self._get_size_fmt(size_or_fmt)
        self._position += size

    def get(self, fmt):
        fmt = self._fmt_order_add(fmt)
        assert(len(fmt) == 2 or
               (len(fmt) > 2 and fmt[-1] == "s" and common.is_number(fmt[1:-1])))

        if fmt[1] == "S": # null terminated string
            string_g = self.data[self._position:].split("\0")[0]
            if len(string_g) >= len(self.data[self._position:]):
                raise struct.error()
            self._position += len(string_g) + 1
            return string_g

        fmt_size = self.calcsize(fmt)
        fmt_item_end = self.calcsize(fmt) + self._position
        unpacked = struct.unpack(fmt, self.data[self._position:fmt_item_end])
        self._position += fmt_size

        return unpacked[0]

    def get_bytes(self, size_or_fmt):
        """
        Parameters
        ----------
        size_or_fmt : int or str

        Returns
        -------
        bytes : str
        """

        size = self._get_size_fmt(size_or_fmt)
        return self.get("%ds" % size)

    def _get_size_fmt(self, size_or_fmt):
        """
        Calc size from argument or return argument if that is instance of int.

        Parameters
        ----------
        size_or_fmt : int or str
            Size of fmt for struct.calcsize

        Returns
        -------
        size : int
        """

        assert(isinstance(size_or_fmt, str) or isinstance(size_or_fmt, int))

        size = 0
        if isinstance(size_or_fmt, str):
            size = self.calcsize(size_or_fmt)
        else:
            size = size_or_fmt
        return size

    def _fmt_order_add(self, fmt):
        """
        Return default_order + fmt if order is not specified in given fmt.

        Parameters
        ----------
        fmt : str
            format string

        Returns
        -------
        fmt : str
            format string
        """

        if self._is_order(fmt[0]):
            return fmt
        else:
            return self.default_order + fmt

    def _is_order(self, char):
        """
        Return true if argument is order character.

        Parameters
        ----------
        char : str

        Returns
        -------
        size : bool
        """

        assert(len(char) == 1)
        if char in ("@=<>!"):
            return True
        return False

    def clear(self):
        """
        Clear all data, set position to 0.
        """

        self.data = ""
        self._position = 0

    def set_default_order(self, default_order):
        """
        Set default order

        Parameters
        ----------
        default_order : str
        """

        self.default_order = default_order

    calcsize = struct.calcsize
