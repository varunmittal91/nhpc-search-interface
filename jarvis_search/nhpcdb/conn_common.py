import sys

class db_opts:
    transaction_header_size = 100
    max_transaction_size = 10000000 - transaction_header_size

    def __init__(self):
        self._opts = []
	self.__action_size = 0
    def addOp(self, db_opt):
        self._opts.append(db_opt)
    def get_opts(self):
        i = 0
        for opt in self._opts:
            self.__action_size += (sys.getsizeof(opt) + 3)
            if self.__action_size > self.max_transaction_size:
                yield self._opts[:i]
                self._opts = self._opts[i:]
                self.__action_size = 0
            i += 1
        yield self._opts
