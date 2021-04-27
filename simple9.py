class Simple9:
    def __init__(self):
        self.shifts = [1, 2, 3, 4, 5, 7, 9, 14, 28]
        self.masks = [i * (1 << 28) for i in range(1, 10)]
        self.encode_type = \
            [ [ self.shifts[-i - 1], 2**self.shifts[i] - 1, self.masks[-i - 1], self.shifts[i] ] \
            for i in range(len(self.masks)) ]
        self.decode_type = \
            { self.masks[-i - 1] : [ self.shifts[-i - 1], 2**self.shifts[i] - 1, self.shifts[i] ] \
            for i in range(len(self.masks)) }

    def encode(self, content):
        """
        Кодирование списка элементов content.
        """
        offset = 0
        res = []
        while offset < len(content):
            for encode_type in self.encode_type:
                (n, upper, code, single_shift) = encode_type
                if offset + n <= len(content) and max(content[offset:offset + n]) <= upper:
                    cur_val = content[offset]
                    for i in range(1, n):
                        cur_val |= (content[offset + i] << (i * single_shift))
                    offset += n
                    res.append(code | cur_val)
                    break
        # return res
        return '\t'.join(map(str, res)).encode('utf-8')

    def decode(self, content):
        """
        Декодирование списка элементов content.
        """
        res = []
        content = content.decode('utf-8')
        for item in content.split('\t'):
            item = int(item)
            (n, upper, single_shift) = self.decode_type[0xf0000000 & item]
            data = 0x0fffffff & item

            for i in range(n):
                res.append(upper & data)
                data >>= single_shift

        return res
