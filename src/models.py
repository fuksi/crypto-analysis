from collections import namedtuple

Trade = namedtuple('Trade', ['time', 'price', 'amount', 'symbol'])
DeltaPoint = namedtuple('DeltaPoint', ['time', 'price', 'd', 'pct', 'n'])
