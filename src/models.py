from collections import namedtuple

Trade = namedtuple('Trade', ['time', 'price'])
DeltaPoint = namedtuple('DeltaPoint', ['time', 'price', 'd', 'pct', 'n'])
