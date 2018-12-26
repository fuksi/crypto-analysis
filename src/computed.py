from models import DeltaPoint
import pendulum

def compute_price_changes(start_point, trades, interval_seconds):
    if interval_seconds not in [15, 20, 30, 60]:
        raise ValueError('Interval value not supported. Must be either 15, 20, 30, or 60')
    if not trades:
        raise ValueError('Trades cannot be empty')
    if not all(trades[i].time <= trades[i+1].time for i in range(len(trades) - 1)):
        raise ValueError('Trades must be in increasing order by time')

    if not start_point:
        first_trade = trades[0]
        first_trade_time = first_trade.time
        start_time = find_closest_possible_start_time(first_trade_time, interval_seconds)
        interval_trades = [t for t in trades if t.time <= start_time]
        start_point = DeltaPoint(start_time, interval_trades[-1].price, 0, 0, 0)

    # Purpose: store the idx of last visited trade, discard all trades before that index
    cut_idx = 0
    trades_count = len(trades)

    result = []
    while True:
        next_time = start_point.time.add(seconds=interval_seconds)
        if next_time > trades[-1].time:
            break

        interval_trades = []
        for i in range(cut_idx, trades_count):
            t = trades[i]
            if t.time > next_time:
                cut_idx = i
                break

            if t.time > start_point.time:
                interval_trades.append(t)

        if not interval_trades:
            next_delta = DeltaPoint(next_time, start_point.price, 0, 0, 0)
        else:
            last_trade = interval_trades[-1]
            d = last_trade.price - start_point.price
            pct = d / start_point.price * 100
            n = len(interval_trades)
            next_delta = DeltaPoint(next_time, last_trade.price, d, pct, n)

        result.append(next_delta)
        start_point = next_delta

    return result

def find_closest_possible_start_time(timestamp, interval):
    ''' Find closest possible start time given a timestamp and interval
    E.g.
        - Given timestamp: 2017-08-01T00:01:24
        - Given interval: 20s

        Start time CAN'T be 
        - 2017-08-01T00:01:00, or 
        - 2017-08-01T00:01:20
        since we won't have data for this

        The closest possible start time is then 2017-08-01T00:01:40
    
    Arguments:
        timestamp: pendulum 
        interval: integer
    '''

    next_start_time = timestamp.start_of('minute')
    while True:
        if next_start_time >= timestamp:
            return next_start_time

        next_start_time = next_start_time.add(seconds=interval)
