from src.models import DeltaPoint

def compute_price_changes(start_point, trades, interval_seconds):
    if interval_seconds not in [15, 20, 30, 60]:
        raise ValueError('Interval value not supported. Must be either 15, 20, 30, or 60')
    if not trades:
        raise ValueError('Trades cannot be empty')
    if not all(trades[i].time < trades[i+1].time for i in range(len(trades) - 1)):
        raise ValueError('Trades must be in increasing order by time')

    result = []
    while True:
        next_time = start_point.time.add(seconds=interval_seconds)
        if next_time > trades[-1].time:
            break

        interval_trades = [t for t in trades if t.time > start_point.time and t.time <= next_time]
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





    