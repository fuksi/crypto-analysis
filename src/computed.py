from .models import DeltaPoint
from .db import PgDb

import pendulum
import pandas as pd
import os
import json

def compute_price_changes(start_point, trades, interval_seconds, end_time=None):
    if not trades:
        return []
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
        if not end_time and next_time > trades[-1].time:
            break
        if end_time and next_time > end_time:
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

def get_pricepoints_winner_count(symbols, start, end, interval, prev_aggregation):
    ''' Get winning count of each symbol over a period 
        Use a variation of 'map reduce' to aggregate the result, since there way too much data to hold in memory
        E.g. if start = 2017-12-08T00:00:00, end = 2018-12-08T22:58:42+00:00, we'll split into
            2017-12-08T00:00:00 - 2017-12-09T23:59:59
            2017-12-09T00:00:00 - 2017-12-10T23:59:59
        ...            
        depend on the job/batch size

    '''
    db = PgDb()

    job_size = 10000
    job_start = start
    job_end = job_start.add(seconds=interval*job_size - 1)

    if prev_aggregation:
        # A partial aggregation can be provided, must have the same starting date
        prev_start, prev_end, result = prev_aggregation
        if prev_start != start:
            raise ValueError('Previous aggregation if provided must have the same starting point!')

        job_start = prev_start.add(interval) # disregard the overlapping pricepoint with the previous aggregation
        job_end = job_start.add(seconds=interval*job_size - 1)
    else:
        result = {}
        for s in symbols:
            result[s] = 0

    if job_end > end:
        job_end = end

    # Aggregate
    while True:
        # Get data
        data = [(ccy, db.get_pricepoints(ccy, job_start, job_end, interval)) for ccy in symbols]

        # Put to df
        df_dict = {}
        for ccy, values in data:
            if not df_dict.get('time'):
                df_dict['time'] = [p['time'] for p in values]
                
            prefix = ccy
            df_dict[f'{prefix}'] = [float(p['pct']) for p in values]
            df_dict[f'{prefix}_trades_count'] = [float(p['n']) for p in values]
            
        df = pd.DataFrame(df_dict)
        df['winner'] = df[symbols].idxmax(axis=1)
        counts = df['winner'].value_counts()
        for symbol in symbols:
            if counts.get(symbol):
                result[symbol] += counts[symbol]
        
        job_start = job_start.add(seconds=interval*job_size)
        job_end = job_start.add(seconds=interval*job_size - 1)

        if job_start > end:
            break

        if job_end > end:
            job_end = end

    return result

def get_all_result():
    result_path = './saved_result/'
    files = sorted([int(f) for f in os.listdir(result_path)])

    results = []
    file_paths = [result_path + str(f) for f in files]
    for path in file_paths:
        with open(path) as f:
            results.append(json.load(f))

    return (files, results)





