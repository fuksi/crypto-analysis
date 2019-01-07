from src.computed_job import compute_all_price_point
from src.computed import get_pricepoints_winner_count, compute_price_changes, get_all_result
from src.db import PgDb
from src.simple_mapreduce import SimpleMapReduce
from src.models import DeltaPoint, Trade
from src.settings import DB_SETTINGS

import pendulum
import numpy as np
import pandas as pd
import json
import os

db = PgDb()
symbols = ['BTCUSD', 'ETHUSD', 'EOSUSD', 'XRPUSD', 'LTCUSD']
intervals = ['15', '20', '30', '60', '120', '300']
nothing_moved_delta_row = [0 for symbol in symbols]
def map_func(start):
    start_timestamp = pendulum.parse(start)

    file_path = './saved_result/' + str(int(start_timestamp.float_timestamp))
    if os.path.isfile(file_path):
        return True

    end_timestamp = start_timestamp.end_of('day')
    symbols_trades = [(symbol, db.get_trades(symbol, start_timestamp, end_timestamp)) for symbol in symbols]

    result, summarized_result = {}, {}
    for i in intervals:
        result[i] = {}
        summarized_result[i] = {}

    for symbol, trades in symbols_trades:
        trades = [Trade(pendulum.instance(time), price) for time, price in trades]
        start_point = None
        start_price = db.get_closest_price_before_ts(symbol, start)
        if start_price:
            start_point = DeltaPoint(start_timestamp, start_price, 0, 0, 0) 

        for interval in intervals:
            pricepoints = compute_price_changes(start_point, trades, int(interval), end_time=end_timestamp)
            result[interval][symbol] = pricepoints


    for interval in intervals:
        winners = []
        movements = [0 for i in range(len(symbols) + 1)]
        row_count = len(result[interval][symbols[0]])
        interval_data = result[interval]

        # generate a row of delta
        # for each row
        # check if nothing moves
        # else find argmax
        for i in range(row_count):
            row = [interval_data[symbol][i].pct for symbol in symbols]
            if row == nothing_moved_delta_row:
                winners.append('NONE')
                movements[0] += 1
            else:
                winner_indx = np.argmax(row)
                number_of_ccy_moved = len([d for d in row if d != 0])
                movements[number_of_ccy_moved] += 1
                winners.append(symbols[winner_indx])

        # no trade ratio
        no_trade_ratio = {}
        for symbol in symbols:
            pp = interval_data[symbol]
            no_trade_count = sum(1 for i in pp if not i.n)
            ratio = no_trade_count / row_count
            no_trade_ratio[symbol] = ratio

        winners_dist = pd.Series(winners).value_counts().divide(row_count).to_dict()
        if not winners_dist.get('NONE'):
            winners_dist['NONE'] = 0

        result[interval] = {
            'winners': winners_dist,
            'movements': movements,
            'count': row_count,
            'no_trade': no_trade_ratio
        }

            
    with open(file_path, 'w') as outfile:
        json.dump(result, outfile)

    return True 

def reduce_func(value):
    return value

def main():
    start = pendulum.parse('2017-07-02T00:00:00Z')
    end = pendulum.parse('2019-01-03T00:00:00Z')
    # period_in_days = 1
    # end = start.add(days=(period_in_days - 1)).end_of('day')

    inputs = []
    while start < end:
        inputs.append(start.to_iso8601_string())
        start = start.add(days=1).start_of('day')

    mr = SimpleMapReduce(map_func, reduce_func)
    mr(inputs)

if __name__ == '__main__':
    main()