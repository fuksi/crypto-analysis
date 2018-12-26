from db import PgDb
from computed import compute_price_changes
from models import Trade
import pendulum

db = PgDb()

def compute_all_price_point():
    symbols = ['BTCUSD', 'ETHUSD', 'EOSUSD', 'XRPUSD', 'LTCUSD']

    # Starting from the last delta point if exists
    # Else starting from common first trade timestamp for all ccys
    start_points = db.get_latest_pricepoints_20(symbols)
    if not start_points:
        start_time = pendulum.parse('2017-07-01T00:00:00Z')
    else:
        start_time = start_points[symbols[0]].time.add(seconds=1)

    # While not stop immediately?
    # 'cause there can be gap in trades
    # if in 4 weeks there is no common trade, let's stop
    no_common_pricepoints_count = 0
    no_common_pricepoints_count_limit = 10

    execution_period_in_days = 1
    while True:
        end_time = start_time.add(days=execution_period_in_days)
        all_pricepoints = {}
        
        # Get all price points
        for symbol in symbols:
            trades = db.get_trades(symbol, start_time, end_time)
            trades = [Trade(pendulum.instance(time), price) for time, price in trades]
            start_point = start_points.get(symbol)
            pricepoints = compute_price_changes(start_point, trades, 20)
            all_pricepoints[symbol] = pricepoints


        '''
        We have different interval of points
        S1 (start) - E1 (end)
        S2 - E2
        S3 - E3
        etc..

        The goal is to get maximum number of price oint that is retrievable for all ccys
        Solution:
            - get the latest (biggest) S
            - get the earliest (smallest) E
        '''
        common_start = max([values[0].time for key, values in all_pricepoints.items()])
        common_end = min([values[-1].time for key, values in all_pricepoints.items()])
        start_indice = {}
        stop_indice = {}
        for symbol in symbols:
            pricepoints = all_pricepoints[symbol]
            count = len(pricepoints)
            for i in range(count):
                if pricepoints[i].time == common_start:
                    start_indice[symbol] = i
                    break

            for i in range(-1, -count-1, -1):
                if pricepoints[i].time == common_end:
                    stop_indice[symbol] = count + i
                    break

        common_pricepoints = {}
        for symbol in symbols:
            common_pricepoints[symbol] = all_pricepoints[symbol][start_indice[symbol]:stop_indice[symbol]]

        if not common_pricepoints[symbols[0]]:
            print(f'No common pricepoint found from {start_time} to {end_time}')

            no_common_pricepoints_count += 1
            start_time = start_time.add(days=execution_period_in_days)
            start_points = {}

            if no_common_pricepoints_count > no_common_pricepoints_count_limit:
                print('Stopped! No common price point found for 10 weeks straight!')
                break
        else:
            for symbol, values in common_pricepoints.items():
                db.insert_pricepoints_20(symbol, values)
                print(f'Inserted {len(values)} for {symbol} from {start_time} to {end_time}')

                # Update start_points
                start_points[symbol] = common_pricepoints[symbol][-1]

            start_time = start_points[symbols[0]].time.add(seconds=1)

compute_all_price_point()