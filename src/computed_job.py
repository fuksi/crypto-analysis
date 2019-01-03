from .db import PgDb
from .computed import compute_price_changes
from .models import Trade
import pendulum

db = PgDb()

def compute_all_price_point():
    symbols = ['BTCUSD', 'ETHUSD', 'EOSUSD', 'XRPUSD', 'LTCUSD']
    intervals = [15, 20, 30, 60, 120]

    # Starting from the last delta point if exists
    # Else starting from common first trade timestamp for all ccys
    for interval in intervals:
        print(f'Generating price points with {str(interval)} seconds interval')
        start_points = db.get_latest_pricepoints(symbols, interval)
        if not start_points:
            start_time = pendulum.parse('2017-07-01T00:00:00Z')
        else:
            start_time = start_points[symbols[0]].time.add(seconds=1)

        # While not stop immediately?
        # 'cause there can be gap in trades
        # Let's stop if there is no common trade for a while
        no_common_pricepoints_count = 0
        no_common_pricepoints_count_limit = 100

        execution_period_in_minutes = 6 
        # execution_period_in_days = 1
        while True:
            start = pendulum.now()
            has_common_pricepoints = True
            # end_time = start_time.add(days=execution_period_in_days)
            end_time = start_time.add(minutes=execution_period_in_minutes)
            all_pricepoints = {}
            
            # Get all price points
            for symbol in symbols:
                trades = db.get_trades(symbol, start_time, end_time)
                trades = [Trade(pendulum.instance(time), price) for time, price in trades]
                start_point = start_points.get(symbol)
                pricepoints = compute_price_changes(start_point, trades, interval)

                if not pricepoints:
                    has_common_pricepoints = False
                    break

                all_pricepoints[symbol] = pricepoints

            
            if has_common_pricepoints:
                '''
                We have different interval of points
                S1 (start) - E1 (end)
                S2 - E2
                S3 - E3
                etc..

                The goal is to get maximum number of price point that is retrievable for all ccys
                Solution:
                    - get the latest (biggest) S
                    - get the earliest (smallest) E
                    - extract all in between
                '''
                common_start = max([values[0].time for key, values in all_pricepoints.items()])
                common_end = min([values[-1].time for key, values in all_pricepoints.items()])
                if common_end > common_start:
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
                        has_common_pricepoints = False
                    else:
                        db.insert_pricepoints(common_pricepoints, interval)
                        print(f'Inserted {len(common_pricepoints[symbols[0]])} for {", ".join(symbols)} from {start_time} to {end_time}')

                        # Update start_points
                        for symbol, values in common_pricepoints.items():
                            start_points[symbol] = common_pricepoints[symbol][-1]

                        start_time = start_points[symbols[0]].time.add(seconds=1)
                        no_common_pricepoints_count = 0
                else:
                    has_common_pricepoints = False

            if not has_common_pricepoints:
                print(f'No common pricepoint found from {start_time} to {end_time}')

                no_common_pricepoints_count += 1
                start_time = start_time.add(minutes=execution_period_in_minutes)
                start_points = {}

            if no_common_pricepoints_count > no_common_pricepoints_count_limit:
                print('Stopped! No common price point found for a very long period!')
                break
            
            took = pendulum.now().diff(start).in_seconds()
            print(f'Task took {str(took)} seconds!')

