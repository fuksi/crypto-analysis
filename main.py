from src.computed_job import compute_all_price_point
from src.computed import get_pricepoints_winner_count
from src.db import PgDb

def main():
    db = PgDb()
    ccys = ['BTCUSD', 'ETHUSD', 'EOSUSD', 'XRPUSD', 'LTCUSD']
    get_pricepoints_winner_count(db, ccys, , '2017-08-10T00:00:00Z', 15)
    # compute_all_price_point()

if __name__ == '__main__':
    main()