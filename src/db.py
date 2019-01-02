import sqlite3
import pendulum
import psycopg2
import pyodbc

from models import DeltaPoint

class PgDb:
    def __init__(self):
        self.create()

    def connect(self):
        return psycopg2.connect(host="localhost",database="postgres", user="postgres", password="yaha")

    def create(self):
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("""
                    create table if not exists candles (
                        symbol varchar(10) NOT NULL,
                        time timestamptz NOT NULL,
                        open decimal(18,10) NOT NULL,
                        close decimal(18,10) NOT NULL,
                        high decimal(18,10) NOT NULL,
                        low decimal(18,10) NOT NULL,
                        volume decimal(18,10) NOT NULL,
                        PRIMARY KEY (symbol, time)
                    )
                """)

                cur.execute("""
                    create table if not exists fundings (
                        symbol varchar(10) NOT NULL,
                        id int NOT NULL,
                        time timestamptz NOT NULL,
                        amount decimal(18,10) NOT NULL,
                        rate decimal(18,10) NOT NULL,
                        period smallint NOT NULL,
                        PRIMARY KEY (symbol, time)
                    )
                """)

                cur.execute("""
                    create table if not exists tradings (
                        symbol varchar(10) NOT NULL,
                        id int NOT NULL,
                        time timestamptz NOT NULL,
                        amount decimal(18,10) NOT NULL,
                        price decimal(18,10) NOT NULL
                    )
                """)

                self.create_pricepoints_tables(cur)

        con.close()

    def create_pricepoints_tables(self, cur):
        intervals = [15, 20, 30, 60, 120]
        for interval in intervals:
            table_name = f'pricepoints_{str(interval)}'
            query = f'''create table if not exists {table_name}
                    (
                        symbol varchar(10) NOT NULL,
                        time timestamptz NOT NULL,
                        price decimal(18,10) NOT NULL,
                        change decimal(18,10) NOT NULL,
                        change_pct decimal(10, 5) NOT NULL,
                        trade_count int NOT NULL
                    );
                    
                    create unique index if not exists multi_idx_symbol_time_{table_name} on {table_name} (symbol, time DESC) 
                    '''
            
            cur.execute(query)
                    

    def insert_candles(self, symbol, candles):
        for candle in candles:
            candle.insert(0, symbol)

        with self.connect() as con:
            with con.cursor() as cur:
                args = [cur.mogrify('(%s, TO_TIMESTAMP(%s/1000), %s, %s, %s, %s, %s)', x).decode('utf-8') for x in candles]
                args_str = ','.join(args)
                cur.execute("""
                    insert into candles(
                        symbol, time, open, close, high, low, volume)
                    values """ + args_str + "on conflict do nothing")
                
        con.close()

    def insert_pricepoints(self, symbol_pricepoints_dict, interval):
        table_name = f'pricepoints_{str(interval)}'
        with self.connect() as con:
            with con.cursor() as cur:
                for symbol, pricepoints in symbol_pricepoints_dict.items():
                    args = [cur.mogrify(f'(\'{symbol}\'' + ', %s::timestamptz, %s, %s, %s, %s)', x).decode('utf-8') for x in pricepoints]
                    args_str = ','.join(args)
                    cur.execute(f'''insert into {table_name}
                                (symbol, time, price, change, change_pct, trade_count)
                                values {args_str} on conflict do nothing''')

        con.close()

    def insert_trades(self, symbol, trades):
        for trade in trades:
            trade.insert(0, symbol)

        with self.connect() as con:
            with con.cursor() as cur:
                args = [cur.mogrify('(%s, %s, TO_TIMESTAMP(%s/1000), %s, %s)', x).decode('utf-8') for x in trades]
                args_str = ','.join(args)
                cur.execute("""
                    insert into tradings(
                        symbol, id, time, amount, price)
                    values""" + args_str + "on conflict do nothing")
                
        con.close()

    def insert_funding_trades(self, symbol, trades):
        for trade in trades:
            trade.insert(0, symbol)

        with self.connect() as con:
            with con.cursor() as cur:
                args = [cur.mogrify('(%s, %s, TO_TIMESTAMP(%s/1000), %s, %s, %s)', x).decode('utf-8') for x in trades]
                args_str = ','.join(args)
                cur.execute("""
                    insert into fundings(
                        symbol, id, time, amount, rate, period)
                    values""" + args_str + "on conflict do nothing")
                
        con.close()

    def get_latest_candle_date(self, symbol):
        """
        Get the time of the most recent candle for a symbol
        """
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute('select max(time) from candles where symbol=%s',
                                    (symbol,))
                result = cur.fetchone()[0]
                if result is None:
                    return
                else:
                    return pendulum.instance(result)
                
        con.close()

    def get_latest_trading_date(self, symbol):
        """
        Get the time of the most recent trading for a symbol
        """
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute('select max(time) from tradings where symbol=%s',
                                    (symbol,))
                result = cur.fetchone()[0]
                if result is None:
                    return
                else:
                    return pendulum.instance(result)
                
        con.close()

    def get_latest_funding_date(self, symbol):
        """
        Get the time of the most recent funding for a symbol
        """
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute('select max(time) from fundings where symbol=%s',
                                    (symbol,))
                result = cur.fetchone()[0]
                if result is None:
                    return
                else:
                    return pendulum.instance(result)
                
        con.close()

    def get_latest_pricepoints(self, symbols, interval):
        table_name = f'pricepoints_{str(interval)}'
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute(f'''select symbol, time, price, 
                                change as d, change_pct as pct, trade_count as n

                                from {table_name}
                                where time = (select max(time) from {table_name})''')

                columns = [col[0] for col in cur.description]
                pricepoints = {}

                for row in cur.fetchall():
                    row_as_dict = dict(zip(columns, row))
                    row_as_dict['time'] = pendulum.instance(row_as_dict['time'])
                    symbol = row_as_dict['symbol']
                    row_as_dict.pop('symbol')
                    pricepoints[symbol] = DeltaPoint(**row_as_dict)
            
                return pricepoints
         
    def get_pricepoints(self, symbol, start, end, interval):
        table_name = f'pricepoints_{str(interval)}'
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute(f"""
                    select symbol, time, price, change as d, change_pct as pct, trade_count as n 
                    from {table_name} 
                    where symbol = %s and time between %s::timestamptz and %s::timestamptz
                """, (symbol, start, end))

                columns = [col[0] for col in cur.description]
                result = []
                for row in cur.fetchall():
                    result.append(dict(zip(columns, row)))

                return result

    def get_trades(self, symbol, start, end):
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute('''
                    select time, price from tradings where symbol=%s and time between %s and %s order by time asc
                ''', (symbol, start, end))

                return cur.fetchall()

class MssqlDatabase:
    def __init__(self):
        self.create()

    def connect(self):
        return pyodbc.connect('yaha')

    def create(self):
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("""
                    if not exists (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'bfx' AND TABLE_NAME='candles')
                    begin
                        create table bfx.candles (
                            symbol nvarchar(10) NOT NULL,
                            [time] datetime NOT NULL,
                            [open] decimal(18,10) NOT NULL,
                            [close] decimal(18,10) NOT NULL,
                            high decimal(18,10) NOT NULL,
                            low decimal(18,10) NOT NULL,
                            volume decimal(18,10) NOT NULL,
                            PRIMARY KEY (symbol, time) WITH (IGNORE_DUP_KEY = ON)
                        )
                    end
                """)

                cur.execute("""
                    if not exists (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'bfx' AND TABLE_NAME='fundings')
                    begin
                        create table bfx.fundings (
                            symbol nvarchar(10) NOT NULL,
                            id int  NOT NULL,
                            time DATETIME NOT NULL,
                            amount decimal(18,10) NOT NULL,
                            rate decimal(18,10) NOT NULL,
                            period smallint NOT NULL,
                        )
                        alter table bfx.fundings add constraint PK_BfxFundings  primary key nonclustered (Id)
                        alter table bfx.fundings rebuild with (IGNORE_DUP_KEY = ON)
                        create clustered index Idx_Bfx_Fundings_SymTime on bfx.fundings(Symbol,time)
                    end
                """)

                cur.execute("""
                    if not exists (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'bfx' AND TABLE_NAME='tradings')
                    begin
                        create table bfx.tradings (
                            symbol nvarchar(10) NOT NULL,
                            id int  NOT NULL,
                            time DATETIME NOT NULL,
                            amount decimal(18,10) NOT NULL,
                            price decimal(18,10) NOT NULL
                        )
                        alter table bfx.tradings add constraint PK_Tradings primary key nonclustered (Id)
                        alter table bfx.tradings rebuild with (IGNORE_DUP_KEY = ON)
                        create clustered index Idx_Bfx_Tradings_SymTime on bfx.tradings(Symbol,time)
                    end
                """)

        con.close()

    def insert_candles(self, symbol, candles):
        # def candle_interator():
        #     for candle in candles:
        #         candle[0] = pendulum.from_timestamp(candle[0]/1000)
        #         candle.insert(0, symbol)
        #         yield candle
        
        args_str = ','.join([f"('{symbol}', '{pendulum.from_timestamp(candle[0]/1000).format('%Y-%m-%dT%H:%M:%SZ')}', {candle[1]}, {candle[2]}, {candle[3]}, {candle[4]}, {candle[5]})" for candle in candles]) 
        with self.connect() as con:
            with con.cursor() as cur:
                # Even ignore_dup_key is on, pyodbc will still throw exception for 'Duplicate key was ignore' events
                try:
                    cur.execute('insert into bfx.candles values' + args_str)
                except pyodbc.IntegrityError:
                    pass
                
        con.close()

    def insert_funding_trades(self, symbol, trades):
        args_str = ','.join([f"('{symbol}', {trade[0]}, '{pendulum.from_timestamp(trade[1]/1000).format('%Y-%m-%dT%H:%M:%SZ')}', {trade[2]}, {trade[3]}, {trade[4]})" for trade in trades]) 

        with self.connect() as con:
            with con.cursor() as cur:
                try:
                    cur.execute('insert into bfx.fundings(symbol, id, time, amount, rate, period) values' + args_str)
                except pyodbc.IntegrityError:
                    pass
                
        con.close()

    def insert_trades(self, symbol, trades):
        args_str = ','.join([f"('{symbol}', {trade[0]}, '{pendulum.from_timestamp(trade[1]/1000).format('%Y-%m-%dT%H:%M:%SZ')}', {trade[2]}, {trade[3]})" for trade in trades]) 

        with self.connect() as con:
            with con.cursor() as cur:
                try:
                    cur.execute('insert into bfx.tradings(symbol, id, time, amount, price) values' + args_str)
                except pyodbc.IntegrityError:
                    pass
                
        con.close()

    def get_latest_candle_date(self, symbol):
        """
        Get the time of the most recent candle for a symbol
        """
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute('select max(time) from bfx.candles where symbol=?',
                                    (symbol))
                result = cur.fetchone()[0]
                if result is None:
                    return
                else:
                    return pendulum.instance(result)
                
        con.close()

    def get_latest_funding_date(self, symbol):
        """
        Get the time of the most recent funding for a symbol
        """
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute('select max(time) from bfx.fundings where symbol=?',
                                    (symbol))
                result = cur.fetchone()[0]
                if result is None:
                    return
                else:
                    return pendulum.instance(result)
                
        con.close()

    def get_latest_trading_date(self, symbol):
        """
        Get the time of the most recent trading for a symbol
        """
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute('select max(time) from bfx.tradings where symbol=?',
                                    (symbol))
                result = cur.fetchone()[0]
                if result is None:
                    return
                else:
                    return pendulum.instance(result)
                
        con.close()