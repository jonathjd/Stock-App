import sqlite3, config
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import REST, TimeFrame
from datetime import datetime

connection = sqlite3.connect(config.DB_FILE)

connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
    SELECT id, symbol, name FROM stock WHERE symbol NOT LIKE ("%/%")
""")


rows = cursor.fetchall()

symbols = []
stock_dict = {}
for row in rows:
    symbol = row['symbol']
    symbols.append(symbol)
    stock_dict[symbol] = row['id']
    
api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.BASE_URL)

chunk_size = 200
for i in range(0, len(symbols), chunk_size):
    symbol_chunk = symbols[i:i+chunk_size]

    barsets = api.get_bars(symbol_chunk, TimeFrame.Day, adjustment='raw')

    for symbol in barsets:
        print(f"processing symbol {symbol.S}")

        stock_id = stock_dict[symbol.S]
        cursor.execute("""
            INSERT INTO stock_price (stock_id, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (stock_id, symbol.t.date(), symbol.o, symbol.h, symbol.l, symbol.c, symbol.v))

connection.commit()

