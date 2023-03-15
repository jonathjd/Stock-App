import sqlite3
import config
from alpaca_trade_api.rest import REST, TimeFrame
from datetime import date
import alpaca_trade_api as tradeapi

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
    SELECT id FROM strategy where name = 'opening_range_breakout'
""")

strategy_id = cursor.fetchone()['id']

cursor.execute("""
    SELECT symbol, name
    FROM stock
    JOIN stock_strategy on stock_strategy.stock_id = stock.id
    where stock_strategy.strategy_id = ?
""", (strategy_id,))

stocks = cursor.fetchall()
symbols = [stock['symbol'] for stock in stocks]

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.BASE_URL)
barsets = api.get_bars(symbols, TimeFrame.Minute, adjustment='raw').df

orders = api.list_orders()
existing_order_symbols = [order.symbol for order in orders]

current_date = date.today().isoformat()
start_minute_bar = f'{current_date} 09:30:00-4:00'
end_minute_bar = f'{current_date} 09:45:00-4:00'

for symbol in symbols:
    minute_bars = api.get_bars(symbol, TimeFrame.Minute, asof=current_date, adjustment='raw').df

    opening_range_mask = (minute_bars.index >= start_minute_bar) & (minute_bars.index < end_minute_bar)
    opening_range_bars = minute_bars.loc[opening_range_mask]

    try:
        opening_range_low = opening_range_bars['low'].min()
        opening_range_high = opening_range_bars['high'].max()
        opening_range = opening_range_high - opening_range_low

        after_opening_range_mask = minute_bars.index >= end_minute_bar
        after_opening_range_bars = minute_bars.loc[after_opening_range_mask]

        after_opening_range_breakout = after_opening_range_bars[after_opening_range_bars['close'] > opening_range_high]

        if not after_opening_range_breakout.empty:
            if symbol not in existing_order_symbols:
                limit_price = after_opening_range_breakout.iloc[0]['close']

                print(f'placing order for {symbol} at {limit_price}, closed above {opening_range_high}, at {after_opening_range_breakout.iloc[0]}')
                api.submit_order(
                    symbol=symbol,
                    side='buy',
                    type='limit',
                    qty=100,
                    time_in_force='day',
                    order_class='bracket',
                    limit_price=limit_price,
                    take_profit=dict(
                        limit_price=limit_price+opening_range,
                    ),
                    stop_loss=dict(
                        stop_price=limit_price-opening_range,
                    ),
                )
            else:
                print(f'Already an order for {symbol}, skipping')

    except Exception as e:
        print(e)







