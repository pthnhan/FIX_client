import pandas as pd

def convert_log_to_df(logfile):
    with open(logfile, 'r') as rf:
        log = rf.readlines()
    data = []
    for line in log:
        fix_message = line.split('8=FIX.4.2')[1].strip().split('\x01')
        row = {}
        for item in fix_message:
            if '=' in item:
                key = item.split('=')[0]
                value = item.split('=')[1]
                row[key] = value
        data.append(row)
    df = pd.DataFrame(data)
    df = df[['52', '37', '55', '54', '31', '32']]
    df = df.dropna()
    df = df.rename(columns={'52': 'datetime', '37': 'order_id', '55': 'symbol', '54': 'side', '31': 'match_price', '32': 'match_qty'})
    df = df.sort_values(by=['symbol', 'datetime'], ascending=[True, True])
    df.match_price = df.match_price.astype(float)
    df.match_qty = df.match_qty.astype(int)
    df = df.set_index('datetime')
    return df

def calculate_total_value_trading(df):
    print("\na) Total trading volume, in USD:")
    df['value'] = df['match_price'] * df['match_qty']
    total_value_trading = df.groupby('symbol')['value'].sum()
    for symbol in total_value_trading.index:
        print("Total trading value of {} is {:,} USD".format(symbol, round(total_value_trading[symbol], 2)))
    print("Total trading value of all symbols is {:,} USD".format(round(total_value_trading.sum(), 2)))

def calculate_pnl(df, fee = 0):
    print("\nb) PNL generated from this trading:")
    for symbol in df['symbol'].unique():
        df_symbol = df[df['symbol'] == symbol]
        open_price = df_symbol['match_price'].iloc[0]
        close_price = df_symbol['match_price'].iloc[-1]
        df_symbol_buy = df_symbol[df_symbol['side'] == '1']
        df_symbol_sell = df_symbol[df_symbol['side'] == '2']
        df_symbol_short = df_symbol[df_symbol['side'] == '5']
        pnl_buy = df_symbol_buy.match_qty * (close_price - df_symbol_buy.match_price) - fee * df_symbol_buy.match_qty
        pnl_sell = df_symbol_sell.match_qty * (df_symbol_sell.match_price - open_price) - fee * df_symbol_sell.match_qty
        pnl_short = df_symbol_short.match_qty * (df_symbol_short.match_price - close_price) - fee * df_symbol_short.match_qty
        print("PnL of {} is {:,} USD".format(symbol, round(pnl_buy.sum() + pnl_sell.sum() + pnl_short.sum(), 2)))

def calculate_vwap(df):
    print("\nc) VWAP of the fills for each instrument:")
    df['value'] = df['match_price'] * df['match_qty']
    df['cum_value'] = df.groupby('symbol')['value'].cumsum()
    df['cum_qty'] = df.groupby('symbol')['match_qty'].cumsum()
    df['vwap'] = df['cum_value'] / df['cum_qty']
    for symbol in df['symbol'].unique():
        df_symbol = df[df['symbol'] == symbol]
        print("VWAP of {} is {:,} USD".format(symbol, round(df_symbol['vwap'].iloc[-1], 2)))


if __name__ == '__main__':
    import os
    df = convert_log_to_df(f"my_results{os.sep}output{os.sep}log{os.sep}FIX.4.2-OPS_CANDIDATE_7_7595-DTL.messages.current.log")
    calculate_total_value_trading(df)
    calculate_pnl(df)
    calculate_vwap(df)