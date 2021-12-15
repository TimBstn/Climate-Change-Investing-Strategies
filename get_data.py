# import standard libraries
import pandas as pd
import datetime

# import finance libraries
import yfinance as yf
import bt

# Top 15 Climate Change Stocks
stocks_long = ['OEG', 'SUNW', 'CSIQ', 'BEP', 'PLUG', 'SEDG', 'ENPH', 'NEE',
               'TSLA', 'GE', 'GOOG', 'NVEE', 'SPWR', 'BLNK', 'FSLR']

# Worst 15 Climate Change Stocks
stocks_short = ['ADM', 'AES', 'PPL', 'DUK', 'FE', 'SO', 'BG', 'AEP', 'AEE', 'CEIX', 'CAG', 'NRG', 'BTU',
                'MO', 'XOM']

# Top 4 ESG Cryptos
cryptos = ['ADA-USD', 'XRP-USD', 'XTZ-USD', 'NANO-USD']


def get_stock_data(stocks, start='2019-01-01'):
    """
    :param stocks: List of stocks the data should be fetched for
    :needed packages: yfinance, pandas, datetime
    :return: returns an excel-file containing data about close prices, finances, earnings, cashflows and
    general information
    """
    # final DataFrames
    info_df = pd.DataFrame()
    close_df = pd.DataFrame()
    finance_df = pd.DataFrame()
    cash_df = pd.DataFrame()

    # loop over all stocks and get course data, info, financial data, balance sheet
    for stock in stocks:
        print(stock)
        ticker = yf.Ticker(stock)

        # info data
        info_dic = ticker.info
        stock_info = pd.DataFrame([info_dic]).T
        stock_info.index.name = 'Info'
        stock_info.columns = ['Value']
        stock_info['Stock'] = stock
        info_df = info_df.append(stock_info)

        # financial data
        fin = ticker.financials
        fin.columns = [(d - datetime.timedelta(15)).year for d in fin.columns]
        fin['Stock'] = stock
        fin['Position'] = fin.index
        fin = fin.melt(id_vars=["Stock", 'Position'], var_name="Year", value_name="Value")
        finance_df = finance_df.append(fin)

        # cashflow
        cash = ticker.cashflow
        cash.columns = [(d - datetime.timedelta(15)).year for d in cash.columns]
        cash['Stock'] = stock
        cash['Position'] = cash.index
        cash = cash.melt(id_vars=["Stock", 'Position'], var_name="Year", value_name="Value")
        cash_df = cash_df.append(cash)

        # course data
        hist = ticker.history(start=start)
        close = hist[['Close']]
        close['Stock'] = stock
        close_df = close_df.append(close)

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter('./data/stock_data.xlsx', engine='xlsxwriter')

    # Write each dataframe to a different worksheet.
    info_df.to_excel(writer, sheet_name='Info')
    close_df.to_excel(writer, sheet_name='Close')
    finance_df.to_excel(writer, sheet_name='Finance', index=False)
    cash_df.to_excel(writer, sheet_name='Cash', index=False)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()


def get_close(stocks, start="2019-01-01"):
    """
    :param stocks: List of stocks for which to create weights
    :param start: start date of strategy
    :return: returns DataFrame with Close data
    """
    close_df = pd.DataFrame()
    # loop over all stocks and get course data
    for stock in stocks:
        print(stock)
        ticker = yf.Ticker(stock)

        # course data
        hist = ticker.history(start=start)
        close = hist[['Close']]
        close.columns = [stock]
        close_df = close_df.join(close, how='outer')

    close_df = close_df.dropna(how='all')
    return close_df


def get_weights(df_long, df_short):
    """
    :param df_long: Return data of stocks to go long in
    :param df_short: Return data of stocks to go short in
    :param short: stocks should be shorted or not
    :return: DataFrame with weights of stocks
    """

    # create signal for long and short
    weights_long = df_long.notna().astype(int)
    weights_short = df_short.notna().astype(int) * -1

    # count how many stocks are available
    count_long = weights_long.gt(0).sum(axis=1)
    count_short = weights_short.lt(0).sum(axis=1)

    count = count_long + count_short

    # create weights
    weights = weights_long.join(weights_short, how='outer').div(count, axis=0)

    return weights


def backtest_strategy(df, name_strategy, weights=None):
    """
    :param df: DataFrame with price data for stocks
    :param name_strategy: name of the strategy
    :param weights: DataFrame with Weighting of the stocks given in df. Is used, when strategy is long and short or
                    nature
    :return: creates excel-file with price and stats data for a buy and hold strategy wit given stocks
    """

    if name_strategy in ['Long-Short', 'Nature']:
        # run strategy
        s = bt.Strategy(name_strategy, [bt.algos.WeighTarget(weights),
                                        bt.algos.Rebalance()])

    else:
        # run strategy
        s = bt.Strategy(name_strategy, [bt.algos.RunDaily(),
                                        bt.algos.SelectAll(),
                                        bt.algos.WeighEqually(),
                                        bt.algos.Rebalance()])

    # run  backtest
    backtest = bt.Backtest(s, df)
    res = bt.run(backtest)

    # create stats for backtests
    stats = res.stats

    # save prices for strategy in dataframe
    prices = pd.DataFrame({name_strategy: res.backtests[name_strategy].stats.prices})

    # create weights
    weights = res.backtests[name_strategy].weights

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(f'./data/backtest_data_{name_strategy}.xlsx', engine='xlsxwriter')

    # Write each dataframe to a different worksheet.
    stats.to_excel(writer, sheet_name='Stats')
    prices.to_excel(writer, sheet_name='Prices')
    weights.to_excel(writer, sheet_name='Weights')

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()


def nature_strategy(df):
    # load data with nature disasters
    nature_df = pd.read_excel('./data/Nature_disasters.xlsx')

    # investing period = 5 days
    nature_df['Investing_End'] = nature_df['Begin Date'] + datetime.timedelta(days=4)
    nature_df = nature_df[['Begin Date', 'Investing_End']]

    # create DataFrame with all dates you should be long
    investment_days = []
    for index, row in nature_df.iterrows():
        investment_days.extend(pd.date_range(start=row['Begin Date'], end=row['Investing_End']))

    investment_days = set(investment_days)
    investment_days = pd.DataFrame(investment_days, columns=['Date'])
    investment_days['Weight'] = 1
    investment_days.index = investment_days['Date']

    # create signal for stocks
    weights = df.notna().astype(int)

    # count number of stocks and create weight
    count = weights.gt(0).sum(axis=1)
    weights = weights.div(count, axis=0)

    # go long on days of nature disaster
    weights = weights.join(investment_days)
    weights.loc[:, :][weights.loc[:, 'Weight'] != 1] = 0
    weights = weights.drop(['Date', 'Weight'], axis=1)
    return weights


#### Create stock data
# get_stock_data(stocks_long)

#### Create Backtest data

# long data
# df_long = get_close(stocks_long)

# short data
# df_short = get_close(stocks_short)

# combine long and short data
# df_long_short = df_long.join(df_short, how='outer')

# weights
# weights = get_weights(df_long, df_short)

# long only strategy
# backtest_strategy(df_long, 'Long only')

# Long Short strategy
# backtest_strategy(df_long_short, 'Long-Short', weights=weights)

# MSCI world
# msci = get_close(['URTH'])
# backtest_strategy(msci, 'MSCI World')

# ESG Leaders ETF
# esg = get_close(['VSGX'])
# backtest_strategy(esg, 'ESG-ETF')

# Crypto ESG
# crypto = get_close(cryptos)
# backtest_strategy(crypto, 'Crypto')

# Nature Strategy
# weights_nature = nature_strategy(df_long)
# backtest_strategy(df_long, 'Nature', weights=weights_nature)
