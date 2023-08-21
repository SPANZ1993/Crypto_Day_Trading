import pandas as pd

def ema_candlestick(data, span, func=lambda x: x.mean()):
    # Calculates an EMA from a pandas series
    # https://pandas.pydata.org/pandas-docs/version/0.17.0/generated/pandas.ewma.html
    # NOTE: It seems like in charting the closing price is usually used for EMA

    # data: Pandas Series
    # Span: Integer

    adjust = False
    return func(pd.Series.ewm(data, span, adjust=adjust))


def macd_candlestick(data, long_span=26, short_span=12, signal_span=9, func=lambda x: x.mean()):
    # https://www.investopedia.com/terms/m/macd.asp#:~:text=MACD%20is%20calculated%20by%20subtracting,the%20exponentially%20weighted%20moving%20average.

    # data: Pandas Series
    # long_span: Integer
    # short_span: Integer
    # signal_span: Integer

    long_span_ema = ema_candlestick(data, span=long_span)
    print(long_span_ema)
    short_span_ema = ema_candlestick(data, span=short_span)
    macd, signal = macd_from_ema_candlestick(long_span_ema, short_span_ema, signal_span)
    return macd, signal


def macd_from_ema_candlestick(long_span_ema, short_span_ema, signal_span=9):
    macd = short_span_ema - long_span_ema
    signal = ema_candlestick(macd, signal_span)
    return macd, signal