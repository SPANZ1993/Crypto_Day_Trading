import pandas as pd

def calc_rsi(sig, span=14):
    '''
    Calculate RSI From a Signal
    span: The number of candles to calculate RSI over
    '''
    if not isinstance(sig, pd.Series):
        sig = pd.Series(sig)
    change = sig.diff()
    change.dropna(inplace=True)
    change_up = change.copy()
    change_down = change.copy()

    change_up[change_up < 0] = 0
    change_down[change_down > 0] = 0

    # Verify that we did not make any mistakes
    assert (change.equals(change_up + change_down))

    # Calculate the rolling average of average up and average down
    avg_up = change_up.rolling(span).mean()
    avg_down = change_down.rolling(span).mean().abs()

    rsi = 100 * avg_up / (avg_up + avg_down)

    return rsi