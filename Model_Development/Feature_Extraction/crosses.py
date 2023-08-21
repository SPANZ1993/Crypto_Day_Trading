import pandas as pd

def cross_above_flags(sigA,sigB):
    '''Return a boolean array denoting where signal A crosses signal B'''
    if isinstance(sigA, pd.Series):
        sigA = sigA.values
    if isinstance(sigB, pd.Series):
        sigB = sigB.values
    flags = []
    assert(len(sigA)==len(sigB))
    for i in range(len(sigA)):
        if i != 0 and (sigA[i-1] < sigB[i-1] and sigA[i] >= sigB[i]):
            flags.append(True)
        else:
            flags.append(False)
    return flags