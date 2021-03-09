import numpy as np
import pandas as pd

from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error

import seaborn as sns
import matplotlib.pyplot as plt


def excalibur(true, preds, title=None, suptitle=None):
    sns.set_style('darkgrid')
    fig = plt.figure(figsize=(10, 7), dpi=100, facecolor='w', edgecolor='k')
    # o = np.argsort(true.flatten())
    # points = zip(true[o], preds[o])

    # print(1)
    plt.xlim([min(true), max(true)])
    plt.ylim([min(true), max(true)])
    # print(2)

    # plot_true = np.array([true[i] for i in range(len(true)) if min(true) < preds[i] and max(true) > preds[i]])
    # plot_preds = np.array([preds[i] for i in range(len(preds)) if min(true) < preds[i] and max(true) > preds[i]])

    plot_mask = np.logical_and(min(true) < preds, max(true) > preds)
    plot_true = true[plot_mask]
    plot_preds = preds[plot_mask]

    # print(3)
    # print("ABOUT TO POLYFIT")
    z = np.polyfit(plot_true.flatten(), plot_preds.flatten(), 1)
    p = np.poly1d(z)
    plt.plot(plot_true, p(plot_true), "r--")
    # print(4)

    colors = MinMaxScaler().fit_transform(np.power(np.abs(true - preds).reshape((len(true), 1)), 0.4)).flatten()
    # print(5)
    plt.scatter(true, preds, s=0.25, c=colors)
    plt.xlabel('True Value')
    plt.ylabel('Predicted Value')
    if suptitle is not None:
        plt.suptitle(suptitle)
    if title is not None:
        plt.title(title)
    plt.viridis()
    plt.show()


if __name__ == '__main__':
    excalibur(np.array(range(10)), np.array(range(10)+np.random.random((10,))))