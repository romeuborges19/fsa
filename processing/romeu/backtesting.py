import os
from typing import TypedDict
import argparse
from matplotlib.gridspec import GridSpec
import pandas as pd
import matplotlib.pyplot as plt
import math
from logs import get_logger
from config import STRATEGY_PATH, GRAPH_PATH


class Position:
    LONG = "LONG"
    SHORT = "SHORT"
    UNKNOWN = "UNKNOWN"


class Entry(TypedDict):
    open: float
    close: float
    posicao: str


logger = get_logger()


def plotar_retornos(ticker: str, df: pd.DataFrame):
    df = df.set_index(["date"])

    mask_long = df["mudanca"] == "LONG"
    mask_short = df["mudanca"] == "SHORT"

    df_long = df[mask_long]
    df_short = df[mask_short]

    fig = plt.figure(figsize=(18, 10), dpi=140)
    gs = GridSpec(2, 1, height_ratios=[1.5, 0.5])
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[1, 0])

    ax1.plot(df.index, df["dinheiro"], color="#377EB8", lw=2)
    ax1.plot(df.index, df["bnh"], color="#2C3E50", ls="--", lw=2)
    ax1.scatter(df_long.index, df_long["dinheiro"], s=50, zorder=5, color="#4DAF4A")
    ax1.scatter(df_short.index, df_short["dinheiro"], s=50, zorder=5, color="#E74C3C")

    def get_color(x):
        if x["retornos"] >= 0:
            return "#4DAF4A"
        return "#E74C3C"

    df["color"] = df.apply(get_color, axis=1)
    ax2.bar(df.index, df["retornos"], color=df["color"])

    ax1.spines["right"].set_visible(False)
    ax1.spines["top"].set_visible(False)
    ax1.set_ylabel("Retorno (R$)")
    ax1.set_xlabel("Data")

    ax2.spines["right"].set_visible(False)
    ax2.spines["top"].set_visible(False)
    ax2.set_ylabel("Retorno (%)")
    ax2.set_xlabel("Data")

    plt.tight_layout()
    fig.savefig(f"{GRAPH_PATH}/{ticker}.png")


def get_amount_of_shares(balance: float, open: float):
    shares = math.floor(balance / open)
    invested_value = shares * open
    return shares, invested_value


def backtest(ticker: str):
    df = pd.read_parquet(f"{STRATEGY_PATH}/{ticker}.parquet")
    data: list[Entry] = df.to_dict("records")

    valor_inicial = 1000
    balance = valor_inicial
    print(f"Valor inicial: {balance}")

    first_day = data[0]
    current_position = first_day["posicao"]
    current_shares, invested_value = (
        get_amount_of_shares(balance, first_day["open"])
        if current_position == "LONG"
        else (0, 0)
    )

    balance -= invested_value

    buy_n_hold = balance + invested_value
    dinheiro = []
    bnh = []
    retornos = [0]
    mudanca = [current_position]
    posicoes = [current_position]

    dinheiro.append(balance + invested_value)
    bnh.append(buy_n_hold)

    for entry in data[1:]:
        todays_return = entry["open"] / entry["close"]
        buy_n_hold = buy_n_hold * todays_return
        mudanca_posicao = None

        if current_position == Position.LONG:
            invested_value = invested_value * todays_return

        if entry["posicao"] == Position.SHORT and current_position in [Position.LONG]:
            balance += invested_value
            invested_value = 0
            current_shares = 0
            mudanca_posicao = entry["posicao"]

        if entry["posicao"] == Position.LONG and current_position in [
            Position.SHORT,
        ]:
            current_shares, invested_value = get_amount_of_shares(
                balance, entry["open"]
            )
            balance -= invested_value
            mudanca_posicao = entry["posicao"]

        dinheiro.append(balance + invested_value)
        bnh.append(buy_n_hold)
        mudanca.append(mudanca_posicao)

        retornos.append((dinheiro[-1] / valor_inicial) - 1)

        if nova_posicao := entry.get("posicao"):
            if nova_posicao != Position.UNKNOWN:
                current_position = entry["posicao"].strip()
        posicoes.append(current_position)

    balance += invested_value

    logger.info(
        f"ticker={ticker} valor_inicial={valor_inicial} valor_final={balance} buy_and_hold={buy_n_hold}"
    )

    df["dinheiro"] = dinheiro
    df["bnh"] = bnh
    df["mudanca"] = mudanca
    df["retornos"] = retornos
    df["retornos"] = 100 * df["retornos"]

    df.to_parquet(f"{STRATEGY_PATH}/{ticker}-resultado.parquet")
    plotar_retornos(ticker, df)


parser = argparse.ArgumentParser()
parser.add_argument("--ticker", dest="ticker", type=str)
if __name__ == "__main__":
    args = parser.parse_args()
    ticker = args.ticker
    if not ticker:
        tickers = os.listdir(STRATEGY_PATH)
        for ticker_file in tickers:
            if "-" in ticker_file:
                continue

            ticker = ticker_file.split(".")[0]
            backtest(ticker)
    else:
        backtest(ticker)
