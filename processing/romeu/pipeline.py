import glob
import json
import math
import pandas as pd
import matplotlib.pyplot as plt

from matplotlib.gridspec import GridSpec

from typing import TypedDict
from config import CAMINHO_NOTICIAS, GRAPH_PATH, OUTPUT_PATH, STRATEGY_PATH
from logs import get_logger

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from database import sqlite_engine, BatchLog
from logs import get_logger
from openai import OpenAI
from decouple import config
from multiprocessing import Process
from config import OUTPUT_PATH

Session = sessionmaker(bind=sqlite_engine)
session = Session()

logger = get_logger()

NUM_WORKERS = 16


class Position:
    LONG = "LONG"
    SHORT = "SHORT"
    UNKNOWN = "UNKNOWN"


class Entry(TypedDict):
    open: float
    close: float
    posicao: str


class Pipeline:
    def _get_output_file(self, client: OpenAI, log: BatchLog):
        if not log.batch_id:
            logger.info(
                f"ticker={log.ticker} sub_id={log.sub_id} status=batch_id_indisponivel"
            )
            return

        try:
            batch = client.batches.retrieve(log.batch_id)
        except Exception:
            logger.info(
                f"ticker={log.ticker} sub_id={log.sub_id} status=erro_ao_consultar"
            )
            return

        if not batch.output_file_id:
            logger.info(
                f"ticker={log.ticker} sub_id={log.sub_id} status=output_indisponivel"
            )
            return

        output_content = client.files.content(batch.output_file_id).content

        output_filename = f"{OUTPUT_PATH}/output_{log.ticker}_{log.sub_id}.jsonl"
        with open(output_filename, "wb") as f:
            f.write(output_content)

        logger.info(
            f"ticker={log.ticker} sub_id={log.sub_id} status=sucesso path={output_filename}"
        )

    def collect(self):
        client = OpenAI(api_key=config("OPENAI_API_KEY"))
        stmt = select(BatchLog).where(~(BatchLog.batch_id.endswith("invalid")))
        result = session.execute(stmt)
        batch_logs = result.scalars().all()

        quantidade_batches = len(batch_logs)
        processos = []
        for pos, log in enumerate(batch_logs):
            p = Process(target=self._get_output_file, args=(client, log))
            p.start()

            processos.append(p)
            if len(processos) == NUM_WORKERS or pos == quantidade_batches - 1:
                for p in processos:
                    p.join()
                processos = []

        logger.info("Processo finalizado")

    def get_output_path(self, ticker: str):
        return f"{OUTPUT_PATH}/output_{ticker}*"

    def sort_method(self, file: str):
        file_name = file.split(".")[0]
        file_name = file_name.split("_")[-1]
        return int(file_name)

    def read_output(self, output_files: list[str]):
        output_files = sorted(output_files, key=self.sort_method)
        print(len(output_files))

        responses = []

        for file in output_files:
            print(f"lendo {file}")
            with open(file, "rb") as jsonl:
                data = list(jsonl)
                for item in data:
                    item_data = json.loads(item)
                    response = json.loads(
                        item_data["response"]["body"]["choices"][0]["message"][
                            "content"
                        ]
                    )
                    hash_id = item_data["custom_id"].split("-")[-1]
                    response["hash_id"] = hash_id
                    responses.append(response)

        return responses

    def reassamble_news(self, news_df: pd.DataFrame, ticker: str):
        output_files = glob.glob(self.get_output_path(ticker))
        output = self.read_output(output_files)
        logger.info(f"ticker={ticker} respostas={len(output)}")

        output_df = pd.DataFrame(output)
        news_df["date"].dt.tz_localize("America/Sao_Paulo")
        df = news_df.merge(output_df, on="hash_id")

        path = f"{STRATEGY_PATH}/{ticker}-completo.parquet"
        df.to_parquet(path)

        # decisoes = [o["decisao"] for o in output]
        # motivos = [o["motivo"] for o in output]
        # news_df["decisao"] = decisoes
        # news_df["motivos"] = motivos
        # path = f"{STRATEGY_PATH}/{ticker}-completo.parquet"
        #
        # news_df["date"] = news_df["date"].dt.tz_localize("America/Sao_Paulo")
        # news_df.to_parquet(path)

        logger.info(f"ticker={ticker} salvo em {path}")

    def plotar_retornos(self, ticker: str, df: pd.DataFrame):
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
        ax1.scatter(
            df_short.index, df_short["dinheiro"], s=50, zorder=5, color="#E74C3C"
        )

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

    def get_amount_of_shares(self, balance: float, open: float):
        shares = math.floor(balance / open)
        invested_value = shares * open
        return shares, invested_value

    def backtest(self, ticker: str):
        df = pd.read_parquet(f"{STRATEGY_PATH}/{ticker}.parquet")
        data: list[Entry] = df.to_dict("records")

        valor_inicial = 1000
        balance = valor_inicial
        print(f"Valor inicial: {balance}")

        first_day = data[0]
        current_position = first_day["posicao"]
        current_shares, invested_value = (
            self.get_amount_of_shares(balance, first_day["open"])
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

            if entry["posicao"] == Position.SHORT and current_position in [
                Position.LONG
            ]:
                balance += invested_value
                invested_value = 0
                current_shares = 0
                mudanca_posicao = entry["posicao"]

            if entry["posicao"] == Position.LONG and current_position in [
                Position.SHORT,
            ]:
                current_shares, invested_value = self.get_amount_of_shares(
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
        self.plotar_retornos(ticker, df)


def main():
    print("oi")
    p = Pipeline()
    p.collect()

    print("oi")
    news_df = pd.read_parquet(f"{CAMINHO_NOTICIAS}/vale3.parquet")
    p.reassamble_news(news_df, "vale3")
    p.backtest("vale3")


if __name__ == "__main__":
    main()
