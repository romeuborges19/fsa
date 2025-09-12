import math
import argparse
import os
import datetime
from time import sleep
import warnings
import pandas as pd
from multiprocessing import Process
from numpy import ndarray
from sklearn.feature_extraction.text import TfidfVectorizer
from pandarallel import pandarallel
from logs import get_logger
from utils import is_weekend, pre_processing
from sklearn.feature_selection import SelectKBest, f_classif
from database import engine
from scipy.stats import zscore

from typing import Callable
from sklearn.exceptions import DataConversionWarning
from pandas.errors import SettingWithCopyWarning

warnings.filterwarnings(action="ignore", category=DataConversionWarning)
warnings.filterwarnings(action="ignore", category=SettingWithCopyWarning)


B_PARAM = 0.75
NOTICIAS_POR_DIA = 10

BEGIN_DATE = "2016-08-01"
END_DATE = "2018-05-07"

QUERY = f"""
SELECT date, content, source
FROM juvenal_news
WHERE date BETWEEN '{BEGIN_DATE}' AND '{END_DATE}'
ORDER BY date
"""


logger = get_logger()


class ProcessSetup:
    def __init__(self):
        self.vectorizer = TfidfVectorizer()
        self.root_folder = "data/treinamento-juvenal/"
        self.setup()

    def setup(self):
        """Prepara os dados iniciais para processamento."""

        logger.info("Iniciando processo...")

        df = pd.read_sql(QUERY, con=engine)
        df["duplicado"] = df["content"].duplicated(keep="first")
        df = df[df.duplicado == False]
        df["date"] = pd.to_datetime(df["date"])

        pandarallel.initialize()
        logger.info("Iniciando tratamento inicial do conteúdo")

        df["content_tratado"] = df["content"].parallel_apply(pre_processing)
        df["weekend"] = df["date"].parallel_apply(is_weekend)

        self.df = df[df.weekend == False]
        self.set_X_total()
        self.set_subespacos()

    def set_subespacos(self):
        """Armazena o subespaço ideal de cada ticker no estado interno da instância"""

        subespacos = pd.read_parquet("data/4-subespaco-ideal.parquet")
        self.subespacos = {
            s["ticker"]: s["subspace"] for s in subespacos.to_dict("records")
        }

    def set_X_total(self):
        """Armazena os dados vetorizados do experimento do Juvenal no estado interno da instância"""

        logger.info("Vetorizando os dados originais")
        df_tokens = pd.read_parquet("data/2-filtrado.parquet")
        self.X_total = self.vectorizer.fit_transform(df_tokens["content"])
        self.total_features = self.X_total.shape[1]


class FilteringMethod:
    @classmethod
    def filtragem_por_zscore(cls, df: pd.DataFrame):
        df["bm25_zscore"] = zscore(df["bm25"])
        return df[df.bm25_zscore >= 1.5]

    @classmethod
    def filtragem_por_fonte(cls, df: pd.DataFrame):
        sources = df.source.unique()
        result = pd.DataFrame()
        for source in sources:
            df_source = df[df.source == source]
            tam = int(len(df_source) * 0.1)

            df_source = df_source.sort_values("bm25", ascending=False)[:tam]
            result = pd.concat([result, df_source])

        return result.sort_values("date")

    @classmethod
    def filtragem_por_data(cls, df: pd.DataFrame):
        result = pd.DataFrame()
        dates = df.date.unique()
        for date in dates:
            df_date = df[df.date == date]
            tam = math.ceil(len(df_date) * 0.01)
            # tam = NOTICIAS_POR_DIA if tam < NOTICIAS_POR_DIA else tam
            df_date = df_date.sort_values("bm25", ascending=False)[:tam]
            result = pd.concat([result, df_date])

        return result.sort_values("date")


stats = []


class ProcessNews:
    def __init__(self, setup: ProcessSetup, method_class, method: str = "zscore"):
        self.setup = setup
        self.method = method
        self.methods = method_class

    def feature_selection_k(self, percentage):
        """Retorna a porcentagem de features que deve ser selecionada"""
        return int(self.setup.total_features * percentage)

    def get_ticker_data(self, path):
        """Retorna os rótulos definidos pelo Gaussian Naive-Bayes no experimento do Juvenal"""
        # TODO: aplicar a filtragem por data logo aqui
        df_ticker = pd.read_parquet(path)
        y = df_ticker.label.to_numpy()
        return y.reshape(-1, 1)

    def get_vocabulario_importante(self, y: ndarray, porcentagem: float):
        """Seleciona o vocabulário importante dado uma lista de rótulos `y`
        e uma porcentagem de features `porcentagem`."""

        k = self.feature_selection_k(porcentagem)
        selector = SelectKBest(f_classif, k=k)

        _ = selector.fit_transform(self.setup.X_total, y)
        indices = selector.get_support(indices=True)
        return [
            p
            for p in self.setup.vectorizer.get_feature_names_out()[indices]
            if not p.isdigit()
        ]

    def calcular_score(self, target: pd.DataFrame, vocabulary: list[str]):
        """Retorna o score para cada registro em `target`, dado um vocabulário
        `vocabulary` de palavras importantes para o ticker.

        O score é o somatório das frequências de cada token encontrado no target."""

        individual_vectorizer = TfidfVectorizer(vocabulary=vocabulary)
        matrix = individual_vectorizer.fit_transform(target["content_tratado"])
        return matrix.sum(axis=1).A1

    def _get_filtragem(self) -> Callable:
        return getattr(FilteringMethod, self.method)

    def executar_filtragem(self, ticker: str):
        nome_ticker = ticker.split(".")[0].split("_")[-1]
        logger.info(f"Processando {nome_ticker.upper()}")

        df_noticias = self.setup.df.copy()

        y = self.get_ticker_data(self.setup.root_folder + ticker)

        porcentagem = self.setup.subespacos[nome_ticker] / 100
        palavras_importantes = self.get_vocabulario_importante(y, porcentagem)

        df_noticias["score"] = self.calcular_score(df_noticias, palavras_importantes)
        df_noticias["qtd_palavras"] = df_noticias["content"].apply(
            lambda x: len(x.strip().split(" "))
        )

        avgdl = df_noticias.qtd_palavras.mean()
        df_noticias["bm25"] = df_noticias["score"] / (
            (1 - B_PARAM) + B_PARAM * (df_noticias["qtd_palavras"] / avgdl)
        )

        df_llm = df_noticias[
            (df_noticias.bm25 != 0)
            & (df_noticias.qtd_palavras > 8)
            & (df_noticias.date >= "2017-09-13")
            & (df_noticias.date <= "2018-05-07")
        ]

        metodo_filtragem = self._get_filtragem()
        df_llm = metodo_filtragem(df_llm)

        df_llm.to_parquet(f"data/filtragem-llm/{nome_ticker}.parquet")
        logger.info(f"DataFrame resultante: {len(df_llm)} linhas")
        del df_llm

    def _processar_todos(self):
        tickers = os.listdir(self.setup.root_folder)
        processos = []
        for ticker in tickers:
            if "itsa4" in ticker or "cvcb3" in ticker:
                continue
            p = Process(target=self.executar_filtragem, args=(ticker,))
            p.start()

            processos.append(p)

            if len(processos) == 5:
                for p in processos:
                    p.join()
                processos = []

        logger.info("Sleeping...")
        sleep(10)
        stats = []
        for ticker in tickers:
            nome_ticker = ticker.split(".")[0].split("_")[-1]
            try:
                data = pd.read_parquet(f"data/filtragem-llm/{nome_ticker}.parquet")
            except FileNotFoundError:
                logger.error(
                    f"Não foi possível ler estatísticas do ticker {nome_ticker}."
                )
                continue

            contagem = data.groupby(by="date", as_index=False)["bm25"].count()

            stats_ticker = {
                "ticker": nome_ticker,
                "media": contagem["bm25"].mean(),
                "min": contagem["bm25"].min(),
                "max": contagem["bm25"].max(),
                "std": contagem["bm25"].std(),
            }
            stats.append(stats_ticker)

        df_stats = pd.DataFrame(stats)
        df_stats.to_parquet("data/stats.parquet")

    def run(self, ticker: str | None = None):
        start = datetime.datetime.now()
        if ticker:
            logger.info(f"== Processando ticker {ticker}")
            self.executar_filtragem(f"train_{ticker}.parquet", [])
        else:
            logger.info("== Processando todos os tickers")
            self._processar_todos()

        end = datetime.datetime.now()
        logger.info(f"== Tempo de processamento: {end - start}")


parser = argparse.ArgumentParser()
parser.add_argument("--ticker", dest="ticker", type=str)

if __name__ == "__main__":
    args = parser.parse_args()
    ticker = args.ticker
    logger.info("== Iniciando seleção de notícias")
    if ticker:
        logger.info(f"Ticker = {ticker}")

    start = datetime.datetime.now()
    setup = ProcessSetup()

    logger.info("== Setup finalizado")

    process = ProcessNews(
        setup,
        method_class=FilteringMethod,
        method="filtragem_por_data",
    )

    end = datetime.datetime.now()

    process.run(ticker)
    logger.info(f"== Tempo total: {end - start}")
