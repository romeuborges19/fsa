import os
import pandas as pd
import argparse
from agno.agent import Agent
from agno.models.openai import OpenAIChat
from decouple import config
from pydantic import BaseModel

from pandarallel import pandarallel
from agno.exceptions import ModelProviderError
from logs import get_logger
from config import SYSTEM_PROMPT, USER_PROMPT
from dto import RespostaLLM

logger = get_logger()

API_KEY: str = config("OPENAI_API_KEY")


class ModelWrapper:
    def __init__(self, ticker: str) -> None:
        self.agent = Agent(
            model=OpenAIChat("gpt-5-nano", api_key=API_KEY),
            markdown=True,
            response_model=RespostaLLM,
            instructions=[SYSTEM_PROMPT.format(ticker=ticker)],
        )

    def coletar(self, x):
        gerado = USER_PROMPT.format(data=x["date"], noticia=x["content"])
        try:
            message = self.agent.run(gerado).content
        except ModelProviderError:
            return 429

        if isinstance(message, BaseModel):
            return message.model_dump()

        return message

    def tentar_novamente(self, x):
        if x["resposta"] == 429:
            return self.coletar(x)
        return x["resposta"]


pandarallel.initialize(nb_workers=16)


def extract_model_response(df: pd.DataFrame):
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize("America/Sao_Paulo")

    def extrair_resposta(x):
        aux = x["resposta"]
        return aux["decisao"]

    df["decisao"] = df.apply(extrair_resposta, axis=1)

    def extrair_resposta(x):
        aux = x["resposta"]
        return aux["motivo"]

    df["motivo"] = df.apply(extrair_resposta, axis=1)
    return df


def get_df_decisao_moda(df: pd.DataFrame):
    df["date"] = df["date"].apply(lambda x: x.date())
    df["date"] = pd.to_datetime(df["date"])
    df["date"] = df["date"].dt.tz_localize("America/Sao_Paulo")
    com_decisao = (
        df.groupby(by=["date", "decisao"])["decisao"]
        .value_counts()
        .groupby(level=0)
        .idxmax()
        .reset_index()
    )

    com_decisao["posicao"] = com_decisao["count"].apply(lambda x: x[1])
    return com_decisao.drop(columns=["count"])


def agregar_decisao_por_dia(df: pd.DataFrame):
    df = df[df.decisao.isnull() == False]
    com_decisao = (
        df.groupby(by=["date", "decisao"])["bm25"]
        .sum()
        .groupby(level=0)
        .idxmax()
        .reset_index()
    )

    com_decisao["posicao"] = com_decisao["bm25"].apply(lambda x: x[1])
    return com_decisao.drop(columns=["bm25"])


def get_df_final(ticker: str, df: pd.DataFrame):
    ticker_data = pd.read_parquet(f"data/tickers2/{ticker}.parquet").reset_index()
    df_ticker = ticker_data.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "Close": "close",
            "High": "high",
            "Low": "low",
            "Volume": "volume",
            "Dividends": "dividends",
            "Stock Splits": "stock_splits",
        }
    )

    final = pd.merge(df_ticker, df, on="date", how="left")
    # final = final[final.date > "2017-09-12"]
    return final


def sucesso(df: pd.DataFrame) -> bool:
    return len(df[df.resposta == 429]) == 0


def analyze_news(ticker: str):
    # model = ModelWrapper(ticker)
    # logger.info(f" == Analisando ticker {ticker}")
    # data = pd.read_parquet(f"data/filtragem-llm/{ticker}.parquet").reset_index()
    # logger.info("Iniciando a consulta ao LLM")
    # data["resposta"] = data.parallel_apply(model.coletar, axis=1)
    #
    # if not sucesso(data):
    #     logger.info("Erros identificados. Tentando novamente")
    #     data["resposta"] = data.parallel_apply(model.tentar_novamente, axis=1)
    #
    # if not sucesso(data):
    #     caminho = f"data/strategies/{ticker}-erro.parquet"
    #
    #     total = len(data)
    #     registros_com_erro = len(data[data.resposta == 429])
    #     registros_com_sucesso = total - registros_com_erro
    #
    #     logger.info(
    #         f" == Não foi possível processar o ticker '{ticker}' sem erros. Tente novamente.\n"
    #         f"total={total} "
    #         f"registros_com_sucesso={registros_com_sucesso} "
    #         f"registros_com_erro={registros_com_erro}\n"
    #         f"Dados salvos em '{caminho}'."
    #     )
    #     data.to_parquet(caminho)
    #     return
    #
    # aux = extract_model_response(data)
    logger.info("Agregando decisões por dia")
    aux = pd.read_parquet(f"data/strategies2/{ticker}-completo.parquet")

    logger.info("Respostas extraídas com sucesso")

    # com_decisao = agregar_decisao_por_dia(aux)
    com_decisao = get_df_decisao_moda(aux)
    df = get_df_final(ticker, com_decisao)
    caminho = f"data/strategies2/{ticker}.parquet"

    df.to_parquet(caminho)
    logger.info(f" == Processo finalizado com sucesso. Dados salvos em '{caminho}'.")


parser = argparse.ArgumentParser()
parser.add_argument("--ticker", dest="ticker", type=str)

if __name__ == "__main__":
    args = parser.parse_args()
    ticker = args.ticker
    if ticker:
        analyze_news(ticker)
    else:
        tickers = os.listdir("data/news")
        for ticker in tickers:
            nome_ticker = ticker.split(".")[0]
            analyze_news(nome_ticker)
