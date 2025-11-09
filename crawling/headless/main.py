import hashlib

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from crawler import exame, valor

engine = create_engine("postgresql+psycopg2://postgres:changeme@localhost:5436/romeu")
session = sessionmaker(autocommit=False, autoflush=False, bind=engine)()

CAMPOS_DB = ["date", "url", "title", "content", "hash_id", "ticker"]
TICKER = "vale3"


def get_hash_id(url: str):
    encoded = url.encode("utf-8")
    hash_obj = hashlib.md5()
    hash_obj.update(encoded)
    return hash_obj.hexdigest()


def get_content_valor(url: str):
    response = requests.get(url)

    s = BeautifulSoup(response.text, "html.parser")
    texts = s.find_all("p", class_="content-text__container")
    content = []
    for t in texts:
        content.append(t.get_text())

    return " ".join(content).strip()


def get_content_exame(url: str):
    try:
        response = requests.get(url)
    except Exception:
        return None

    s = BeautifulSoup(response.text, "html.parser")
    content = []
    for text in s.find_all(
        "p",
        class_="m-0 p-0 xl:text-pretty body-extra-large overflow-hidden py-3 text-colors-text dark:text-colors-background lg:py-4",
    ):
        content.append(text.get_text())

    return " ".join(content).strip()


def main():
    noticias_valor = valor.crawl(TICKER)
    valor_df = pd.DataFrame(noticias_valor)

    noticias_exame = exame.crawl(TICKER)
    exame_df = pd.DataFrame(noticias_exame)

    valor_df = valor_df[valor_df.date >= "2024-03-15"]
    exame_df = exame_df[exame_df.date >= "2024-03-15"]

    valor_df["content"] = valor_df["url"].parallel_apply(get_content_valor)
    valor_df["date"] = pd.to_datetime(valor_df["date"]).dt.tz_convert(
        "America/Sao_Paulo"
    )
    valor_df["source"] = valor_df["url"].apply(
        lambda x: x.split("/")[2] if pd.notnull(x) else x
    )
    valor_df["hash_id"] = valor_df["url"].apply(get_hash_id)

    exame_df["content"] = exame_df["link"].parallel_apply(get_content_exame)
    exame_df = exame_df[~exame_df.content.isna()]
    exame_df = exame_df.rename(columns={"link": "url"})
    exame_df["date"] = pd.to_datetime(exame_df["date"]).dt.tz_localize(
        "America/Sao_Paulo"
    )
    exame_df["source"] = exame_df["url"].apply(
        lambda x: x.split("/")[2] if pd.notnull(x) else x
    )
    exame_df["ticker"] = TICKER
    valor_df["ticker"] = TICKER

    fontes_valor = valor_df["source"].unique()
    fontes_exame = exame_df["source"].unique()

    for f in [*fontes_valor, *fontes_exame]:
        query = text(f"DELETE FROM noticias WHERE url ILIKE 'https://{f}%'")
        session.execute(query)

    exame_db = exame_df[CAMPOS_DB]
    exame_db.to_sql("noticias", engine, if_exists="append", index=False)

    valor_db = valor_df[CAMPOS_DB]
    valor_db.to_sql("noticias", engine, if_exists="append", index=False)


if __name__ == "__main__":
    main()
