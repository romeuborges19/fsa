import json

import requests

url_por_ticker = {
    "vale3": "https://falkor-cda.bastian.globo.com/tenants/valor/instances/a1f19bbb-854a-4be0-9f81-cc7760787fec/posts/page/{page}"
}


def crawl(
    ticker: str, termo_pesquisa: str | None = None, num_paginas: int | None = None
):
    responses = []
    url = url_por_ticker[ticker]

    for pagina in range(1, 84):
        print(pagina)
        retry = 0
        while True:
            res = requests.get(url.format(page=pagina))

            if res.status_code == 200:
                break

            retry += 1
            print(f"Tentando novamente: {retry}. {res.status_code}")

        res = json.loads(res.text)

        conteudo = [
            {
                "id": i.get("id"),
                "title": i.get("content").get("title"),
                "url": i.get("content").get("url"),
                "date": i.get("created"),
            }
            for i in res["items"]
        ]
        responses.extend(conteudo)

    return responses
