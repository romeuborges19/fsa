import json

import requests

EXAME_API_URL = "https://bff.exame.com/api/xm/wp/v2/news?page={page}&per_page=25&_details=false&_fields=id,slug,date,link,title,excerpt&search={ticker}&order=desc"


def crawl(
    ticker: str, termo_pesquisa: str | None = None, num_paginas: int | None = None
):
    url, params = EXAME_API_URL.split("?")
    params = params.split("&")
    query_params = {p.split("=")[0]: p.split("=")[1] for p in params}
    query_params["search"] = termo_pesquisa or ticker

    responses = []

    for pagina in range(1, 84):
        print(pagina)
        query_params["page"] = str(pagina)
        while True:
            res = requests.get(url, params=query_params)

            if res.status_code == 200:
                responses.append(res)
                break

    tudo = []

    for r in responses:
        data = json.loads(r.text)
        for item in data:
            tudo.append(item)

    return tudo
