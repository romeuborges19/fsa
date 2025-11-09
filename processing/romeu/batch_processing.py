from datetime import datetime
import json
from multiprocessing import Process
import os
from typing import TypedDict
import pandas as pd
from openai import BadRequestError, NotFoundError, OpenAI
from decouple import config
from openai.lib._parsing._completions import type_to_response_format_param
from sqlalchemy import delete, null, select
from dto import RespostaLLM
from config import SYSTEM_PROMPT, USER_PROMPT
from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import NoResultFound, MultipleResultsFound
from database import sqlite_engine, BatchLog
from logs import get_logger

response_format = type_to_response_format_param(RespostaLLM)


Session = sessionmaker(bind=sqlite_engine)
session = Session()

CAMINHO_NOTICIAS = "data/news/"
NUM_WORKERS = 16
MAX_BATCHES = 15
MINIMUM_BATCHES = 10

logger = get_logger()


class RetornoBatch(TypedDict):
    ticker: str | None
    sub_id: int | None
    batch_id: str | None
    file_name: str | None
    file_id: str | None
    should_retry: bool | None


def get_or_upload_batch_file(
    client: OpenAI, ticker: str, id: int, batch_file_name: str
):
    """Cria as batch files e salva seu registro no banco de dados."""
    stmt = select(BatchLog).where(BatchLog.ticker == ticker, BatchLog.sub_id == id)

    try:
        batch_log = session.execute(stmt).scalar_one()
    except NoResultFound:
        batch_log = None
    except MultipleResultsFound:
        delete_stmt = delete(BatchLog).where(
            BatchLog.ticker == ticker,
            BatchLog.sub_id == id,
            BatchLog.should_retry == True,
        )
        session.execute(delete_stmt)
        batch_log = None

    if not batch_log:
        batch_log = BatchLog(
            ticker=ticker,
            sub_id=str(id),
            should_retry=True,
        )
        logger.info(f"{batch_log.sub_id} = {id} = {str(id)}")
        session.add(batch_log)

    if batch_log.file_id:
        try:
            stored_file = client.files.retrieve(batch_log.file_id)
            batch_log.file_id = stored_file.id
        except NotFoundError:
            batch_log.file_id = None

    if not batch_log.file_id:
        try:
            batch_file = client.files.create(
                file=open(batch_file_name, "rb"), purpose="batch"
            )
        except Exception as e:
            logger.error(f"Erro ao subir arquivo para OpenAI. ticker={ticker} erro={e}")
            return

        logger.info(
            f"ticker={ticker} msg=arquivo_criado nome={batch_file.filename} file_id={batch_file.id}"
        )
        batch_log.file_id = batch_file.id
        batch_log.file_name = batch_file.filename

    session.commit()


def create_jsonl(ticker, sub_id, batch_tasks: list[dict]):
    batch_file_name = get_batch_filename(ticker, sub_id)

    if not os.path.isfile(batch_file_name):
        with open(batch_file_name, "w+") as f:
            for task in batch_tasks:
                f.write(json.dumps(task) + "\n")

    return batch_file_name


def upload_files_to_openai(client: OpenAI, ticker: str, all_tasks: list[list[dict]]):
    for id, batch_tasks in enumerate(all_tasks):
        sub_id = id + 1

        batch_file_name = create_jsonl(ticker, sub_id, batch_tasks)
        get_or_upload_batch_file(client, ticker, sub_id, batch_file_name)


def get_batch_filename(ticker, sub_id):
    return f"data/batches/batch_tasks_{ticker}_{sub_id}.jsonl"


def get_batch_tasks_from_data(
    client: OpenAI, ticker: str, data: list[dict]
) -> list[dict] | None:
    all_tasks = []
    tasks = []
    per_batch_limit = 100
    print("tamanho", len(data))

    for id, item in enumerate(data):
        aux_id = id + 1
        if (aux_id % per_batch_limit) == 0:
            all_tasks.append(tasks)
            tasks = []

        user_prompt = USER_PROMPT.format(
            data=item["date"], noticia=item["title"].strip()
        )
        system_prompt = SYSTEM_PROMPT.format(ticker=ticker)

        task = {
            "custom_id": f"task_req_{ticker}_{id}-{item['hash_id']}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-5-nano",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": user_prompt,
                    },
                ],
                "response_format": response_format,
            },
        }
        tasks.append(task)

    if tasks:
        all_tasks.append(tasks)

    upload_files_to_openai(client, ticker, all_tasks)


def create_batch_files(client, arquivos_noticias, uploaded_files):
    logger.info(f"arquivos_salvos_openai={len(uploaded_files)}")

    processos = []
    quantidade_noticias = len(arquivos_noticias)

    for pos, arquivo in enumerate(arquivos_noticias):
        ticker = arquivo.split(".")[0]
        caminho = f"{CAMINHO_NOTICIAS}{ticker}.parquet"
        data = pd.read_parquet(caminho).to_dict("records")
        print(len(data))

        p = Process(
            target=get_batch_tasks_from_data,
            args=(client, ticker, data),
        )
        p.start()

        processos.append(p)
        if len(processos) == NUM_WORKERS or pos == quantidade_noticias - 1:
            for p in processos:
                p.join()
            processos = []


def send_request(client, log: BatchLog):
    try:
        batch_job = client.batches.create(
            input_file_id=log.file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        if batch_job.status == "failed":
            logger.info(f"ticker={log.ticker} status=job_failed")  # Olhar no dashboard
            return None

        return batch_job.id
    except Exception as err:
        logger.info(
            f"ticker={log.ticker} status=erro_solicitar_processamento erro={err}"
        )
        return None


def solicitar_para_log(client: OpenAI, log: BatchLog):
    batch_id = send_request(client, log)
    log.batch_id = batch_id
    session.commit()
    logger.info(
        f"ticker={log.ticker} status=solicitacao_sucesso batch_id={log.batch_id}"
    )


def solicitar_processamento(client: OpenAI, log: BatchLog):
    if not log.batch_id:
        solicitar_para_log(client, log)
        return

    try:
        batch = client.batches.retrieve(log.batch_id)
    except BadRequestError as err:
        logger.error(f"ticker={log.ticker} status=erro_ao_solicitar erro={err}")
        return

    if batch.errors:
        erros = batch.errors.data or []
        for erro in erros:
            if erro.code == "invalid_type":
                logger.info(f"ticker={log.ticker} erro=tipo_invalido status=pulando")

                log.should_retry = False
                log.batch_id = f"{log.batch_id}_invalid"
                session.commit()
                return

    if batch.status == "completed":
        logger.info(f"ticker={log.ticker} status=finalizado")
        log.should_retry = False
        session.commit()
        return

    if log.should_retry:
        solicitar_para_log(client, log)


def deve_solicitar(client: OpenAI):
    batches = []
    cursor = None
    while True:
        response = client.batches.list(limit=100, after=cursor)

        for batch in response.data:
            batches.append(batch)

        if not response.has_more:
            break

        cursor = response.data[-1].id

    em_andamento = len(
        [
            b
            for b in batches
            if b.status
            in [
                "validating",
                "in_progress",
                "finalizing",
                "cancelling",
            ]
        ]
    )
    logger.info(f"== {em_andamento} processos em andamento ==")
    if em_andamento >= MINIMUM_BATCHES:
        return False
    return True


def processar_acoes():
    logger.info("Iniciando job")
    client = OpenAI(api_key=config("OPENAI_API_KEY"))

    arquivos_noticias = os.listdir(CAMINHO_NOTICIAS)
    uploaded_files = client.files.list().data
    uploaded_files = {f.filename: f.id for f in uploaded_files}

    create_batch_files(client, arquivos_noticias, uploaded_files)

    if not deve_solicitar(client):
        return

    logger.info("== Solicitando ==")

    stmt = (
        select(BatchLog)
        .where(
            ((BatchLog.should_retry == True) & ~(BatchLog.batch_id.endswith("invalid")))
            | (BatchLog.batch_id.is_(null()))
        )
        .order_by(BatchLog.id.asc())
    )
    batch_logs = session.execute(stmt).scalars().all()

    processos = []
    quantidade_logs = len(batch_logs)

    for pos, log in enumerate(batch_logs):
        if pos > MAX_BATCHES:
            break

        p = Process(
            target=solicitar_processamento,
            args=(client, log),
        )
        p.start()

        processos.append(p)
        if (
            len(processos) == NUM_WORKERS
            or pos == quantidade_logs - 1
            or pos == MAX_BATCHES - 1
        ):
            for p in processos:
                p.join()
            processos = []


scheduler = BlockingScheduler()
scheduler.add_job(processar_acoes, "date", run_date=datetime.now())
scheduler.add_job(processar_acoes, "interval", minutes=5)


def main():
    logger.info("Iniciando scheduler")
    scheduler.start()


if __name__ == "__main__":
    main()
