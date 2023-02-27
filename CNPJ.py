# AIRFLOW IMPORTS
from airflow.decorators import dag, task
import pendulum

# DAG IMPORTS
from requests import get as rget
from bs4 import BeautifulSoup
from datetime import datetime
from os import path, remove
import pickle
import zipfile
from glob import glob
import logging


DOWNLOAD_PATH = "/opt/airflow/data/cnpj/download/"
EXTRACTED_PATH = "/opt/airflow/data/cnpj/extracted/"
PICKEL_PATH = "/opt/airflow/data/cnpj/"
URL_CNPJ = "https://dadosabertos.rfb.gov.br/CNPJ/"


@task()
def get_files_date():
    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info("Pedido de requisição web iniciado")
    soup = BeautifulSoup(rget(URL_CNPJ).text, "html.parser")
    LOGGER.info("Pedido de requisição web concluído")
    date = list(
        set(
            list(
                map(
                    lambda x: x.find_all("td")[2].text.strip()[:-6],
                    filter(
                        lambda x: x.text.find("LAYOUT") == -1,
                        soup.find_all("tr")[3:-3],
                    ),
                )
            )
        )
    )[0]
    LOGGER.info("Parser HTML concluído")
    return date


@task()
def save_pickle_date(date):
    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info(f"Arquivo pickel gerado com a string {date}")
    with open(path.join(PICKEL_PATH, "last_date.pk"), "wb") as pk:
        pickle.dump(date, pk)


@task()
def load_pikle_date():
    LOGGER = logging.getLogger("airflow.task")
    pickle_path = path.join(PICKEL_PATH, "last_date.pk")
    if path.exists(pickle_path):
        LOGGER.info("Arquivo pickel encontrado")
        with open(pickle_path, "rb") as pk:
            last_date = pickle.load(pk)
            LOGGER.info("Data do arquivo pickle carregada")
        return last_date
    else:
        LOGGER.info("Arquivo pickle NÃO encontrado")
        save_pickle_date("1999-01-01")
        LOGGER.info('Arquivo pickle criado com a string "1999-01-01"')
        return "1999-01-01"


@task()
def is_newer(date1, date2):
    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info(
        f'Verificando se data do pickle "{date1}" é mais recente que data dos arquivos no site "{date2}"'
    )
    if (
        datetime.strptime(date1, "%Y-%m-%d").date()
        < datetime.strptime(date2, "%Y-%m-%d").date()
    ):
        LOGGER.info("Data do pickle é mais recente que a data dos arquivos no site")
        return True
    else:
        LOGGER.info("Data dos arquivos no site é mais recente que a data do pickle")
        return False


@task()
def get_files_url():
    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info("Pedido de requisição web iniciado")
    soup = BeautifulSoup(rget(URL_CNPJ).text, "html.parser")
    LOGGER.info("Pedido de requisição web concluído")
    urls = list(
        map(
            lambda x: path.join(URL_CNPJ, x.find("a")["href"]),
            filter(lambda x: x.text.find("LAYOUT") == -1, soup.find_all("tr")[3:-3]),
        )
    )
    LOGGER.info("Parser HTML concluído")
    return urls


@task()
def download_extract_delete_files(urls):
    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info("Download de arquivos iniciado")
    for url in urls:
        url = path.basename(url)
        download_path = path.abspath(path.join(DOWNLOAD_PATH, url))
        download_url = path.join(URL_CNPJ, url)
        LOGGER.info(f"Fazendo download {download_path} da URL: {download_url}")
        with open(download_path, "wb") as binary:
            binary.write(rget(download_url).content)
            LOGGER.info(f"Download concluído: {download_path}")
        with zipfile.ZipFile(download_path, "r") as zip_ref:
            LOGGER.info(
                f"Fazendo extração do arquivo {download_path} na pasta: {EXTRACTED_PATH}"
            )
            zip_ref.extractall(EXTRACTED_PATH)
            LOGGER.info(f"Extração concluída {download_path}")
        remove(download_path)
        LOGGER.info(f"Arquivo {download_path} excluído")


@dag(
    "CNPJ",
    start_date=pendulum.datetime(2023, 2, 27, tz="UTC"),
    schedule_interval="@monthly",
    tags=["Receita", "OSINT", "CNPJ"],
    catchup=False,
)
def cnpj():
    pickle_date = load_pikle_date()
    files_date = get_files_date()
    if is_newer(pickle_date, files_date):
        download_extract_delete_files(get_files_url())
        save_pickle_date(files_date)


cnpj()
