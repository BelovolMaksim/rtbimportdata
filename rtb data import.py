from datetime import date, timedelta
import pandas as pd
import requests
import base64
from rtbhouse_sdk.client import BasicAuth, Client
from tabulate import tabulate
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os
from config import PASSWORD, USERNAME, GCP_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID

# Установите переменную окружения GOOGLE_APPLICATION_CREDENTIALS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/maksym_bilovol/python_scripts/key.json"

def upload_data_to_bigquery(data_frame):
    # Инициализация клиента BigQuery
    client = bigquery.Client(project=GCP_PROJECT_ID)

    # Получение ссылки на таблицу в BigQuery
    dataset_ref = client.dataset(BQ_DATASET_ID)
    table_ref = dataset_ref.table(BQ_TABLE_ID)

    # Проверка существования таблицы
    try:
        table = client.get_table(table_ref)
    except NotFound:
        # Если таблицы нет, создаем её
        schema = [bigquery.SchemaField(column, "STRING") for column in data_frame.columns]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)

    # Загрузка данных в таблицу
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True

    with open("data.csv", "w") as data_file:
        data_frame.to_csv(data_file, index=False)

    with open("data.csv", "rb") as data_file:
        job = client.load_table_from_file(data_file, table_ref, job_config=job_config)

    job.result()

def get_rtb_stats(advertiser_id, campaign_name, day_from, day_to, metrics):
    base_url = "https://api.panel.rtbhouse.com/v5/advertisers"
    url = f"{base_url}/{advertiser_id}/summary-stats"
    
    # Маппинг названий кампаний
    campaign_mapping = {
        "UA_Brocard_ru": 'ZRDkN',
        "UA_Brocard_ua": 'bByGR',
        "UA_Brocard_ukr_versus": 'WOlVv'
    }
    
    # Получение соответствующего campaign_id из маппинга
    campaign_id = campaign_mapping.get(campaign_name, campaign_name)
    
    # Обновленные параметры запроса
    params = {
        "dayFrom": day_from,
        "dayTo": day_to,
        "groupBy": "day",
        "metrics": "-".join(metrics),
        "subcampaigns": campaign_id,
        "countConvention": "ATTRIBUTED"  # Используем "ATTRIBUTED" как значениe countConvention
    }
    
    auth_header = f"{USERNAME}:{PASSWORD}"
    auth_header_encoded = base64.b64encode(auth_header.encode()).decode()
    
    headers = {
        "accept": "application/json",
        "Authorization": f"Basic {auth_header_encoded}"
    }

    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        rtbhouse_data = response.json()["data"]
        data_frame = pd.DataFrame(rtbhouse_data)
        data_frame["subcampaign"] = campaign_name
        return data_frame
    else:
        print(f"Ошибка при запросе для кампании {campaign_name}:")
        print(f"Статус кода: {response.status_code}")
        print(f"Текст ответа: {response.text}")
        return None

if __name__ == "__main__":
    with Client(auth=BasicAuth(USERNAME, PASSWORD)) as api:
        day_to = date.today() - timedelta(days=1)  # Вчерашний день
        day_from = day_to  # Тоже вчерашний день

        # Обновленный список метрик, объединенных через тире
        metrics = [
            "clicksCount-ctr",
            "impsCount",
            "campaignCost",
            "conversionsCount",
            "conversionsValue",
            "cr",
            "ecpa"
        ]

        campaign_info = [
            {"name": "UA_Brocard_ru"},
            {"name": "UA_Brocard_ua"},
            {"name": "UA_Brocard_ukr_versus"}
        ]

        advertiser_id = 'S2Ea7XTGF2A5norEF03q'

        data_frames = []

        for campaign in campaign_info:
            campaign_name = campaign["name"]
            print(f"Запрос данных для кампании {campaign_name}")
            df = get_rtb_stats(advertiser_id, campaign_name, day_from, day_to, metrics)
            if df is not None:
                data_frames.append(df)
                print(f"Данные для кампании {campaign_name} успешно получены")
            else:
                print(f"Ошибка получения данных для кампании {campaign_name}")

        if data_frames:
            final_data_frame = pd.concat(data_frames, ignore_index=True)
            print(tabulate(final_data_frame, headers=final_data_frame.columns))

            upload_data_to_bigquery(final_data_frame)
        else:
            print("Нет данных для загрузки в BigQuery.")
